//! 2-second maintenance loop driving the server's background work: queue
//! pairing, queue-stats broadcast, stale-queue cleanup, accept/playing/report
//! expiry, and gameserver allocation for accepted matches.
use std::sync::Arc;
use std::time::Duration;

use lobby_core::traits::{MatchStore, QueueStore, RatingStore};
use lobby_core::types::LeaderboardEntry;

use crate::state::AppState;
use crate::ws::{OpponentInfo, ServerMessage};

pub async fn tick_loop(state: Arc<AppState>, shutdown: Option<tokio::sync::watch::Receiver<bool>>) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    let mut shutdown = shutdown;
    let mut reconcile_ticks = 0_u8;
    loop {
        // Either the 2s tick (run the maintenance body below) or a shutdown
        // signal from the test harness (exit the loop, dropping AppState).
        let stop = async {
            if let Some(rx) = shutdown.as_mut() {
                let _ = rx.changed().await;
            } else {
                std::future::pending::<()>().await;
            }
        };
        tokio::select! {
            _ = interval.tick() => {}
            _ = stop => break,
        }

        // Durable command processing is independent of Temporal signals. This
        // poll guarantees admitted commands survive lost signals and restarts.
        match crate::commands::drain_pending(&state).await {
            // Only wake long-pollers when work was actually applied, so an idle
            // tick never shortens a client's 25-second events poll.
            Ok(applied) if applied > 0 => state.event_notify.notify_waiters(),
            Ok(_) => {}
            Err(error) => tracing::warn!(%error, "ranked command drain failed"),
        }

        // Reconcile on startup and every ten seconds. Durable command draining
        // remains every two seconds; workflow ensure traffic need not.
        if reconcile_ticks == 0 {
            let reconcile_state = state.clone();
            tokio::spawn(async move {
                match reconcile_state.store.nonterminal_umvc3_tokens().await {
                    Ok(tokens) => {
                        for token in tokens {
                            crate::temporal::umvc3::reconcile_umvc3_match(&reconcile_state, &token).await;
                        }
                    }
                    Err(error) => tracing::warn!(%error, "UMVC3 workflow reconciliation query failed"),
                }
            });
            reconcile_ticks = 4;
        } else {
            reconcile_ticks -= 1;
        }
        // Gameserver-authoritative modes stay in-process. P2P modes are owned
        // by their Temporal pairing schedules.
        for spec in &state.game_modes {
            if spec.authority != lobby_core::types::ResultAuthority::Gameserver {
                continue;
            }
            match state
                .store
                .pair_next_match_with_accept_timeout(
                    spec.id,
                    spec,
                    state.config.pair_cooldown_secs as i64,
                    state.config.match_accept_timeout_secs,
                )
                .await
            {
                Ok(Some(match_info)) => {
                    tracing::info!(
                        "match {} formed: {} vs {} ({})",
                        match_info.match_token,
                        match_info.player_a,
                        match_info.player_b,
                        match_info.game_mode
                    );
                    // Notify both players — spawn each notification to avoid
                    // blocking the 2s tick cycle on Steam API HTTP calls.
                    let state_a = state.clone();
                    let info_a = match_info.clone();
                    tokio::spawn(async move {
                        let opponent_name = state_a
                            .store
                            .get_display_name(info_a.player_b)
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "Unknown".into());
                        let opponent_player_id = info_a.player_b.to_string();
                        let connections = state_a.connections.lock().await;
                        if let Some(tx_a) = connections.get(&info_a.player_a).map(|e| &e.tx) {
                            let _ = tx_a.send(ServerMessage::MatchFound {
                                match_token: info_a.match_token.clone(),
                                opponent: OpponentInfo {
                                    player_id: opponent_player_id,
                                    display_name: opponent_name,
                                },
                                timeout_ms: 30_000,
                                game_type: info_a.connection,
                                game_mode: info_a.game_mode.clone(),
                            });
                        }
                    });

                    let state_b = state.clone();
                    let info_b = match_info;
                    tokio::spawn(async move {
                        let opponent_name = state_b
                            .store
                            .get_display_name(info_b.player_a)
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or_else(|| "Unknown".into());
                        let opponent_player_id = info_b.player_a.to_string();
                        let connections = state_b.connections.lock().await;
                        if let Some(tx_b) = connections.get(&info_b.player_b).map(|e| &e.tx) {
                            let _ = tx_b.send(ServerMessage::MatchFound {
                                match_token: info_b.match_token.clone(),
                                opponent: OpponentInfo {
                                    player_id: opponent_player_id,
                                    display_name: opponent_name,
                                },
                                timeout_ms: 30_000,
                                game_type: info_b.connection,
                                game_mode: info_b.game_mode.clone(),
                            });
                        }
                    });
                }
                Ok(None) => {}
                Err(e) => tracing::error!("queue tick failed: {e}"),
            }
        }

        // Live queue stats for the demo: best-effort, never blocks the tick on errors.
        // Leaderboard and stats are per-mode now.
        for spec in &state.game_modes {
            let mode = spec.id;
            if let Ok(queue) = state.store.get_queue(mode).await {
                let ratings = state.store.list_ratings(mode).await.unwrap_or_default();
                let mut leaderboard: Vec<LeaderboardEntry> = Vec::with_capacity(ratings.len());
                for (id, r) in ratings {
                    leaderboard.push(LeaderboardEntry {
                        player_id: id.to_string(),
                        mu: r.mu,
                        sigma: r.sigma,
                        rating: r.mu - 3.0 * r.sigma,
                    });
                }
                let now = chrono::Utc::now();
                let mut sends: Vec<(uuid::Uuid, ServerMessage)> = Vec::new();
                for entry in &queue {
                    let wait_s = (now - entry.queued_at).num_seconds().max(0) as f64;
                    let (lo, hi) = lobby_core::queue::search_band(
                        wait_s,
                        entry.mu,
                        entry.difficulty.mmr_offset(),
                    );
                    let candidates = queue
                        .iter()
                        .filter(|o| o.user_id != entry.user_id && o.mu >= lo && o.mu <= hi)
                        .count() as u32;
                    let rating = state
                        .store
                        .get_rating(entry.user_id, mode)
                        .await
                        .unwrap_or(lobby_core::types::OpenSkillRating {
                            mu: entry.mu,
                            sigma: 25.0 / 3.0,
                            last_updated: now,
                        });
                    sends.push((
                        entry.user_id,
                        ServerMessage::QueueStatus {
                            elapsed_ms: (wait_s * 1000.0) as u64,
                            band_lo: lo,
                            band_hi: hi,
                            candidates,
                            queue_size: queue.len() as u32,
                            my_mu: rating.mu,
                            my_sigma: rating.sigma,
                            my_rating: rating.mu - 3.0 * rating.sigma,
                            leaderboard: leaderboard.clone(),
                        },
                    ));
                }
                let connections = state.connections.lock().await; // lock ONLY after all awaits
                for (sid, msg) in sends {
                    if let Some(entry) = connections.get(&sid) {
                        let _ = entry.tx.send(msg);
                    }
                }
            }
        }

        // Safety net for the idle-paused pairing schedules: if players are
        // queued but the schedule is paused (a resume was lost), unpause it
        // so the next tick pairs them. Idle cost is the get_queue SELECT
        // above; the schedule RPC only happens when >=2 are queued.
        for spec in &state.game_modes {
            if spec.authority != lobby_core::types::ResultAuthority::ServerReferee {
                continue;
            }
            if let Ok(queue) = state.store.get_queue(spec.id).await
                && queue.len() >= 2
            {
                crate::temporal::schedule::ensure_running(&state, spec.id).await;
            }
        }

        // NativeReport schedules are enabled only when queue admission is
        // enabled. Existing leased rows remain cancellable/heartbeat-visible
        // while disabled but cannot form new matches.
        if state.config.ranked_queue_enabled {
            for spec in &state.game_modes {
                if spec.authority != lobby_core::types::ResultAuthority::NativeReport {
                    continue;
                }
                if let Ok(queue) = state.store.get_queue(spec.id).await
                    && queue.len() >= 2
                {
                    crate::temporal::schedule::ensure_running(&state, spec.id).await;
                }
            }
        }
        if let Ok(removed) = lobby_core::queue::cleanup_stale(&state.store).await {
            for sid in &removed {
                // The entry is gone — the owner must be back in the menus so a
                // later reconnect reports "in_menus", not a stale "queueing".
                let _ = state
                    .player_manager
                    .handle_disconnect(*sid, &state.store)
                    .await;
            }
            if !removed.is_empty() {
                let connections = state.connections.lock().await;
                for sid in &removed {
                    if let Some(e) = connections.get(sid) {
                        let _ = e.tx.send(ServerMessage::QueueExpired);
                    }
                }
                drop(connections);
                // The sweep runs OUT of Temporal (in-process maintenance); the
                // session must still learn its queue row is gone, so its
                // `queued` copy clears and the player can re-queue. Send the
                // signal after the client notification — a re-queue click only
                // happens once the client has seen QueueExpired.
                for sid in &removed {
                    crate::temporal::signals::signal_queue_expired(&state, *sid).await;
                }
            }
        }

        // Server-authoritative matches: allocate a gameserver once both accepted.
        let in_progress = state
            .store
            .get_matches_by_status(lobby_core::types::MatchStatus::InProgress)
            .await;
        if let Ok(matches) = in_progress {
            for m in matches {
                if m.connection != lobby_core::types::ConnectionStrategy::Server || m.server_address.is_some()
                {
                    continue;
                }
                let since = m.accepted_at.unwrap_or(m.created_at);
                let age = (chrono::Utc::now() - since).num_seconds().max(0) as u64;
                if age > state.gameserver_alloc_timeout_secs {
                    tracing::info!(
                        "match {} server allocation timed out after {}s -> Disputed",
                        m.match_token,
                        age
                    );
                    let _ = state
                        .store
                        .update_match_status(
                            &m.match_token,
                            lobby_core::types::MatchStatus::Disputed,
                        )
                        .await;
                    crate::ws::notify_match_players(
                        &state,
                        &m.match_token,
                        ServerMessage::GameServerError {
                            match_token: m.match_token.clone(),
                            message: "server allocation timed out".into(),
                        },
                    )
                    .await;
                    crate::ws::notify_match_players(
                        &state,
                        &m.match_token,
                        ServerMessage::MatchResult {
                            match_token: m.match_token.clone(),
                            outcome: serde_json::to_value(
                                lobby_core::types::MatchOutcome::Disputed,
                            )
                            .unwrap(),
                        },
                    )
                    .await;
                    continue;
                }
                match state.gameserver.allocate(&m).await {
                    Ok(alloc) => {
                        if let Err(e) = state
                            .store
                            .mark_server_ready(
                                &m.match_token,
                                &alloc.server_address,
                                alloc.join_token.as_deref(),
                            )
                            .await
                        {
                            tracing::warn!("mark_server_ready failed for {}: {e}", m.match_token);
                            continue;
                        }
                        tracing::info!(
                            "match {} gameserver ready: {}",
                            m.match_token,
                            alloc.server_address
                        );
                        crate::ws::notify_match_players(
                            &state,
                            &m.match_token,
                            ServerMessage::GameServerReady {
                                match_token: m.match_token.clone(),
                                address: alloc.server_address,
                                join_token: alloc.join_token,
                            },
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::warn!(
                            "match {} allocation failed (will retry): {e}",
                            m.match_token
                        )
                    }
                }
            }
        }
        // Gameserver never reported -> Disputed (mirrors the report-timeout path).
        if let Ok(expired) = state
            .match_manager
            .expire_playing_matches(
                &state.store,
                state.gameserver_result_timeout_secs,
                &state.store,
            )
            .await
        {
            for token in expired {
                crate::ws::notify_match_players(
                    &state,
                    &token,
                    ServerMessage::MatchResult {
                        match_token: token.clone(),
                        outcome: serde_json::to_value(lobby_core::types::MatchOutcome::Disputed)
                            .unwrap(),
                    },
                )
                .await;
            }
        }
    }
}
