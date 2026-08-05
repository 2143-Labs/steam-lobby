//! 2-second maintenance loop driving the server's background work: queue
//! pairing, queue-stats broadcast, stale-queue cleanup, accept/playing/report
//! expiry, and gameserver allocation for accepted matches.
use std::sync::Arc;
use std::time::Duration;

use lobby_core::traits::{MatchStore, QueueStore, RatingStore};
use lobby_core::types::LeaderboardEntry;

use crate::state::AppState;
use crate::ws::{notify_match_players, OpponentInfo, ServerMessage};

pub async fn tick_loop(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        for (mode, game_type) in &state.game_modes {
            match state
                .matchmaking_queue
                .tick(mode, *game_type, &state.store, &state.store, &state.store, &state.store)
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
                        // Fetch the display name BEFORE taking the connections lock so
                        // the global mutex is never held across a Steam API call.
                        let opponent_name = state_a
                            .steam_auth
                            .get_player_summary(info_a.player_b)
                            .await
                            .unwrap_or_else(|_| "Unknown".into());
                        let connections = state_a.connections.lock().await;
                        if let Some(tx_a) = connections.get(&info_a.player_a).map(|e| &e.tx) {
                            let _ = tx_a.send(ServerMessage::MatchFound {
                                match_token: info_a.match_token.clone(),
                                opponent: OpponentInfo {
                                    steam_id: info_a.player_b,
                                    display_name: opponent_name,
                                },
                                timeout_ms: 30_000,
                                game_type: info_a.game_type,
                            });
                        }
                    });

                    let state_b = state.clone();
                    let info_b = match_info;
                    tokio::spawn(async move {
                        let opponent_name = state_b
                            .steam_auth
                            .get_player_summary(info_b.player_a)
                            .await
                            .unwrap_or_else(|_| "Unknown".into());
                        let connections = state_b.connections.lock().await;
                        if let Some(tx_b) = connections.get(&info_b.player_b).map(|e| &e.tx) {
                            let _ = tx_b.send(ServerMessage::MatchFound {
                                match_token: info_b.match_token.clone(),
                                opponent: OpponentInfo {
                                    steam_id: info_b.player_a,
                                    display_name: opponent_name,
                                },
                                timeout_ms: 30_000,
                                game_type: info_b.game_type,
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
        for (mode, _game_type) in &state.game_modes {
            if let Ok(queue) = state.store.get_queue(mode).await {
                let leaderboard: Vec<LeaderboardEntry> = state
                    .store
                    .list_ratings(mode)
                    .await
                    .unwrap_or_default()
                    .into_iter()
                    .map(|(id, r)| LeaderboardEntry {
                        steam_id: id,
                        mu: r.mu,
                        sigma: r.sigma,
                        rating: r.mu - 3.0 * r.sigma,
                    })
                    .collect();
                let now = chrono::Utc::now();
                let mut sends: Vec<(u64, ServerMessage)> = Vec::new();
                for entry in &queue {
                    let wait_s = (now - entry.queued_at).num_seconds().max(0) as f64;
                    let (lo, hi) = lobby_core::queue::search_band(
                        wait_s,
                        entry.mu,
                        entry.difficulty.mmr_offset(),
                    );
                    let candidates = queue
                        .iter()
                        .filter(|o| o.steam_id != entry.steam_id && o.mu >= lo && o.mu <= hi)
                        .count() as u32;
                    let rating = state
                        .store
                        .get_rating(entry.steam_id, mode)
                        .await
                        .unwrap_or(lobby_core::types::OpenSkillRating {
                            mu: entry.mu,
                            sigma: 25.0 / 3.0,
                            last_updated: now,
                        });
                    sends.push((
                        entry.steam_id,
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
        if let Ok(removed) = state.matchmaking_queue.cleanup_stale(&state.store).await {
            for sid in &removed {
                // The entry is gone — the owner must be back in the menus so a
                // later reconnect reports "in_menus", not a stale "queueing".
                let _ = state.player_manager.handle_disconnect(*sid, &state.store).await;
            }
            if !removed.is_empty() {
                let connections = state.connections.lock().await;
                for sid in removed {
                    if let Some(e) = connections.get(&sid) {
                        let _ = e.tx.send(ServerMessage::QueueExpired);
                    }
                }
            }
        }

        // Tell both players when their match dies so no client is left
        // waiting on a panel that can never resolve.
        if let Ok(expired) = state
            .match_manager
            .expire_pending_accepts(&state.store, &state.store)
            .await
        {
            for token in expired {
                notify_match_players(
                    &state,
                    &token,
                    ServerMessage::MatchExpired {
                        match_token: token.clone(),
                    },
                )
                .await;
            }
        }
        // Server-authoritative matches: allocate a gameserver once both accepted.
        let in_progress = state
            .store
            .get_matches_by_status(lobby_core::types::MatchStatus::InProgress)
            .await;
        if let Ok(matches) = in_progress {
            for m in matches {
                if m.game_type != lobby_core::types::GameType::Server || m.server_address.is_some() {
                    continue;
                }
                let since = m.accepted_at.unwrap_or(m.created_at);
                let age = (chrono::Utc::now() - since).num_seconds().max(0) as u64;
                if age > state.gameserver_alloc_timeout_secs {
                    tracing::info!(
                        "match {} server allocation timed out after {}s -> Disputed",
                        m.match_token, age
                    );
                    let _ = state
                        .store
                        .update_match_status(&m.match_token, lobby_core::types::MatchStatus::Disputed)
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
                            outcome: serde_json::to_value(lobby_core::types::MatchOutcome::Disputed)
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
                        tracing::warn!("match {} allocation failed (will retry): {e}", m.match_token)
                    }
                }
            }
        }
        // Gameserver never reported -> Disputed (mirrors the report-timeout path).
        if let Ok(expired) = state
            .match_manager
            .expire_playing_matches(&state.store, state.gameserver_result_timeout_secs, &state.store)
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
        if let Ok(resolved) = state
            .match_manager
            .expire_pending_reports(&state.store, &state.store, &state.store)
            .await
        {
            for (token, outcome) in resolved {
                notify_match_players(
                    &state,
                    &token,
                    ServerMessage::MatchResult {
                        match_token: token.clone(),
                        outcome: serde_json::to_value(&outcome).unwrap(),
                    },
                )
                .await;
            }
        }
    }
}
