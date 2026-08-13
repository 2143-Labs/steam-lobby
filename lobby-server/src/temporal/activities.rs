//! Temporal activities: the ONLY place DB + WebSocket work happens. Each
//! method wraps an existing `MatchManager`/store call 1:1 (verified
//! `match_lifecycle.rs` — all take `&dyn` store refs), or the extracted
//! handler-body DB writes from `ws.rs`. Workflow structs stay plain
//! serializable state; `LobbyActivities` holds the `Arc<AppState>`.
use std::sync::Arc;

use temporalio_sdk::activities::{ActivityContext, ActivityError};

use lobby_core::types::{
    GameType, MatchDifficulty, MatchEvent, MatchInfo, MatchStatus, PlayerId, PlayerState,
    QueueEntry,
};

use lobby_core::traits::{MatchStore, PlayerStore, QueueStore};

use crate::state::AppState;
use crate::ws::{ServerMessage, notify_match_players};

/// All activity args/returns must be `Serialize + Deserialize + Send + Sync`
/// (Temporal payload boundary). Player IDs are UUIDs, tokens/demos are Strings.

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct MatchStateArgs {
    pub match_token: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct QueueArgs {
    pub user_id: PlayerId,
    pub mode: String,
    pub difficulty: MatchDifficulty,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct FinishMatchArgs {
    pub match_token: String,
    pub winner: Option<PlayerId>,
    pub demo_hashes: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct StartForfeitArgs {
    pub match_token: String,
    /// Some(starter) → starter wins; None → neither started (double loss).
    pub winner: Option<PlayerId>,
}

/// The ticker's pairing body for one mode: scan queue, MMR-band pair, create
/// match, record Paired, signal both sessions `match_found`, and return the
/// formed match (so the workflow can start its `P2PMatchWorkflow`).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct PairResult {
    pub match_info: Option<MatchInfo>,
}

/// Raw DB shapes for session start: the player's state + current queue entry,
/// recovered so a reconnect-while-queued player can unqueue on the new session.
/// The session run does the mapping (QueueEntry -> QueueArgs).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct SessionSync {
    pub state: PlayerState,
    pub queued: Option<QueueEntry>,
}

pub struct LobbyActivities {
    pub state: Arc<AppState>,
}

#[temporalio_macros::activities]
impl LobbyActivities {
    /// Accept one player's match. Mirrors `MatchManager::accept_match`.
    #[activity]
    pub async fn accept_match(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: MatchStateArgs,
        user_id: PlayerId,
    ) -> Result<(), ActivityError> {
        self.state
            .match_manager
            .accept_match(&args.match_token, user_id, &self.state.store)
            .await?;
        Ok(())
    }

    /// Record one player's P2P connection: `MatchManager::mark_connected`
    /// (DB Reporting) + the `opponent_connected` broadcast (the StartMatch
    /// handler body, ws.rs:744-778).
    #[activity]
    pub async fn mark_connected(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: MatchStateArgs,
        user_id: PlayerId,
    ) -> Result<(), ActivityError> {
        self.state
            .match_manager
            .mark_connected(&args.match_token, user_id, &self.state.store)
            .await?;
        // After the SECOND player connects the match flips to Reporting: spawn
        // the server-authoritative pong referee for PLAYBACK (Step 11 — it
        // renders frames/checksums; the workflow owns resolution).
        if let Ok(Some(m)) = self.state.store.get_match(&args.match_token).await {
            if m.game_mode == "rps_1v1"
                && m.status == lobby_core::types::MatchStatus::Reporting
            {
                crate::rps::spawn_game(&self.state, &m);
            } else if self.state.config.pong_enabled
                && m.game_type == lobby_core::types::GameType::P2p
                && m.status == lobby_core::types::MatchStatus::Reporting
            {
                crate::pong::spawn_game(&self.state, &m);
            }
            let other = if user_id == m.player_a {
                m.player_b
            } else {
                m.player_a
            };
            let connections = self.state.connections.lock().await;
            if let Some(e) = connections.get(&other) {
                let _ = e.tx.send(ServerMessage::OpponentConnected {
                    match_token: args.match_token.clone(),
                });
            }
        }
        Ok(())
    }

    /// Both players accepted — mark both, flip InProgress, record Accepted,
    /// broadcast `match_started` with the configured window length. (The
    /// accept-complete DB writes extracted from the AcceptMatch handler,
    /// ws.rs:682-714.)
    #[activity]
    pub async fn mark_accepts(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: MatchStateArgs,
    ) -> Result<(), ActivityError> {
        let state = self.state.clone();
        let token = args.match_token;
        if let Ok(Some(m)) = state.store.get_match(&token).await {
            // The workflow's two accept_match activities (one per player)
            // already recorded the Accepted events; mark_accepts only flips
            // the status and broadcasts match_started.
            let _ = state.store.mark_accepted(&token, m.player_a).await;
            let _ = state.store.mark_accepted(&token, m.player_b).await;
            let _ = state
                .store
                .update_match_status(&token, MatchStatus::InProgress)
                .await;
        }
        notify_match_players(
            &state,
            &token,
            ServerMessage::MatchStarted {
                match_token: token.clone(),
                start_timeout_secs: state.config.start_timeout_secs,
            },
        )
        .await;
        Ok(())
    }

    /// A decline: DB Disputed + MatchDeclined to both (the DeclineMatch
    /// handler body, ws.rs:715-743).
    #[activity]
    pub async fn handle_decline(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: MatchStateArgs,
        declined_by: Option<PlayerId>,
    ) -> Result<(), ActivityError> {
        let state = self.state.clone();
        let token = args.match_token;
        notify_match_players(
            &state,
            &token,
            ServerMessage::MatchDeclined {
                match_token: token.clone(),
            },
        )
        .await;
        if let Ok(Some(m)) = state.store.get_match(&token).await
            && m.status == MatchStatus::PendingAccept
        {
            let _ = state
                .store
                .update_match_status(&token, MatchStatus::Disputed)
                .await;
        }
        let _ = state
            .store
            .record_match_event(&token, MatchEvent::Declined, declined_by)
            .await;
        Ok(())
    }

    /// The agree-path resolve: ratings + match_results + Resolved +
    /// on_match_ended + game_over/match_result broadcasts (the `resolve_agreed`
    /// body), then signal both sessions `match_complete`.
    #[activity]
    pub async fn finish_match(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: FinishMatchArgs,
    ) -> Result<(), ActivityError> {
        let state = self.state.clone();
        let token = args.match_token;
        let winner = args
            .winner
            .ok_or_else(|| lobby_core::error::LobbyError::InvalidReport("no winner".into()))?;
        if let Ok(Some(m)) = state.store.get_match(&token).await {
            let outcome = state
                .match_manager
                .resolve_pong(&token, winner, &state.store, &state.store, &state.store)
                .await?;
            notify_match_players(
                &state,
                &token,
                ServerMessage::GameOver {
                    match_token: token.clone(),
                    winner: winner.to_string(),
                },
            )
            .await;
            if let Ok(outcome_value) = serde_json::to_value(&outcome) {
                notify_match_players(
                    &state,
                    &token,
                    ServerMessage::MatchResult {
                        match_token: token.clone(),
                        outcome: outcome_value,
                    },
                )
                .await;
            }
            crate::temporal::workflows::signal_session_complete(&state, m.player_a, &token).await;
            crate::temporal::workflows::signal_session_complete(&state, m.player_b, &token).await;
        }
        Ok(())
    }

    /// START-window forfeit (the Part A start window's timeout body): the
    /// match is still InProgress when the window expires — flip it to
    /// Reporting first (resolve_pong/forfeit validate on Reporting), then
    /// resolve. One started → the starter wins; neither → double loss.
    #[activity]
    pub async fn resolve_start_forfeit(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: StartForfeitArgs,
    ) -> Result<(), ActivityError> {
        let _ = self
            .state
            .store
            .update_match(
                &args.match_token,
                MatchStatus::Reporting,
                chrono::Utc::now(),
            )
            .await;
        let outcome = if let Some(winner) = args.winner {
            let outcome = self
                .state
                .match_manager
                .resolve_pong(
                    &args.match_token,
                    winner,
                    &self.state.store,
                    &self.state.store,
                    &self.state.store,
                )
                .await?;
            notify_match_players(
                &self.state,
                &args.match_token,
                ServerMessage::GameOver {
                    match_token: args.match_token.clone(),
                    winner: winner.to_string(),
                },
            )
            .await;
            outcome
        } else {
            self.state
                .match_manager
                .resolve_forfeit(
                    &args.match_token,
                    &self.state.store,
                    &self.state.store,
                    &self.state.store,
                )
                .await?
        };
        // MatchResult to both (mirror the old in-process start window).
        notify_match_players(
            &self.state,
            &args.match_token,
            ServerMessage::MatchResult {
                match_token: args.match_token.clone(),
                outcome: serde_json::to_value(&outcome).unwrap_or_default(),
            },
        )
        .await;
        Ok(())
    }

    /// A dispute resolution: DB Disputed + both InMenus + MatchResult(Disputed)
    /// (the submit_report disagreement/expiry tail, match_lifecycle.rs:156-176).
    #[activity]
    pub async fn resolve_dispute(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: MatchStateArgs,
    ) -> Result<(), ActivityError> {
        let state = self.state.clone();
        let token = args.match_token;
        if let Ok(Some(m)) = state.store.get_match(&token).await
            && m.status != MatchStatus::Resolved
        {
            let _ = state
                .store
                .update_match_status(&token, MatchStatus::Disputed)
                .await;
            let _ = state
                .player_manager
                .reporting_complete(m.player_a, &state.store)
                .await;
            let _ = state
                .player_manager
                .reporting_complete(m.player_b, &state.store)
                .await;
        }
        notify_match_players(
            &state,
            &token,
            ServerMessage::MatchResult {
                match_token: token.clone(),
                outcome: serde_json::json!({ "Disputed": {} }),
            },
        )
        .await;
        Ok(())
    }

    /// The ticker's pairing body for one mode (queue.rs tick → create_match +
    /// record_match_event + MatchFound notify + session match_found signals).
    #[activity]
    pub async fn pair_matches(
        self: Arc<Self>,
        _ctx: ActivityContext,
        mode: String,
    ) -> Result<PairResult, ActivityError> {
        let game_type = self
            .state
            .game_modes
            .iter()
            .find(|(m, _)| *m == mode)
            .map(|(_, t)| *t)
            .unwrap_or(GameType::P2p);
        let match_info = match self
            .state
            .store
            .pair_next_match(
                &mode,
                game_type,
                self.state.config.pair_cooldown_secs as i64,
            )
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("pair_matches tick failed for {mode}: {e}");
                return Err(ActivityError::from(e));
            }
        };
        let remaining = self
            .state
            .store
            .get_queue(&mode)
            .await
            .map(|q| q.len())
            .unwrap_or(0);
        tracing::debug!(
            "pair_matches ran for mode {} (queue size {})",
            mode,
            remaining
        );
        // Idle schedule: fewer than two players left (no pair possible) —
        // pause the pairing schedule so an idle server stops creating
        // `PairOnceWorkflow` runs. `enter_queue` unpauses it; the in-process
        // ticker is the safety net. Best-effort.
        if remaining < 2 {
            crate::temporal::schedule::pause_if_idle(&self.state, &mode).await;
        }
        if let Some(m) = &match_info {
            tracing::info!(
                "pair_matches formed match {}: {} vs {}",
                m.match_token,
                m.player_a,
                m.player_b
            );
        }
        if let Some(m) = &match_info {
            // Start the P2PMatchWorkflow FIRST so the accept signals can never
            // race ahead of the workflow's existence.
            crate::temporal::workflows::start_p2p_match(
                &self.state,
                m,
                self.state.config.match_accept_timeout_secs,
                self.state.config.start_timeout_secs,
                self.state.config.report_timeout_secs,
            )
            .await;
            // Broadcast MatchFound to both players (mirror the in-process
            // ticker's notify, ticker.rs:44-108) and signal their sessions.

            for pid in [m.player_a, m.player_b] {
                let opponent = if pid == m.player_a {
                    m.player_b
                } else {
                    m.player_a
                };
                // The opponent's display name comes from the users table (set
                // at login from the provider userinfo) — no Steam API call in
                // the pairing path.
                let opponent_name = self
                    .state
                    .store
                    .get_display_name(opponent)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| "Unknown".into());
                let msg = ServerMessage::MatchFound {
                    match_token: m.match_token.clone(),
                    opponent: crate::ws::OpponentInfo {
                        player_id: opponent.to_string(),
                        display_name: opponent_name,
                    },
                    timeout_ms: 30_000,
                    game_type: m.game_type,
                    game_mode: m.game_mode.clone(),
                };
                let connections = self.state.connections.lock().await;
                if let Some(e) = connections.get(&pid) {
                    let _ = e.tx.send(msg);
                }
                drop(connections);
            }
            crate::temporal::workflows::notify_match_found(&self.state, m).await;
        }
        Ok(PairResult { match_info })
    }

    /// Enter the queue: QueueStore::enqueue only. The player state transition
    /// (→ Queueing) is the separate `set_player_state` activity — calling
    /// `begin_matchmaking` here would fail with InvalidStateTransition because
    /// the state is already Queueing by the time this runs.
    #[activity]
    pub async fn enter_queue(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: QueueArgs,
    ) -> Result<(), ActivityError> {
        tracing::info!(
            "enter_queue activity: user_id={} mode={}",
            args.user_id,
            args.mode
        );
        let rating = <dyn lobby_core::traits::RatingStore>::get_rating(
            &self.state.store,
            args.user_id,
            &args.mode,
        )
        .await
        .unwrap_or(lobby_core::types::OpenSkillRating {
            mu: 25.0,
            sigma: 25.0 / 3.0,
            last_updated: chrono::Utc::now(),
        });
        let entry = lobby_core::types::QueueEntry {
            user_id: args.user_id,
            game_mode: args.mode.clone(),
            difficulty: args.difficulty,
            mu: rating.mu,
            queued_at: chrono::Utc::now(),
        };
        self.state.store.enqueue(&entry).await?;
        // Queueing is proof of life: refresh liveness so the stale-queue
        // sweep (30s since last_heartbeat) never evicts a just-queued entry
        // whose previous heartbeat predates the queue click (mirrors the
        // in-process begin_matchmaking).
        let _ = self.state.store.update_heartbeat(args.user_id).await;
        // The pairing schedule may be paused (idle). A fresh queue entry
        // means a pair may be possible again — unpause it so the next tick
        // pairs. Best-effort; the in-process ticker re-checks too.
        crate::temporal::schedule::ensure_running(&self.state, &args.mode).await;
        tracing::info!("enter_queue activity done: user_id={}", args.user_id);
        Ok(())
    }

    /// Leave the queue: QueueStore::dequeue only (state handled separately).
    #[activity]
    pub async fn leave_queue(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: QueueArgs,
    ) -> Result<(), ActivityError> {
        self.state.store.dequeue(args.user_id, &args.mode).await?;
        Ok(())
    }

    /// Set the player state directly (PlayerStore::set_player_state).
    #[activity]
    pub async fn set_player_state(
        self: Arc<Self>,
        _ctx: ActivityContext,
        user_id: PlayerId,
        state: PlayerState,
    ) -> Result<(), ActivityError> {
        self.state.store.set_player_state(user_id, state).await?;
        Ok(())
    }

    /// DB re-read + sanity for the match (always true for now).
    #[activity]
    pub async fn verify_match(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: MatchStateArgs,
    ) -> Result<bool, ActivityError> {
        match self.state.store.get_match(&args.match_token).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Err(lobby_core::error::LobbyError::MatchNotFound(args.match_token).into()),
            Err(e) => Err(e.into()),
        }
    }
    /// Recover the raw DB state for a session start: the player's state +
    /// current queue entry (reconnect-while-queued). The session run maps the
    /// queue entry to `QueueArgs` — the recovered session_id must be the NEW
    /// session's, which only the run knows.
    #[activity]
    pub async fn sync_session(
        self: Arc<Self>,
        _ctx: ActivityContext,
        user_id: PlayerId,
    ) -> Result<SessionSync, ActivityError> {
        let state = self.state.store.get_player_state(user_id).await?;
        let queued = self.state.store.get_queued_entry(user_id).await?;
        Ok(SessionSync {
            state: state.map(|p| p.state).unwrap_or(PlayerState::InMenus),
            queued,
        })
    }
}
