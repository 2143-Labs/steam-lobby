//! Temporal activities: the ONLY place DB + WebSocket work happens. Each
//! method wraps an existing `MatchManager`/store call 1:1 (verified
//! `match_lifecycle.rs` — all take `&dyn` store refs), or the extracted
//! handler-body DB writes from `ws.rs`. Workflow structs stay plain
//! serializable state; `LobbyActivities` holds the `Arc<AppState>`.
use std::sync::Arc;

use temporalio_sdk::activities::{ActivityContext, ActivityError};

use lobby_core::types::{
    GameType, MatchDifficulty, MatchEvent, MatchInfo, MatchStatus, PlayerState, SteamId,
};

use lobby_core::traits::{MatchStore, PlayerStore, QueueStore};

use crate::state::AppState;
use crate::ws::{ServerMessage, notify_match_players};

/// All activity args/returns must be `Serialize + Deserialize + Send + Sync`
/// (Temporal payload boundary). Steam IDs are u64, tokens/demos are Strings.

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct MatchStateArgs {
    pub match_token: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct QueueArgs {
    pub steam_id: SteamId,
    pub mode: String,
    pub difficulty: MatchDifficulty,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct FinishMatchArgs {
    pub match_token: String,
    pub winner: Option<SteamId>,
    pub demo_hashes: Vec<String>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ResolveArgs {
    pub match_token: String,
    pub winner: SteamId,
}

/// The ticker's pairing body for one mode: scan queue, MMR-band pair, create
/// match, record Paired, signal both sessions `match_found`, and return the
/// formed match (so the workflow can start its `P2PMatchWorkflow`).
#[derive(serde::Serialize, serde::Deserialize)]
pub struct PairResult {
    pub match_info: Option<MatchInfo>,
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
        steam_id: SteamId,
    ) -> Result<(), ActivityError> {
        self.state
            .match_manager
            .accept_match(&args.match_token, steam_id, &self.state.store)
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
        steam_id: SteamId,
    ) -> Result<(), ActivityError> {
        self.state
            .match_manager
            .mark_connected(&args.match_token, steam_id, &self.state.store)
            .await?;
        if let Ok(Some(m)) = self.state.store.get_match(&args.match_token).await {
            let other = if steam_id == m.player_a {
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
            let _ = state.store.mark_accepted(&token, m.player_a).await;
            let _ = state.store.mark_accepted(&token, m.player_b).await;
            let _ = state
                .store
                .update_match_status(&token, MatchStatus::InProgress)
                .await;
            let _ = state
                .store
                .record_match_event(&token, MatchEvent::Accepted, None)
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
        declined_by: SteamId,
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
            .record_match_event(&token, MatchEvent::Declined, Some(declined_by))
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

    /// Resolve a match where a player never started (starter wins) — the
    /// workflow's start-window timeout branch.
    #[activity]
    pub async fn resolve_pong(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: ResolveArgs,
    ) -> Result<(), ActivityError> {
        self.state
            .match_manager
            .resolve_pong(
                &args.match_token,
                args.winner,
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
                winner: args.winner,
            },
        )
        .await;
        Ok(())
    }

    /// Resolve a match where neither player started (double loss, Part A).
    #[activity]
    pub async fn resolve_forfeit(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: MatchStateArgs,
    ) -> Result<(), ActivityError> {
        self.state
            .match_manager
            .resolve_forfeit(
                &args.match_token,
                &self.state.store,
                &self.state.store,
                &self.state.store,
            )
            .await?;
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
        let match_info = self
            .state
            .matchmaking_queue
            .tick(
                &mode,
                game_type,
                &self.state.store,
                &self.state.store,
                &self.state.store,
                &self.state.store,
            )
            .await?;
        if let Some(m) = &match_info {
            crate::temporal::workflows::notify_match_found(&self.state, m).await;
        }
        Ok(PairResult { match_info })
    }

    /// Enter the queue: player state Queueing + QueueStore::enqueue. Mirrors
    /// the BeginMatchmaking handler (ws.rs:641-672).
    #[activity]
    pub async fn enter_queue(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: QueueArgs,
    ) -> Result<(), ActivityError> {
        self.state
            .player_manager
            .begin_matchmaking(args.steam_id, args.difficulty, &self.state.store)
            .await?;
        let rating = <dyn lobby_core::traits::RatingStore>::get_rating(
            &self.state.store,
            args.steam_id,
            &args.mode,
        )
        .await
        .unwrap_or(lobby_core::types::OpenSkillRating {
            mu: 25.0,
            sigma: 25.0 / 3.0,
            last_updated: chrono::Utc::now(),
        });
        let entry = lobby_core::types::QueueEntry {
            steam_id: args.steam_id,
            game_mode: args.mode.clone(),
            difficulty: args.difficulty,
            mu: rating.mu,
            queued_at: chrono::Utc::now(),
        };
        self.state.store.enqueue(&entry).await?;
        Ok(())
    }

    /// Leave the queue: player state InMenus + QueueStore::dequeue. Mirrors
    /// the CancelMatchmaking handler (ws.rs:673-680).
    #[activity]
    pub async fn leave_queue(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: QueueArgs,
    ) -> Result<(), ActivityError> {
        self.state
            .player_manager
            .cancel_matchmaking(args.steam_id, &self.state.store)
            .await?;
        self.state.store.dequeue(args.steam_id, &args.mode).await?;
        Ok(())
    }
    /// Set the player state directly (PlayerStore::set_player_state).
    #[activity]
    pub async fn set_player_state(
        self: Arc<Self>,
        _ctx: ActivityContext,
        steam_id: SteamId,
        state: PlayerState,
    ) -> Result<(), ActivityError> {
        self.state.store.set_player_state(steam_id, state).await?;
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
}
