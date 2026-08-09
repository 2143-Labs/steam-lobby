//! The four lifecycle workflows. Determinism rules (the SDK enforces these at
//! runtime — the nondeterminism detector is on by default): workflow code may
//! NOT use tokio/futures primitives, std time, or I/O — use `ctx.timer()`,
//! `ctx.wait_condition()`, signals, and activities. All DB + WS work lives in
//! `crate::temporal::activities::LobbyActivities`.
//!
//! Workflow structs are plain serializable state with `#[derive(Default)]`;
//! `Arc<AppState>` reaches them only through activities.
use std::sync::Arc;
use std::time::Duration;

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, ChildWorkflowOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult,
    workflows::select,
};

use lobby_core::types::{MatchDifficulty, MatchInfo, PlayerState, SteamId};

use crate::state::AppState;
use crate::temporal::activities::{
    self, FinishMatchArgs, MatchStateArgs, QueueArgs,
};

// ─────────────────────────────────────────────────────────────────────────────
// Shared args (all must be Serialize + Deserialize + Send + Sync)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct SessionArgs {
    pub steam_id: SteamId,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct MatchFoundArgs {
    pub match_token: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct MatchCompleteArgs {
    pub match_token: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct MatchmakerArgs {
    pub mode: String,
    pub accept_timeout_secs: u64,
    pub start_timeout_secs: u64,
    pub report_timeout_secs: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct MatchArgs {
    pub match_token: String,
    pub player_a: SteamId,
    pub player_b: SteamId,
    pub mode: String,
    pub difficulty: MatchDifficulty,
    pub accept_timeout_secs: u64,
    pub start_timeout_secs: u64,
    pub report_timeout_secs: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ChoiceArgs {
    pub steam_id: SteamId,
    pub accept: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct StartArgs {
    pub steam_id: SteamId,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct WhoWonArgs {
    pub steam_id: SteamId,
    pub winner: SteamId,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct DemoArgs {
    pub steam_id: SteamId,
    pub demo_hash: String,
}

fn short_activity() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5)).build()
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. UserSessionWorkflow — one per logged-in player. Owns the session: queue
//    state, in-match flag, disconnect. Child QueueWorkflow per queue entry.
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
#[derive(Default)]
pub struct UserSessionWorkflow {
    steam_id: SteamId,
    queued: Option<QueueArgs>,
    in_match: bool,
    last_match: Option<String>,
    disconnected: bool,
}

#[workflow_methods]
impl UserSessionWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, args: SessionArgs) -> WorkflowResult<()> {
        ctx.state_mut(|s| s.steam_id = args.steam_id);
        // Ends when the disconnect signal arrives.
        ctx.wait_condition(|s| s.disconnected).await;
        // On disconnect, cancel a still-pending queue child (best-effort).
        if let Some(queued) = ctx.state(|s| s.queued.clone()) {
            let _ = ctx
                .external_workflow(format!("queue-{}-{}", queued.steam_id, queued.mode), None)
                .cancel(Some("session disconnected".into()))
                .await;
        }
        Ok(())
    }

    #[signal]
    pub async fn queue(ctx: &mut WorkflowContext<Self>, args: QueueArgs) {
        if ctx.state(|s| s.queued.is_some()) {
            return; // re-queue guard
        }
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::set_player_state,
                (args.steam_id, PlayerState::Queueing),
                short_activity(),
            )
            .await;
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::enter_queue,
                args.clone(),
                short_activity(),
            )
            .await;
        let _ = ctx
            .start_child_workflow(
                crate::temporal::workflows::QueueWorkflow::run,
                args.clone(),
                ChildWorkflowOptions::workflow_id(format!("queue-{}-{}", args.steam_id, args.mode)),
            )
            .await;
        ctx.state_mut(|s| s.queued = Some(args));
    }

    #[signal]
    pub async fn unqueue(ctx: &mut WorkflowContext<Self>) {
        let Some(queued) = ctx.state(|s| s.queued.clone()) else {
            return;
        };
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::leave_queue,
                queued.clone(),
                short_activity(),
            )
            .await;
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::set_player_state,
                (queued.steam_id, PlayerState::InMenus),
                short_activity(),
            )
            .await;
        ctx.state_mut(|s| s.queued = None);
    }

    #[signal]
    pub async fn match_found(ctx: &mut WorkflowContext<Self>, args: MatchFoundArgs) {
        let steam_id = ctx.state(|s| s.steam_id);
        // Cancel the queue child (we leave the queue implicitly on match).
        if let Some(queued) = ctx.state(|s| s.queued.clone()) {
            let _ = ctx
                .external_workflow(format!("queue-{}-{}", queued.steam_id, queued.mode), None)
                .cancel(Some("match found".into()))
                .await;
        }
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::set_player_state,
                (steam_id, PlayerState::InMatch),
                short_activity(),
            )
            .await;
        ctx.state_mut(|s| {
            s.queued = None;
            s.in_match = true;
            s.last_match = Some(args.match_token);
        });
    }

    #[signal]
    pub async fn match_complete(ctx: &mut WorkflowContext<Self>, _args: MatchCompleteArgs) {
        let steam_id = ctx.state(|s| s.steam_id);
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::set_player_state,
                (steam_id, PlayerState::InMenus),
                short_activity(),
            )
            .await;
        ctx.state_mut(|s| {
            s.in_match = false;
            s.last_match = None;
        });
    }

    #[signal]
    pub fn disconnect(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.disconnected = true;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. QueueWorkflow — child of a session per queue entry. Pure state holder:
//    enter_queue on start, then waits for cancellation (the session cancels it
//    on unqueue/match_found/disconnect). The MatchmakerWorkflow does pairing.
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
#[derive(Default)]
pub struct QueueWorkflow;

#[workflow_methods]
impl QueueWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, args: QueueArgs) -> WorkflowResult<()> {
        ctx.execute_activity(
            activities::LobbyActivities::enter_queue,
            args,
            short_activity(),
        )
        .await?;
        // Wait until the parent session cancels us (unqueue, match_found, or
        // disconnect). `ctx.cancelled()` resolves when a cancel request lands.
        ctx.cancelled().await;
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. MatchmakerWorkflow — one per game mode, started at boot. Replaces the 2s
//    ticker's pairing loop: every 2s run the pair_matches activity for the
//    mode. The activity signals both sessions' `match_found` and returns the
//    formed match; this workflow starts the P2PMatchWorkflow for it.
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
#[derive(Default)]
pub struct MatchmakerWorkflow;

#[workflow_methods]
impl MatchmakerWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, args: MatchmakerArgs) -> WorkflowResult<()> {
        loop {
            ctx.timer(Duration::from_secs(2)).await;
            // pair_matches: scan queue, pair by MMR band, create_match + record
            // the event, START the P2PMatchWorkflow, then broadcast MatchFound
            // and signal both sessions. The activity owns the P2P-workflow
            // start so clients can never accept before the workflow exists.
            ctx.execute_activity(
                activities::LobbyActivities::pair_matches,
                args.mode.clone(),
                short_activity(),
            )
            .await?;
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 4. P2PMatchWorkflow — the coordinator. workflow_id = match-{token}. No server
//    referee in the workflow (p2p-first): it coordinates accept → start window
//    → finish. Each phase races ctx.timer against ctx.wait_condition inside the
//    SDK's deterministic `select!` (the v0.6.0 updatable_timer shape).
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
#[derive(Default)]
pub struct P2PMatchWorkflow {
    accepts: Vec<SteamId>,
    declined: bool,
    started: Vec<SteamId>,
    who_won: Vec<(SteamId, SteamId)>,
    demo_hashes: Vec<(SteamId, String)>,
}

#[workflow_methods]
impl P2PMatchWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, args: MatchArgs) -> WorkflowResult<()> {
        // Phase 1: verify (DB re-read + sanity; always true for now).
        ctx.execute_activity(
            activities::LobbyActivities::verify_match,
            MatchStateArgs {
                match_token: args.match_token.clone(),
            },
            short_activity(),
        )
        .await?;

        // Phase 2: await accept — race the accept timer against "both accepted
        // / anyone declined".
        select! {
            _ = ctx.timer(Duration::from_secs(args.accept_timeout_secs)) => {
                ctx.execute_activity(
                    activities::LobbyActivities::handle_decline,
                    (MatchStateArgs { match_token: args.match_token.clone() }, args.player_a),
                    short_activity(),
                ).await?;
                return Ok(());
            }
            _ = ctx.wait_condition(|s| {
                s.declined
                    || (s.accepts.len() == 2
                        && s.accepts.contains(&args.player_a)
                        && s.accepts.contains(&args.player_b))
            }) => {
                if ctx.state(|s| s.declined) {
                    let declined_by = ctx.state(|s| s.accepts.first().copied()).unwrap_or(args.player_a);
                    ctx.execute_activity(
                        activities::LobbyActivities::handle_decline,
                        (MatchStateArgs { match_token: args.match_token.clone() }, declined_by),
                        short_activity(),
                    ).await?;
                    return Ok(());
                }
            }
        }

        // All accepted → record each player's accept (validated DB writes:
        // mark_accepted + Accepted event + InProgress on the second) then
        // broadcast match_started.
        ctx.execute_activity(
            activities::LobbyActivities::accept_match,
            (
                MatchStateArgs {
                    match_token: args.match_token.clone(),
                },
                args.player_a,
            ),
            short_activity(),
        )
        .await?;
        ctx.execute_activity(
            activities::LobbyActivities::accept_match,
            (
                MatchStateArgs {
                    match_token: args.match_token.clone(),
                },
                args.player_b,
            ),
            short_activity(),
        )
        .await?;
        ctx.execute_activity(
            activities::LobbyActivities::mark_accepts,
            MatchStateArgs {
                match_token: args.match_token.clone(),
            },
            short_activity(),
        )
        .await?;

        // Phase 3: the start window — race the start timer against "both started".
        select! {
            _ = ctx.timer(Duration::from_secs(args.start_timeout_secs)) => {
                // Forfeit: one started → the starter wins; neither → double
                // loss (Part A). resolve_start_forfeit flips InProgress →
                // Reporting first (resolve_pong/forfeit validate on Reporting).
                let started = ctx.state(|s| s.started.clone());
                let winner = if started.len() == 1 {
                    Some(started[0])
                } else {
                    None
                };
                ctx.execute_activity(
                    activities::LobbyActivities::resolve_start_forfeit,
                    activities::StartForfeitArgs {
                        match_token: args.match_token.clone(),
                        winner,
                    },
                    short_activity(),
                )
                .await?;
                return Ok(());
            }
            _ = ctx.wait_condition(|s| {
                s.started.contains(&args.player_a) && s.started.contains(&args.player_b)
            }) => {
                // Both started → mark_connected (DB Reporting + opponent_connected
                // broadcasts).
                ctx.execute_activity(
                    activities::LobbyActivities::mark_connected,
                    (MatchStateArgs { match_token: args.match_token.clone() }, args.player_a),
                    short_activity(),
                )
                .await?;
                ctx.execute_activity(
                    activities::LobbyActivities::mark_connected,
                    (MatchStateArgs { match_token: args.match_token.clone() }, args.player_b),
                    short_activity(),
                ).await?;
            }
        }

        // Phase 4: endgame (p2p-first) — race the report timer against both
        // who_won+submit_demo signals.
        select! {
            _ = ctx.timer(Duration::from_secs(args.report_timeout_secs)) => {
                ctx.execute_activity(
                    activities::LobbyActivities::resolve_dispute,
                    MatchStateArgs { match_token: args.match_token.clone() },
                    short_activity(),
                ).await?;
            }
            _ = ctx.wait_condition(|s| s.who_won.len() == 2) => {
                let (wa, wb) = {
                    let mut a = 0u64;
                    let mut b = 0u64;
                    for (sid, w) in &ctx.state(|s| s.who_won.clone()) {
                        if *sid == args.player_a { a = *w; }
                        if *sid == args.player_b { b = *w; }
                    }
                    (a, b)
                };
                if wa == wb {
                    let demos: Vec<String> = ctx
                        .state(|s| s.demo_hashes.iter().map(|(_, h)| h.clone()).collect());
                    ctx.execute_activity(
                        activities::LobbyActivities::finish_match,
                        FinishMatchArgs {
                            match_token: args.match_token.clone(),
                            winner: Some(wa),
                            demo_hashes: demos,
                        },
                        short_activity(),
                    ).await?;
                } else {
                    ctx.execute_activity(
                        activities::LobbyActivities::resolve_dispute,
                        MatchStateArgs { match_token: args.match_token.clone() },
                        short_activity(),
                    ).await?;
                }
            }
        }
        Ok(())
    }

    #[signal]
    pub fn match_choice(&mut self, _ctx: &mut SyncWorkflowContext<Self>, args: ChoiceArgs) {
        if args.accept {
            if !self.accepts.contains(&args.steam_id) {
                self.accepts.push(args.steam_id);
            }
        } else {
            self.declined = true;
        }
    }

    #[signal]
    pub fn start(&mut self, _ctx: &mut SyncWorkflowContext<Self>, args: StartArgs) {
        if !self.started.contains(&args.steam_id) {
            self.started.push(args.steam_id);
        }
    }

    #[signal]
    pub fn who_won(&mut self, _ctx: &mut SyncWorkflowContext<Self>, args: WhoWonArgs) {
        self.who_won.retain(|(sid, _)| *sid != args.steam_id);
        self.who_won.push((args.steam_id, args.winner));
    }

    #[signal]
    pub fn submit_demo(&mut self, _ctx: &mut SyncWorkflowContext<Self>, args: DemoArgs) {
        self.demo_hashes.retain(|(sid, _)| *sid != args.steam_id);
        self.demo_hashes.push((args.steam_id, args.demo_hash));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Start the P2PMatchWorkflow for a formed match (called by the pair_matches
/// activity BEFORE signaling match_found, so clients can never accept before
/// the workflow exists). Best-effort; a failed start logs.
pub(crate) async fn start_p2p_match(
    state: &Arc<AppState>,
    m: &MatchInfo,
    accept_timeout_secs: u64,
    start_timeout_secs: u64,
    report_timeout_secs: u64,
) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    if let Err(e) = client
        .start_workflow(
            P2PMatchWorkflow::run,
            MatchArgs {
                match_token: m.match_token.clone(),
                player_a: m.player_a,
                player_b: m.player_b,
                mode: m.game_mode.clone(),
                difficulty: m.player_a_difficulty,
                accept_timeout_secs,
                start_timeout_secs,
                report_timeout_secs,
            },
            temporalio_client::WorkflowStartOptions::new(
                &state.config.temporal_task_queue,
                format!("match-{}", m.match_token),
            )
            .build(),
        )
        .await
    {
        tracing::warn!("failed to start P2PMatchWorkflow for {}: {e}", m.match_token);
    }
}

/// Signal a session's `match_found` (called by the pair_matches activity via
/// the client on AppState). Best-effort.
pub(crate) async fn notify_match_found(state: &Arc<AppState>, m: &MatchInfo) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    for pid in [m.player_a, m.player_b] {
        let handle = client.get_workflow_handle::<UserSessionWorkflow>(format!(
            "user-session-{pid}-{}",
            state.config.temporal_task_queue
        ));
        let _ = handle
            .signal(
                UserSessionWorkflow::match_found,
                MatchFoundArgs {
                    match_token: m.match_token.clone(),
                },
                temporalio_client::WorkflowSignalOptions::default(),
            )
            .await;
    }
}

/// Signal a session's `match_complete` (called by the finish_match activity).
pub(crate) async fn signal_session_complete(state: &Arc<AppState>, steam_id: SteamId, token: &str) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle = client.get_workflow_handle::<UserSessionWorkflow>(format!(
        "user-session-{steam_id}-{}",
        state.config.temporal_task_queue
    ));
    let _ = handle
        .signal(
            UserSessionWorkflow::match_complete,
            MatchCompleteArgs {
                match_token: token.to_string(),
            },
            temporalio_client::WorkflowSignalOptions::default(),
        )
        .await;
}
