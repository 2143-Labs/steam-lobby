//! The lifecycle workflows. Determinism rules (the SDK enforces these at
//! runtime — the nondeterminism detector is on by default): workflow code may
//! NOT use tokio/futures primitives, std time, or I/O — use `ctx.timer()`,
//! `ctx.wait_condition()`, signals, and activities. All DB + WS work lives in
//! `crate::temporal::activities::LobbyActivities`.
//!
//! Workflow structs are plain serializable state with `#[derive(Default)]`;
//! `Arc<AppState>` reaches them only through activities.
//!
//! Every workflow terminates: `UserSessionWorkflow` ends on the disconnect
//! signal or its 24h TTL; `PairOnceWorkflow` returns after one activity; the
//! `P2PMatchWorkflow` races timers against wait-conditions and every branch
//! returns. There is deliberately NO long-lived queue/matchmaker workflow —
//! the queue is the `matchmaking_queue` DB row, driven by session signals.
use std::sync::Arc;
use std::time::Duration;

use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, SyncWorkflowContext, WorkflowContext, WorkflowResult, workflows::select,
};

use lobby_core::types::{MatchDifficulty, MatchInfo, PlayerId, PlayerState};

use crate::state::AppState;
use crate::temporal::activities::{self, FinishMatchArgs, MatchStateArgs, QueueArgs};

// ─────────────────────────────────────────────────────────────────────────────
// Shared args (all must be Serialize + Deserialize + Send + Sync)
// ─────────────────────────────────────────────────────────────────────────────

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct SessionArgs {
    pub user_id: uuid::Uuid,
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
pub struct PairOnceArgs {
    pub mode: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct MatchArgs {
    pub match_token: String,
    pub player_a: PlayerId,
    pub player_b: PlayerId,
    pub mode: String,
    pub difficulty: MatchDifficulty,
    pub accept_timeout_secs: u64,
    pub start_timeout_secs: u64,
    pub report_timeout_secs: u64,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ChoiceArgs {
    pub user_id: PlayerId,
    pub accept: bool,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct StartArgs {
    pub user_id: PlayerId,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct WhoWonArgs {
    pub user_id: PlayerId,
    pub winner: PlayerId,
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct DemoArgs {
    pub user_id: PlayerId,
    pub demo_hash: String,
}

fn short_activity() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5)).build()
}

// ─────────────────────────────────────────────────────────────────────────────
// 1. UserSessionWorkflow — one per WS connection. Owns the session: queue
//    state, in-match flag, disconnect. Driven entirely by signals; the queue
//    is the DB row, so there is no child workflow.
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
#[derive(Default)]
pub struct UserSessionWorkflow {
    user_id: uuid::Uuid,
    queued: Option<QueueArgs>,
    in_match: bool,
    last_match: Option<String>,
    disconnected: bool,
}

#[workflow_methods]
impl UserSessionWorkflow {
    /// Runs before any signal: the session's user_id is visible to signal
    /// handlers (a queue/unqueue signal delivered in the workflow's first
    /// task must not see the default nil UUID).
    #[init]
    pub fn init(_ctx: &temporalio_sdk::WorkflowContextView, args: SessionArgs) -> Self {
        Self {
            user_id: args.user_id,
            ..Default::default()
        }
    }

    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>) -> WorkflowResult<()> {
        // Recover reconnect-while-queued: adopt the DB queue entry so unqueue
        // works on the new session. The DB row IS the queue — there is no child
        // workflow to re-link.
        let user_id = ctx.state(|s| s.user_id);
        if let Ok(sync) = ctx
            .execute_activity(
                activities::LobbyActivities::sync_session,
                user_id,
                short_activity(),
            )
            .await
        {
            ctx.state_mut(|s| {
                s.queued = sync.queued.map(|q| QueueArgs {
                    user_id: q.user_id,
                    mode: q.game_mode,
                    difficulty: q.difficulty,
                });
            });
        }
        // 24h TTL: kills orphaned sessions whose disconnect signal was lost
        // (server crash mid-flight). A live connection re-creates its session
        // on the next reconnect anyway. Deterministic select! — the SDK's
        // updatable-timer shape (already used at the P2P phase-2 select).
        const SESSION_TTL: u64 = 24 * 3600;
        select! {
            _ = ctx.timer(Duration::from_secs(SESSION_TTL)) => {}
            _ = ctx.wait_condition(|s| s.disconnected) => {}
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
                (args.user_id, PlayerState::Queueing),
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
        ctx.state_mut(|s| s.queued = Some(args));
    }

    #[signal]
    pub async fn unqueue(ctx: &mut WorkflowContext<Self>) {
        let user_id = ctx.state(|s| s.user_id);
        let queued = match ctx.state(|s| s.queued.clone()) {
            Some(q) => q,
            None => {
                // Fresh session (reconnect): the run's sync_session recovery
                // may not have landed yet, and an unqueue signal racing it
                // would see an empty copy. The queue ROW is authoritative —
                // read it directly (idempotent; the run's own sync does the
                // same read).
                let Ok(sync) = ctx
                    .execute_activity(
                        activities::LobbyActivities::sync_session,
                        user_id,
                        short_activity(),
                    )
                    .await
                else {
                    return;
                };
                let Some(q) = sync.queued else { return; };
                QueueArgs {
                    user_id: q.user_id,
                    mode: q.game_mode,
                    difficulty: q.difficulty,
                }
            }
        };
        // Clear the session's queue copy FIRST: the queue signal's re-queue
        // guard reads it, and a re-queue arriving while the DB cleanup below
        // is in flight must not be swallowed. leave_queue's DELETE and the
        // re-queue's enter_queue upsert (ON CONFLICT) make row order moot.
        ctx.state_mut(|s| s.queued = None);
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::leave_queue,
                queued.clone(),
                short_activity(),
            )
            .await;
        // A re-queue may have landed while the cleanup ran (its handler set
        // s.queued back to Some and scheduled its own Queueing write): don't
        // clobber that with a stale InMenus reset.
        if !ctx.state(|s| s.queued.is_some()) {
            let _ = ctx
                .execute_activity(
                    activities::LobbyActivities::set_player_state,
                    (queued.user_id, PlayerState::InMenus),
                    short_activity(),
                )
                .await;
        }
    }

    #[signal]
    pub async fn match_found(ctx: &mut WorkflowContext<Self>, args: MatchFoundArgs) {
        let user_id = ctx.state(|s| s.user_id);
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::set_player_state,
                (user_id, PlayerState::InMatch),
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
        let user_id = ctx.state(|s| s.user_id);
        let _ = ctx
            .execute_activity(
                activities::LobbyActivities::set_player_state,
                (user_id, PlayerState::InMenus),
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
    /// The ticker's stale-entry sweep dropped our queue row (out of Temporal —
    /// the cleanup runs in-process). Clear the session's `queued` copy so the
    /// player can re-queue; the ticker already reset the DB player state.
    #[signal]
    pub fn queue_expired(&mut self, _ctx: &mut SyncWorkflowContext<Self>) {
        self.queued = None;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 2. PairOnceWorkflow — the Schedule's per-tick pairing run. One activity, then
//    returns. A 2s Schedule per P2P mode fires it; ScheduleOverlapPolicy::Skip
//    prevents concurrent runs. The activity starts the P2PMatchWorkflow and
//    signals both sessions' `match_found` before returning.
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
#[derive(Default)]
pub struct PairOnceWorkflow;

#[workflow_methods]
impl PairOnceWorkflow {
    #[run]
    pub async fn run(ctx: &mut WorkflowContext<Self>, args: PairOnceArgs) -> WorkflowResult<()> {
        ctx.execute_activity(
            activities::LobbyActivities::pair_matches,
            args.mode,
            short_activity(),
        )
        .await?;
        Ok(())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// 3. P2PMatchWorkflow — the coordinator. workflow_id = match-{token}. No server
//    referee in the workflow (p2p-first): it coordinates accept → start window
//    → finish. Each phase races ctx.timer against ctx.wait_condition inside the
//    SDK's deterministic `select!` (the v0.6.0 updatable_timer shape).
// ─────────────────────────────────────────────────────────────────────────────

#[workflow]
#[derive(Default)]
pub struct P2PMatchWorkflow {
    accepts: Vec<PlayerId>,
    declined: bool,
    /// Who declined (for the audit event); None = accept-timeout (no actor).
    declined_by: Option<PlayerId>,
    started: Vec<PlayerId>,
    who_won: Vec<(PlayerId, PlayerId)>,
    demo_hashes: Vec<(PlayerId, String)>,
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
                // Accept-timeout is NOT a decline — no actor in the event.
                ctx.execute_activity(
                    activities::LobbyActivities::handle_decline,
                    (MatchStateArgs { match_token: args.match_token.clone() }, None),
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
                    let declined_by = ctx.state(|s| s.declined_by);
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
                    let mut a = None;
                    let mut b = None;
                    for (sid, w) in &ctx.state(|s| s.who_won.clone()) {
                        if *sid == args.player_a { a = Some(*w); }
                        if *sid == args.player_b { b = Some(*w); }
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
                            winner: wa,
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
            if !self.accepts.contains(&args.user_id) {
                self.accepts.push(args.user_id);
            }
        } else {
            self.declined = true;
            self.declined_by = Some(args.user_id);
        }
    }

    #[signal]
    pub fn start(&mut self, _ctx: &mut SyncWorkflowContext<Self>, args: StartArgs) {
        if !self.started.contains(&args.user_id) {
            self.started.push(args.user_id);
        }
    }

    #[signal]
    pub fn who_won(&mut self, _ctx: &mut SyncWorkflowContext<Self>, args: WhoWonArgs) {
        self.who_won.retain(|(sid, _)| *sid != args.user_id);
        self.who_won.push((args.user_id, args.winner));
    }

    #[signal]
    pub fn submit_demo(&mut self, _ctx: &mut SyncWorkflowContext<Self>, args: DemoArgs) {
        self.demo_hashes.retain(|(sid, _)| *sid != args.user_id);
        self.demo_hashes.push((args.user_id, args.demo_hash));
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
        tracing::warn!(
            "failed to start P2PMatchWorkflow for {}: {e}",
            m.match_token
        );
    }
}

/// Signal a session's `match_found` (called by the pair_matches activity via
/// the client on AppState). Best-effort.
pub(crate) async fn notify_match_found(state: &Arc<AppState>, m: &MatchInfo) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    for pid in [m.player_a, m.player_b] {
        // Per-connection sessions: target the CURRENT connection's session.
        // No connection -> the player is offline, so there is no session to
        // signal (the MatchFound broadcast still reached the client).
        let Some(session_id) = state
            .connections
            .lock()
            .await
            .get(&pid)
            .map(|e| e.session_id.clone())
        else {
            continue;
        };
        let handle = client.get_workflow_handle::<UserSessionWorkflow>(format!(
            "user-session-{pid}-{session_id}"
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
pub(crate) async fn signal_session_complete(
    state: &Arc<AppState>,
    user_id: uuid::Uuid,
    token: &str,
) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    // Per-connection sessions: the match_complete signal goes to the CURRENT
    // connection's session (the one that entered the match). No connection ->
    // the player is offline; the match already reset their DB state.
    let Some(session_id) = state
        .connections
        .lock()
        .await
        .get(&user_id)
        .map(|e| e.session_id.clone())
    else {
        return;
    };
    let handle = client.get_workflow_handle::<UserSessionWorkflow>(format!(
        "user-session-{user_id}-{session_id}"
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
