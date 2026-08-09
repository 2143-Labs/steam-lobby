//! Server-side signal helpers: the WS handlers signal workflows through the
//! client on `state.temporal` instead of calling `MatchManager` directly
//! (Step 9). All are best-effort — when `state.temporal` is `None` (Temporal
//! down, or the transition window before cutover) they no-op and the caller
//! falls back to the in-process path.
use std::sync::Arc;

use temporalio_client::{WorkflowSignalOptions, WorkflowStartOptions};

use lobby_core::types::{MatchDifficulty, SteamId};

use crate::state::AppState;
use crate::temporal::activities::QueueArgs;
use crate::temporal::workflows::{self, ChoiceArgs, DemoArgs, SessionArgs, StartArgs, WhoWonArgs};

/// The session workflow ID is per CONNECTION (`user-session-{steam_id}-{session_id}`):
/// each WS connection owns its own session workflow, so a crash-then-reconnect
/// or a replaced connection never collides with (or kills) a sibling session.
/// The session UUID, not the task queue, provides isolation.
fn session_workflow_id(steam_id: SteamId, session_id: &str) -> String {
    format!("user-session-{steam_id}-{session_id}")
}
pub(crate) async fn start_user_session(state: &Arc<AppState>, steam_id: SteamId, session_id: &str) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let _ = client
        .start_workflow(
            workflows::UserSessionWorkflow::run,
            SessionArgs { steam_id },
            WorkflowStartOptions::new(
                &state.config.temporal_task_queue,
                session_workflow_id(steam_id, session_id),
            )
            .build(),
        )
        .await;
}

/// Signal the session to enter the queue (BeginMatchmaking).
pub(crate) async fn signal_queue(
    state: &Arc<AppState>,
    steam_id: SteamId,
    session_id: &str,
    mode: String,
    difficulty: MatchDifficulty,
) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle = client.get_workflow_handle::<workflows::UserSessionWorkflow>(
        session_workflow_id(steam_id, session_id),
    );
    let _ = handle
        .signal(
            workflows::UserSessionWorkflow::queue,
            QueueArgs {
                steam_id,
                mode,
                difficulty,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
}

/// Signal the session to leave the queue (CancelMatchmaking).
pub(crate) async fn signal_unqueue(state: &Arc<AppState>, steam_id: SteamId, session_id: &str) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle = client.get_workflow_handle::<workflows::UserSessionWorkflow>(
        session_workflow_id(steam_id, session_id),
    );
    let _ = handle
        .signal(
            workflows::UserSessionWorkflow::unqueue,
            (),
            WorkflowSignalOptions::default(),
        )
        .await;
}


/// Signal the match workflow with an accept/decline choice (AcceptMatch /
/// DeclineMatch).
pub(crate) async fn signal_match_choice(
    state: &Arc<AppState>,
    token: &str,
    steam_id: SteamId,
    accept: bool,
) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle =
        client.get_workflow_handle::<workflows::P2PMatchWorkflow>(format!("match-{token}"));
    if let Err(e) = handle
        .signal(
            workflows::P2PMatchWorkflow::match_choice,
            ChoiceArgs { steam_id, accept },
            WorkflowSignalOptions::default(),
        )
        .await
    {
        tracing::warn!("signal match_choice to match-{token} failed: {e}");
    }
}

/// Signal the match workflow that a player clicked START (StartMatch).
pub(crate) async fn signal_start(state: &Arc<AppState>, token: &str, steam_id: SteamId) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle =
        client.get_workflow_handle::<workflows::P2PMatchWorkflow>(format!("match-{token}"));
    let _ = handle
        .signal(
            workflows::P2PMatchWorkflow::start,
            StartArgs { steam_id },
            WorkflowSignalOptions::default(),
        )
        .await;
}

/// Signal the match workflow with a who_won report (MatchReport).
pub(crate) async fn signal_who_won(
    state: &Arc<AppState>,
    token: &str,
    steam_id: SteamId,
    winner: SteamId,
) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle =
        client.get_workflow_handle::<workflows::P2PMatchWorkflow>(format!("match-{token}"));
    let _ = handle
        .signal(
            workflows::P2PMatchWorkflow::who_won,
            WhoWonArgs { steam_id, winner },
            WorkflowSignalOptions::default(),
        )
        .await;
}
/// Signal the session that the player disconnected (the disconnect block).
/// Unconditional per connection: a connection that ends — including one
/// replaced by a newer connection for the same player — ends ITS OWN session.
pub(crate) async fn signal_disconnect(state: &Arc<AppState>, steam_id: SteamId, session_id: &str) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle = client.get_workflow_handle::<workflows::UserSessionWorkflow>(
        session_workflow_id(steam_id, session_id),
    );
    let _ = handle
        .signal(
            workflows::UserSessionWorkflow::disconnect,
            (),
            WorkflowSignalOptions::default(),
        )
        .await;
}

/// The ticker's stale-entry sweep removed this player's queue row (out of
/// Temporal — the sweep runs in-process). Tell the CURRENT connection's
/// session so its `queued` copy clears and the player can re-queue. No
/// connection -> no live session to notify.
pub(crate) async fn signal_queue_expired(state: &Arc<AppState>, steam_id: SteamId) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let Some(session_id) = state
        .connections
        .lock()
        .await
        .get(&steam_id)
        .map(|e| e.session_id.clone())
    else {
        return;
    };
    let handle = client.get_workflow_handle::<workflows::UserSessionWorkflow>(
        session_workflow_id(steam_id, &session_id),
    );
    let _ = handle
        .signal(
            workflows::UserSessionWorkflow::queue_expired,
            (),
            WorkflowSignalOptions::default(),
        )
        .await;
}

/// Signal the match workflow with a demo submission (MatchReport).
pub(crate) async fn signal_submit_demo(
    state: &Arc<AppState>,
    token: &str,
    steam_id: SteamId,
    demo_hash: String,
) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle =
        client.get_workflow_handle::<workflows::P2PMatchWorkflow>(format!("match-{token}"));
    let _ = handle
        .signal(
            workflows::P2PMatchWorkflow::submit_demo,
            DemoArgs {
                steam_id,
                demo_hash,
            },
            WorkflowSignalOptions::default(),
        )
        .await;
}

