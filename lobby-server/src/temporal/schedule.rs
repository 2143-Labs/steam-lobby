//! Schedule lifecycle helpers (per-mode pairing schedules). A pairing schedule
//! is PAUSED when the queue can't produce a match (idle) and UNPAUSED when a
//! player enqueues, so an idle server creates ~zero workflows instead of one
//! `PairOnceWorkflow` run every 2s. All best-effort: a lost pause/resume is at
//! worst the old behavior (a run every 2s) — the in-process ticker re-checks
//! and unpauses if players are queued (safety net), and the worker creates the
//! schedule unpaused at boot.
use std::sync::Arc;

use crate::state::AppState;

/// The pairing schedule ID for a mode (matches creation in `mod.rs`).
pub(crate) fn schedule_id(state: &Arc<AppState>, mode: &str) -> String {
    format!("matchmaker-{mode}-{}", state.config.temporal_task_queue)
}

/// Pause the mode's pairing schedule if it is currently running and the queue
/// can't produce a match. Best-effort; Temporal-down is a no-op.
pub(crate) async fn pause_if_idle(state: &Arc<AppState>, mode: &str) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle = client.get_schedule_handle(schedule_id(state, mode));
    let Ok(desc) = handle.describe(Default::default()).await else {
        return; // schedule gone (or Temporal hiccup) — nothing to pause
    };
    if !desc.paused() {
        let _ = handle
            .pause(Some("queue empty — no pair possible".to_string()), Default::default())
            .await;
    }
}

/// Unpause the mode's pairing schedule if it is paused (a player just
/// enqueued, so a pair may be possible again). Best-effort.
pub(crate) async fn ensure_running(state: &Arc<AppState>, mode: &str) {
    let Some(client) = state.temporal.read().ok().and_then(|g| g.clone()) else {
        return;
    };
    let handle = client.get_schedule_handle(schedule_id(state, mode));
    let Ok(desc) = handle.describe(Default::default()).await else {
        return;
    };
    if desc.paused() {
        let _ = handle
            .unpause(
                Some("players queued — resume pairing".to_string()),
                Default::default(),
            )
            .await;
    }
}
