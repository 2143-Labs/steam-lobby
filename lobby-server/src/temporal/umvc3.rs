//! Durable UMVC3 ranked-match orchestration.
//!
//! PostgreSQL is the canonical state machine. The workflow only schedules
//! deterministic timers and invokes activities; it never inspects the database
//! or derives phase transitions itself.

use std::{sync::Arc, time::Duration};

use temporalio_client::WorkflowStartOptions;
use temporalio_common::{
    RetryPolicy,
    protos::temporal::api::enums::v1::{
        WorkflowIdConflictPolicy, WorkflowIdReusePolicy,
    },
};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::{
    ActivityOptions, WorkflowContext, WorkflowResult,
    activities::{ActivityContext, ActivityError},
    workflows::select,
};

use crate::{
    commands::{self, DrainMatchState},
    state::AppState,
};

const COMMAND_POLL_INTERVAL: Duration = Duration::from_secs(2);
const ACTIVITY_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(5);

/// Input shared by workflow starts and both lifecycle activities.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Umvc3MatchArgs {
    pub match_token: String,
}

/// Input for a phase expiry. The version makes a late timer harmless after a
/// command has already advanced the canonical database state.
#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
pub struct Umvc3ExpireArgs {
    pub match_token: String,
    pub expected_phase_version: i64,
}

fn lifecycle_activity_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(ACTIVITY_ATTEMPT_TIMEOUT)
        .retry_policy(
            RetryPolicy::builder()
                .initial_interval(Duration::from_secs(1))
                .backoff_coefficient(2.0)
                .maximum_interval(Duration::from_secs(30))
                .maximum_attempts(0)
                .build(),
        )
        .build()
}

/// Activity facade for the canonical PostgreSQL UMVC3 command processor.
///
/// Keeping this separate from the workflow makes every database read and write
/// an activity-history event and therefore replay-safe.
pub struct Umvc3Activities {
    pub state: Arc<AppState>,
}

#[temporalio_macros::activities]
impl Umvc3Activities {
    /// Drain all currently contiguous commands for this match and return the
    /// resulting canonical phase, version, deadline and DB-relative remaining
    /// duration.
    #[activity]
    pub async fn drain_match(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: Umvc3MatchArgs,
    ) -> Result<DrainMatchState, ActivityError> {
        Ok(commands::drain_match(&self.state, &args.match_token).await?)
    }

    /// Expire the phase only if the timer's phase version is still current.
    /// The command layer performs the compare-and-transition transaction and
    /// returns the post-transaction state.
    #[activity]
    pub async fn expire_phase(
        self: Arc<Self>,
        _ctx: ActivityContext,
        args: Umvc3ExpireArgs,
    ) -> Result<DrainMatchState, ActivityError> {
        Ok(commands::expire(
            &self.state,
            &args.match_token,
            args.expected_phase_version,
        )
        .await?)
    }
}

/// One durable workflow per UMVC3 ranked match (`match-{match_token}`).
///
/// Durable commands are polled every two seconds to bound wake-up latency. A
/// deadline-bearing phase additionally races that poll against the remaining
/// duration calculated from PostgreSQL time by the preceding activity. The
/// version passed to expiry prevents a stale deadline from advancing a newer
/// phase.
#[workflow]
#[derive(Default)]
pub struct UMVC3MatchWorkflow;

#[workflow_methods]
impl UMVC3MatchWorkflow {
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        args: Umvc3MatchArgs,
    ) -> WorkflowResult<()> {
        let mut state = ctx
            .execute_activity(
                Umvc3Activities::drain_match,
                args.clone(),
                lifecycle_activity_options(),
            )
            .await?;

        while !state.terminal {
            state = match state.remaining_ms {
                Some(remaining_ms) => {
                    select! {
                        _ = ctx.timer(Duration::from_millis(remaining_ms)) => {
                            ctx.execute_activity(
                                Umvc3Activities::expire_phase,
                                Umvc3ExpireArgs {
                                    match_token: args.match_token.clone(),
                                    expected_phase_version: state.phase_version,
                                },
                                lifecycle_activity_options(),
                            ).await?
                        }
                        _ = ctx.timer(COMMAND_POLL_INTERVAL) => {
                            ctx.execute_activity(
                                Umvc3Activities::drain_match,
                                args.clone(),
                                lifecycle_activity_options(),
                            ).await?
                        }
                    }
                }
                None => {
                    ctx.timer(COMMAND_POLL_INTERVAL).await;
                    ctx.execute_activity(
                        Umvc3Activities::drain_match,
                        args.clone(),
                        lifecycle_activity_options(),
                    )
                    .await?
                }
            };
        }

        Ok(())
    }
}


/// The stable Temporal workflow ID for a canonical UMVC3 match.
pub(crate) fn workflow_id(match_token: &str) -> String {
    format!("match-{match_token}")
}

/// Start the canonical workflow, or use the already-running execution.
///
/// `RejectDuplicate` prevents a terminal match's workflow ID from being reused;
/// `UseExisting` makes concurrent post-commit starts converge on the one live
/// execution. Temporal unavailability is intentionally a best-effort no-op so
/// the durable command inbox remains the recovery source of truth.
pub(crate) async fn start_umvc3_match(state: &Arc<AppState>, match_token: &str) {
    start_or_use_existing(state, match_token, "start").await;
}

/// Reconcile a non-terminal canonical match with its Temporal execution.
/// This has the same idempotent semantics as the post-pairing start helper.
pub(crate) async fn reconcile_umvc3_match(state: &Arc<AppState>, match_token: &str) {
    start_or_use_existing(state, match_token, "reconcile").await;
}

async fn start_or_use_existing(state: &Arc<AppState>, match_token: &str, operation: &str) {
    let Some(client) = state.temporal.read().ok().and_then(|slot| slot.clone()) else {
        return;
    };

    let id = workflow_id(match_token);
    let options = WorkflowStartOptions::new(&state.config.temporal_task_queue, id)
        .id_reuse_policy(WorkflowIdReusePolicy::RejectDuplicate)
        .id_conflict_policy(WorkflowIdConflictPolicy::UseExisting)
        .build();

    match client
        .start_workflow(
            UMVC3MatchWorkflow::run,
            Umvc3MatchArgs {
                match_token: match_token.to_owned(),
            },
            options,
        )
        .await
    {
        Ok(_) => {
            if let Err(error) = state.store.mark_umvc3_workflow_started(match_token).await {
                tracing::warn!(match_token, operation, %error, "failed to audit UMVC3 workflow ensure");
            }
        }
        Err(error) => {
            tracing::warn!(match_token, operation, %error, "failed to ensure UMVC3 match workflow");
        }
    }
}
