//! Temporal integration: the in-process worker that runs the lifecycle
//! workflows (`UserSessionWorkflow`, `PairOnceWorkflow`, `P2PMatchWorkflow`)
//! plus the `LobbyActivities` they drive.
//!
//! The worker lives inside the lobby-server binary (same axum process, one
//! Deployment). `start_temporal` connects to the configured Temporal frontend,
//! registers workflows + activities, and polls the task queue for the process
//! lifetime. On connect failure it logs and exits, leaving `state.temporal`
//! `None` so the WS handlers fall back to the in-process path.
pub mod activities;
pub mod schedule;
pub mod signals;
pub mod workflows;
// The SDK worker runs workflow tasks on an internal `tokio::task::LocalSet`
// (workflow state uses `Rc<RefCell>` — not `Send`), so `Worker::run()` cannot
// be driven from a multi-threaded runtime via `tokio::spawn`. It runs on its
// own OS thread with a multi-thread tokio runtime; `Runtime::new_assume_tokio`
// creates the SDK's core runtime inside that tokio context. The multi-thread
// runtime keeps the activity pollers from being starved by long workflow
// replays (the matchmaker's every-2s timer history).

use std::sync::Arc;

use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

use crate::state::AppState;

/// Spawn the in-process Temporal worker on its own OS thread + current-thread
/// tokio runtime. Non-blocking; returns immediately. On connect failure the
/// worker logs `temporal unavailable` and exits, leaving `state.temporal` None
/// (the WS handlers then fall back to the in-process path).
pub fn start_temporal(
    state: Arc<AppState>,
) -> Option<tokio::sync::oneshot::Receiver<Box<dyn Fn() + Send + Sync>>> {
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name("temporal-worker".into())
        .spawn(move || {
            // Multi-thread runtime: the SDK's workflow replay of long histories
            // (e.g. the matchmaker's every-2s timer) must not starve the
            // activity pollers on a single current-thread runtime.
            let rt = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .expect("failed to build temporal worker runtime");
            // The worker sends its shutdown handle back so the caller (the
            // test harness) can stop it at teardown — Worker::run() then
            // returns and the thread exits. Production ignores the receiver.
            rt.block_on(run_worker(state, shutdown_tx));
        })
        .ok()?;
    Some(shutdown_rx)
}

/// Connect to Temporal, set `state.temporal`, and run the worker for the
/// process lifetime (returns only on fatal worker error).
async fn run_worker(
    state: Arc<AppState>,
    shutdown_tx: tokio::sync::oneshot::Sender<Box<dyn Fn() + Send + Sync>>,
) {
    match run_worker_inner(state, shutdown_tx).await {
        Ok(()) => {}
        Err(e) => {
            tracing::warn!("temporal unavailable — running in-process only: {e}");
        }
    }
}

async fn run_worker_inner(
    state: Arc<AppState>,
    shutdown_tx: tokio::sync::oneshot::Sender<Box<dyn Fn() + Send + Sync>>,
) -> anyhow::Result<()> {
    let runtime = Runtime::new_assume_tokio(Default::default())?;

    let conn_opts = ConnectionOptions::new(
        state
            .config
            .temporal_address
            .clone()
            .parse::<temporalio_client::Url>()?,
    )
    .build();
    let connection = Connection::connect(conn_opts).await?;
    let client = Client::new(
        connection,
        ClientOptions::new(state.config.temporal_namespace.clone()).build(),
    )?;

    // Hand the client to the WS handlers (Step 9): they signal workflows via
    // `state.temporal`. Set BEFORE the worker starts polling so handlers never
    // see a None client once the worker is up.
    {
        let mut slot = state
            .temporal
            .write()
            .map_err(|e| anyhow::anyhow!("temporal slot poisoned: {e}"))?;
        *slot = Some(Arc::new(client.clone()));
    }

    let worker_options = WorkerOptions::new(&state.config.temporal_task_queue)
        .register_workflow::<workflows::UserSessionWorkflow>()?
        .register_workflow::<workflows::PairOnceWorkflow>()?
        .register_workflow::<workflows::P2PMatchWorkflow>()?
        .register_activities(activities::LobbyActivities {
            state: state.clone(),
        })
        .build();

    // One Schedule per P2P game mode fires a short `PairOnceWorkflow` every 2s
    // (replaces the old per-mode MatchmakerWorkflow `loop {}` — bounded
    // history). `ScheduleOverlapPolicy::Skip` is the single-writer guarantee:
    // the server appends a timestamp to each scheduled workflow ID, so Skip —
    // not the `pair-{mode}` prefix — prevents concurrent pairing runs (the
    // `FOR UPDATE` in `pair_next_match` is the second line of defense).
    // server_arena (GameType::Server) stays in-process — the ticker still
    // pairs it. Schedules survive restarts by design (creating an existing ID
    // is an error, handled below); the worker deletes only the schedules THIS
    // boot created on shutdown, so tests don't accumulate schedules on the
    // dev Temporal while production (which never stops the worker) persists.
    let mut created_schedules: Vec<String> = Vec::new();
    for (mode, game_type) in &state.game_modes {
        if *game_type != lobby_core::types::GameType::P2p {
            continue;
        }
        let schedule_id = format!("matchmaker-{mode}-{}", state.config.temporal_task_queue);
        match client
            .create_schedule(
                &schedule_id,
                temporalio_client::schedules::CreateScheduleOptions::builder()
                    .action(temporalio_client::schedules::ScheduleAction::start_workflow(
                        workflows::PairOnceWorkflow::run,
                        workflows::PairOnceArgs { mode: mode.clone() },
                        &state.config.temporal_task_queue,
                        // The task queue suffix keeps parallel workers' runs
                        // distinct: Temporal appends only a 1-second-resolution
                        // timestamp to the workflow ID, so two schedules firing
                        // in the same second (e.g. the test suite's per-test
                        // workers) would otherwise collide on one ID.
                        format!("pair-{mode}-{}", state.config.temporal_task_queue),
                    ))
                    .spec(temporalio_client::schedules::ScheduleSpec::from_interval(
                        std::time::Duration::from_secs(2),
                    ))
                    .overlap_policy(
                        temporalio_client::schedules::ScheduleOverlapPolicy::Skip,
                    )
                    .build(),
            )
            .await
        {
            Ok(_) => {
                created_schedules.push(schedule_id);
                tracing::info!("pairing schedule created for {mode}");
            }
            Err(e) => {
                // Idempotent boot: the schedule already exists (it survives
                // restarts by design) — keep it. Anything else is a real error.
                let exists = client
                    .get_schedule_handle(&schedule_id)
                    .describe(Default::default())
                    .await
                    .is_ok();
                if exists {
                    tracing::debug!("pairing schedule already exists for {mode}");
                } else {
                    tracing::warn!("failed to create pairing schedule for {mode}: {e}");
                }
            }
        }
    }

    let mut worker = Worker::new(&runtime, client.clone(), worker_options)
        .map_err(|e| anyhow::anyhow!("worker init failed: {e}"))?;
    // Hand the caller (the test harness) a shutdown handle so it can stop the
    // worker at teardown; production drops the receiver.
    let _ = shutdown_tx.send(Box::new(worker.shutdown_handle()) as Box<dyn Fn() + Send + Sync>);
    tracing::info!(
        "temporal worker started on task queue {} (namespace {})",
        state.config.temporal_task_queue,
        state.config.temporal_namespace
    );
    worker.run().await?;
    // Worker stopped (the test harness fired the shutdown handle): delete the
    // schedules this boot created so tests don't accumulate schedules on the
    // dev Temporal. Production never stops the worker, so this is a no-op
    // there — schedules persist across restarts by design.
    for id in &created_schedules {
        let _ = client.get_schedule_handle(id).delete(Default::default()).await;
    }
    Ok(())
}
