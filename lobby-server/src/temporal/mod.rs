//! Temporal integration: the in-process worker that runs the four lifecycle
//! workflows (`UserSessionWorkflow`, `QueueWorkflow`, `MatchmakerWorkflow`,
//! `P2PMatchWorkflow`) plus the `LobbyActivities` they drive.
//!
//! The worker lives inside the lobby-server binary (same axum process, one
//! Deployment). `start_temporal` connects to the configured Temporal frontend,
//! registers workflows + activities, and polls the task queue for the process
//! lifetime. On connect failure it logs and exits, leaving `state.temporal`
//! `None` so the WS handlers fall back to the in-process path.
pub mod activities;
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
        .register_workflow::<workflows::QueueWorkflow>()?
        .register_workflow::<workflows::MatchmakerWorkflow>()?
        .register_workflow::<workflows::P2PMatchWorkflow>()?
        .register_activities(activities::LobbyActivities {
            state: state.clone(),
        })
        .build();

    // Start one MatchmakerWorkflow per P2P game mode (replaces the ticker's
    // pairing loop). server_arena (GameType::Server) stays in-process — the
    // ticker still pairs it and the gameserver allocation/expiry path owns
    // its lifecycle (out of scope for v1). Best-effort; a failed start logs
    // and the mode simply has no matchmaking until the next restart.
    for (mode, game_type) in &state.game_modes {
        if *game_type != lobby_core::types::GameType::P2p {
            continue;
        }
        match client
            .start_workflow(
                workflows::MatchmakerWorkflow::run,
                workflows::MatchmakerArgs {
                    mode: mode.clone(),
                    accept_timeout_secs: state.config.match_accept_timeout_secs,
                    start_timeout_secs: state.config.start_timeout_secs,
                    report_timeout_secs: state.config.report_timeout_secs,
                },
                temporalio_client::WorkflowStartOptions::new(
                    &state.config.temporal_task_queue,
                    format!("matchmaker-{mode}-{}", state.config.temporal_task_queue),
                )
                .build(),
            )
            .await
        {
            Ok(_) => tracing::info!("matchmaker workflow started for {mode}"),
            Err(e) => {
                // Already started (server restart with a running matchmaker) is
                // expected — the existing workflow keeps matching. Anything
                // else is a real problem.
                let msg = e.to_string();
                if msg.contains("already started") {
                    tracing::debug!("matchmaker workflow already running for {mode}");
                } else {
                    tracing::warn!("failed to start matchmaker workflow for {mode}: {e}");
                }
            }
        }
    }

    let mut worker = Worker::new(&runtime, client, worker_options)
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
    Ok(())
}
