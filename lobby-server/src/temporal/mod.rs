//! Temporal integration: the in-process worker that runs the four lifecycle
//! workflows (`UserSessionWorkflow`, `QueueWorkflow`, `MatchmakerWorkflow`,
//! `P2PMatchWorkflow`) plus the `LobbyActivities` they drive.
//!
//! The worker lives inside the lobby-server binary (same axum process, one
//! Deployment). `start_temporal` connects to the configured Temporal frontend,
//! registers workflows + activities, and polls the task queue for the process
//! lifetime. On connect failure it logs and exits, leaving `state.temporal`
//! `None` so the WS handlers fall back to the in-process path.
//!
//! The SDK worker runs workflow tasks on an internal `tokio::task::LocalSet`
//! (workflow state uses `Rc<RefCell>` — not `Send`), so `Worker::run()` cannot
//! be driven from a multi-threaded runtime via `tokio::spawn`. It needs its own
//! current-thread tokio runtime on a dedicated OS thread; `Runtime::new_assume_tokio`
//! creates the SDK's core runtime inside that tokio context.
pub mod activities;
pub mod workflows;

use std::sync::Arc;

use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};

use crate::state::AppState;

/// Spawn the in-process Temporal worker on its own OS thread + current-thread
/// tokio runtime. Non-blocking; returns immediately. On connect failure the
/// worker logs `temporal unavailable` and exits, leaving `state.temporal` None
/// (the WS handlers then fall back to the in-process path).
pub fn start_temporal(state: Arc<AppState>) {
    std::thread::Builder::new()
        .name("temporal-worker".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to build temporal worker runtime");
            rt.block_on(run_worker(state));
        })
        .expect("failed to spawn temporal worker thread");
}

/// Connect to Temporal, set `state.temporal`, and run the worker for the
/// process lifetime (returns only on fatal worker error).
async fn run_worker(state: Arc<AppState>) {
    if let Err(e) = run_worker_inner(state).await {
        tracing::warn!("temporal unavailable — running in-process only: {e}");
    }
}

async fn run_worker_inner(state: Arc<AppState>) -> anyhow::Result<()> {
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

    let mut worker = Worker::new(&runtime, client, worker_options)
        .map_err(|e| anyhow::anyhow!("worker init failed: {e}"))?;
    tracing::info!(
        "temporal worker started on task queue {} (namespace {})",
        state.config.temporal_task_queue,
        state.config.temporal_namespace
    );
    worker.run().await?;
    Ok(())
}
