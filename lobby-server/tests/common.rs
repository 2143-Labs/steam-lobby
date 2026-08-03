use std::sync::Arc;

use lobby_server::{build_app, AppConfig};
use sqlx::PgPool;

/// Serializes test runs so the shared DB is truncated/used by one test at a time.
static DB_LOCK: std::sync::LazyLock<tokio::sync::Mutex<()>> =
    std::sync::LazyLock::new(|| tokio::sync::Mutex::new(()));
pub struct TestHarness {
    pub base_url: String, // "http://127.0.0.1:PORT"
    pub ws_url: String,   // "ws://127.0.0.1:PORT/ws"
    pub pool: PgPool,     // connected to lobby_test
    _state: Arc<lobby_server::AppState>, // keep alive so ticker keeps running
    _server: tokio::task::JoinHandle<()>,
    /// Holds the DB lock for the whole test; dropped (released) when the harness drops.
    _lock_guard: tokio::sync::MutexGuard<'static, ()>,
}

pub async fn setup() -> TestHarness {
    let _lock_guard = DB_LOCK.lock().await;

    let root_url = "postgres://lobby:lobby@localhost:5432/lobby";

    // Create lobby_test if it doesn't exist.
    let root_pool = PgPool::connect(root_url).await.expect("connect root DB — run `just db-up` first");
    let exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = 'lobby_test')",
    )
    .fetch_one(&root_pool)
    .await
    .expect("query pg_database");
    if !exists {
        sqlx::query("CREATE DATABASE lobby_test")
            .execute(&root_pool)
            .await
            .expect("create lobby_test");
    }

    let test_url = "postgres://lobby:lobby@localhost:5432/lobby_test";
    let pool = PgPool::connect(test_url).await.expect("connect lobby_test");

    // Fresh DB has no tables — apply migrations (idempotent; build_app's own
    // migrate!() later sees them as already applied and is a no-op).
    sqlx::migrate!().run(&pool).await.expect("migrate lobby_test");

    // Clean slate: truncate all tables (RESTART IDENTITY resets serials; CASCADE handles FKs).
    sqlx::query(
        "TRUNCATE users, player_state, ratings, matchmaking_queue, matches, \
         match_reports, match_results RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .expect("truncate tables");

    let config = AppConfig {
        db_url: test_url.to_string(),
        steam_api_key: "test".into(),
        app_id: 480,
        jwt_secret: "integration-test-secret".into(),
        host: "127.0.0.1".into(),
        port: 0,
        match_accept_timeout_secs: 30,
        report_timeout_secs: 300,
        public_url: None,
    };

    let (app, state) = build_app(config).await; // runs migrations + spawns ticker
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind port 0");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server");
    });

    TestHarness {
        base_url: format!("http://{addr}"),
        ws_url: format!("ws://{addr}/ws"),
        pool,
        _state: state,
        _server: server,
        _lock_guard,
    }
}
