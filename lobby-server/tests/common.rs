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
    setup_full(false, None).await
}

/// Harness with the pong game enabled (LOBBY_PONG = true) for p2p matches.
pub async fn setup_pong() -> TestHarness {
    setup_full(true, None).await
}

pub async fn setup_with_creator(creator_url: Option<&str>) -> TestHarness {
    setup_full(false, creator_url).await
}

async fn setup_full(pong_enabled: bool, creator_url: Option<&str>) -> TestHarness {
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
         match_reports, match_results, match_events RESTART IDENTITY CASCADE",
    )
    .execute(&pool)
    .await
    .expect("truncate tables");

    let config = AppConfig {
        db_url: test_url.to_string(),
        steam_api_key: "test".into(),
        app_id: 480,
        jwt_secret: "integration-test-secret-0123456789abcdef".into(),
        host: "127.0.0.1".into(),
        port: 0,
        match_accept_timeout_secs: 30,
        report_timeout_secs: 300,
        pair_cooldown_secs: 300,
        public_url: Some("https://lobby.example.com".into()),
        auth_dev_mode: true,
        jwt_ttl_secs: 86400,
        cors_origins: vec!["https://lobby.example.com".into()],
        game_modes: vec![
            ("ranked_1v1".into(), lobby_core::types::GameType::P2p),
            ("server_arena".into(), lobby_core::types::GameType::Server),
        ],
        gameserver_creator_url: creator_url.map(|s| s.to_string()),
        gameserver_alloc_timeout_secs: 60,
        gameserver_result_timeout_secs: 300,
        pong_enabled,
    };

    let (app, state) = build_app(config).await; // runs migrations + spawns ticker
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind port 0");
    let addr = listener.local_addr().expect("local addr");
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .expect("server");
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
