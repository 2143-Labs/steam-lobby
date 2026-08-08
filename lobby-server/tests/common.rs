#![allow(dead_code)] // shared harness: each test binary uses only the setup_* it needs

use std::sync::Arc;

use lobby_server::{build_app, AppConfig};
use sqlx::ConnectOptions;
use sqlx::PgPool;

/// Each #[sqlx::test] provides its own fresh, pre-migrated database and hands
/// the injected pool to the test fn (the `fn(Pool)` signature — sqlx closes
/// that pool with a `close().await` after the test, which yields the runtime
/// so the aborted server + ticker unwind BEFORE the post-test DROP DATABASE).
pub struct TestHarness {
    pub base_url: String, // "http://127.0.0.1:PORT"
    pub ws_url: String,   // "ws://127.0.0.1:PORT/ws"
    pub pool: PgPool,     // the injected test pool (≤5 conns, parented to sqlx's master pool)
    _state: Arc<lobby_server::AppState>, // keep alive so ticker keeps running
    _server: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Stop the ticker and abort the axum server. The actual unwind happens when
/// the runtime next polls the tasks — guaranteed to happen BEFORE the DROP
/// DATABASE, because sqlx's `fn(Pool)` wrapper awaits `pool.close()` between
/// the test fn returning and running cleanup.
impl Drop for TestHarness {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        self._server.abort();
    }
}

pub async fn setup(pool: PgPool) -> TestHarness {
    setup_full(pool, false, None, None, 15, 0).await
}

/// Harness with the pong game enabled (LOBBY_PONG = true) for p2p matches.
pub async fn setup_pong(pool: PgPool) -> TestHarness {
    setup_full(pool, true, None, None, 15, 0).await
}

/// Pong harness with the round 3-2-1 countdown enabled (90 ticks = 3s).
pub async fn setup_pong_countdown(pool: PgPool) -> TestHarness {
    setup_full(pool, true, None, None, 15, 90).await
}

/// Pong harness with a short START window (forfeit after `secs`).
pub async fn setup_pong_start_timeout(pool: PgPool, secs: u64) -> TestHarness {
    setup_full(pool, true, None, None, secs, 0).await
}

pub async fn setup_with_creator(pool: PgPool, creator_url: Option<&str>) -> TestHarness {
    setup_full(pool, false, creator_url, None, 15, 0).await
}

pub async fn setup_with_turn(pool: PgPool, turn_secret: Option<&str>) -> TestHarness {
    setup_full(pool, true, None, turn_secret.map(String::from), 15, 0).await
}


async fn setup_full(
    pool: PgPool,
    pong_enabled: bool,
    creator_url: Option<&str>,
    turn_secret: Option<String>,
    start_timeout_secs: u64,
    countdown_ticks: u32,
) -> TestHarness {
    // sqlx::test has already created + migrated the per-test database; the
    // URL is the one piece of info the test binary does not otherwise know.
    let db_url = pool.connect_options().to_url_lossy().to_string();
    // The server uses the injected pool itself so that sqlx's post-test
    // pool.close() (in the fn(Pool) wrapper) closes the server's connections
    // before the per-test database is dropped.
    let pool_clone = pool.clone();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut held_rx = shutdown_rx.clone(); // teardown-gate receiver (see below)

    let config = AppConfig {
        db_url,
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
        start_timeout_secs,
        pong_countdown_ticks: countdown_ticks,
        turn_secret,
        turn_uris: vec!["turn:turn.john2143.com:3478?transport=udp".into()],
        ticker_shutdown: Some(shutdown_rx),
        pool: Some(pool_clone),
    };

    let (app, state) = build_app(config).await; // runs migrations (no-op; already applied) + spawns ticker
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

    // Teardown gate: hold one checked-out connection and release it only
    // AFTER the test fn returns (signalled by the harness drop). sqlx's
    // post-test pool.close() waits on this permit, giving the runtime a
    // guaranteed window to unwind the aborted server + stopped ticker before
    // the DROP DATABASE — without it, the DROP can land while the ticker's
    // connection is still closing (intermittent "accessed by other users").
    let held = pool.acquire().await.expect("acquire held connection");
    tokio::spawn(async move {
        let _ = held_rx.changed().await; // fires when the harness drops
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        drop(held);
    });

    TestHarness {
        base_url: format!("http://{addr}"),
        ws_url: format!("ws://{addr}/ws"),
        pool,
        _state: state,
        _server: server,
        shutdown_tx,
    }
}
