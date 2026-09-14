#![allow(dead_code)] // shared harness: each test binary uses only the setup_* it needs

use std::sync::Arc;

use lobby_server::{AppConfig, build_app};
use sqlx::ConnectOptions;
use sqlx::PgPool;

/// Each #[sqlx::test] provides its own fresh, pre-migrated database and hands
/// the injected pool to the test fn (the `fn(Pool)` signature — sqlx closes
/// that pool with a `close().await` after the test, which yields the runtime
/// so the aborted server + ticker unwind BEFORE the post-test DROP DATABASE).
pub struct TestHarness {
    pub base_url: String,                // "http://127.0.0.1:PORT"
    pub ws_url: String,                  // "ws://127.0.0.1:PORT/ws"
    pub pool: PgPool, // the injected test pool (≤5 conns, parented to sqlx's master pool)
    pub state: Arc<lobby_server::AppState>, // keep alive so ticker keeps running
    _server: tokio::task::JoinHandle<()>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

/// Stop the ticker and abort the axum server. The actual unwind happens when
/// the runtime next polls the tasks — guaranteed to happen BEFORE the DROP
/// DATABASE, because sqlx's `fn(Pool)` wrapper awaits `pool.close()` between
/// the test fn returning and running cleanup.
impl Drop for TestHarness {
    fn drop(&mut self) {
        // Stop the Temporal worker so it stops polling its queue BEFORE the
        // test DB drops (its matchmaker workflow would otherwise keep
        // scheduling activities against the closing pool).
        if let Some(h) = self
            .state
            .temporal_shutdown
            .write()
            .ok()
            .and_then(|mut g| g.take())
        {
            h();
        }
        let _ = self.shutdown_tx.send(true);
        self._server.abort();
    }
}

impl TestHarness {
    /// Describe a workflow execution by ID and return its status string
    /// ("Running", "Completed", "Failed", ...). `None` if Temporal is down or
    /// the workflow doesn't exist.
    pub async fn workflow_status(&self, workflow_id: &str) -> Option<String> {
        let client = self.state.temporal.read().ok()?.clone()?;
        let handle =
            client.get_workflow_handle::<temporalio_client::UntypedWorkflow>(workflow_id);
        handle
            .describe(Default::default())
            .await
            .ok()
            .map(|d| format!("{:?}", d.status()))
    }

    /// Seed a live native principal and return `(user_id, session_id, bearer)`.
    /// This crosses only the real database/session-token boundary used by HTTP
    /// and WebSocket authentication.
    pub async fn native_principal(&self, steam_id: u64) -> (uuid::Uuid, uuid::Uuid, String) {
        let user_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO users (steam_id, display_name, primary_provider) \
             VALUES ($1, $2, 'steam') \
             ON CONFLICT (steam_id) DO UPDATE SET display_name=EXCLUDED.display_name \
             RETURNING id",
        )
        .bind(steam_id as i64)
        .bind(format!("Player {steam_id}"))
        .fetch_one(&self.pool)
        .await
        .expect("seed native user");
        sqlx::query(
            "INSERT INTO player_state (user_id,state,last_heartbeat) VALUES($1,'InMenus',NOW()) \
             ON CONFLICT(user_id) DO NOTHING",
        )
        .bind(user_id)
        .execute(&self.pool)
        .await
        .expect("seed native player state");
        let session_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO native_sessions(user_id,expires_at) VALUES($1,NOW()+INTERVAL '1 hour') RETURNING session_id",
        )
        .bind(user_id)
        .fetch_one(&self.pool)
        .await
        .expect("seed native session");
        let token_version: i32 = sqlx::query_scalar("SELECT token_version FROM users WHERE id=$1")
            .bind(user_id)
            .fetch_one(&self.pool)
            .await
            .expect("read token version");
        let bearer = self
            .state
            .steam_auth
            .generate_native_token(user_id, session_id, token_version as u32)
            .expect("issue native token");
        (user_id, session_id, bearer)
    }
}

/// Public knobs for focused integration-test servers. Defaults preserve the
/// existing non-Temporal reference harness; tests opt into only the behavior
/// they exercise.
pub struct TestConfig {
    pub game_modes: Vec<&'static lobby_core::types::ModeSpec>,
    pub pong_enabled: bool,
    pub temporal_enabled: bool,
    pub auth_dev_mode: bool,
    /// Run the background maintenance ticker. Disable only when a test must
    /// hold admitted commands pending and drive the public drain API itself.
    pub ticker_enabled: bool,
    pub steam_backed_accounts_only: bool,
    pub public_url: Option<String>,
    pub cors_origins: Vec<String>,
    pub provider_overrides: Vec<lobby_server::auth_providers::ProviderConfig>,
    pub ranked_queue_enabled: bool,
    pub ranked_queue_lease_secs: u64,
    pub match_accept_timeout_secs: u64,
    pub umvc3_trying_timeout_secs: u64,
    pub umvc3_connect_timeout_secs: u64,
    pub umvc3_ready_timeout_secs: u64,
    pub umvc3_play_timeout_secs: u64,
    pub report_timeout_secs: u64,
    pub start_timeout_secs: u64,
    pub pong_countdown_ticks: u32,
    pub gameserver_creator_url: Option<String>,
    pub turn_secret: Option<String>,
}

impl Default for TestConfig {
    fn default() -> Self {
        Self {
            game_modes: vec![
                lobby_core::types::mode_spec("pong_1v1").unwrap(),
                lobby_core::types::mode_spec("server_arena").unwrap(),
            ],
            pong_enabled: false,
            temporal_enabled: false,
            auth_dev_mode: true,
            ticker_enabled: true,
            steam_backed_accounts_only: false,
            public_url: Some("https://lobby.example.com".into()),
            cors_origins: vec!["https://lobby.example.com".into()],
            provider_overrides: vec![],
            ranked_queue_enabled: true,
            ranked_queue_lease_secs: 45,
            match_accept_timeout_secs: 30,
            umvc3_trying_timeout_secs: 15,
            umvc3_connect_timeout_secs: 30,
            umvc3_ready_timeout_secs: 60,
            umvc3_play_timeout_secs: 7200,
            report_timeout_secs: 300,
            start_timeout_secs: 15,
            pong_countdown_ticks: 0,
            gameserver_creator_url: None,
            turn_secret: None,
        }
    }
}

pub async fn setup_with_config(pool: PgPool, config: TestConfig) -> TestHarness {
    setup_config_impl(pool, config).await
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
    setup_full_impl(
        pool,
        pong_enabled,
        creator_url,
        turn_secret,
        start_timeout_secs,
        countdown_ticks,
        None,
        vec![],
    )
    .await
}

/// Unique per-test task queue: each test gets its own worker + matchmaker
/// workflow, so parallel tests never cross-talk on workflow IDs.
fn unique_queue() -> String {
    format!(
        "lobby-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

/// Temporal pong harness with the round 3-2-1 countdown enabled.
pub async fn setup_temporal_pong_countdown(pool: PgPool) -> TestHarness {
    setup_temporal_full(pool, true, 15, 90).await
}

/// Temporal harness with a gameserver creator URL (server_arena tests: the
/// ticker still pairs Server-type matches and allocates gameservers).
pub async fn setup_temporal_with_creator(pool: PgPool, creator_url: Option<&str>) -> TestHarness {
    let queue = unique_queue();
    setup_full_impl(pool, false, creator_url, None, 15, 0, Some(queue), vec![]).await
}

/// Temporal harness with a TURN secret (webrtc tests).
pub async fn setup_temporal_with_turn(pool: PgPool, turn_secret: Option<&str>) -> TestHarness {
    let queue = unique_queue();
    setup_full_impl(
        pool,
        true,
        None,
        turn_secret.map(String::from),
        15,
        0,
        Some(queue),
        vec![],
    )
    .await
}

/// Harness with a live Temporal worker: the WS handlers signal workflows, the
/// worker's activities drive the SAME AppState the test built (per-test unique
/// task queue — parallel tests never share a worker or workflow IDs).
pub async fn setup_temporal(pool: PgPool) -> TestHarness {
    setup_temporal_full(pool, false, 15, 0).await
}

/// Temporal harness with the pong game enabled (p2p matches).
pub async fn setup_temporal_pong(pool: PgPool) -> TestHarness {
    setup_temporal_full(pool, true, 15, 0).await
}

/// Temporal pong harness with a short START window (workflow forfeit timer).
pub async fn setup_temporal_pong_start_timeout(pool: PgPool, secs: u64) -> TestHarness {
    setup_temporal_full(pool, true, secs, 0).await
}

async fn setup_temporal_full(
    pool: PgPool,
    pong_enabled: bool,
    start_timeout_secs: u64,
    countdown_ticks: u32,
) -> TestHarness {
    let queue = unique_queue();
    setup_full_impl(
        pool,
        pong_enabled,
        None,
        None,
        start_timeout_secs,
        countdown_ticks,
        Some(queue),
        vec![],
    )
    .await
}

/// Temporal harness with injected OAuth/OIDC provider configs (the provider
/// tests point them at a mock OAuth server).
pub async fn setup_temporal_with_auth(
    pool: PgPool,
    providers: Vec<lobby_server::auth_providers::ProviderConfig>,
) -> TestHarness {
    let queue = unique_queue();
    setup_full_impl(
        pool,
        true,
        None,
        None,
        15,
        0,
        Some(queue),
        providers,
    )
    .await
}

async fn setup_full_impl(
    pool: PgPool,
    pong_enabled: bool,
    creator_url: Option<&str>,
    turn_secret: Option<String>,
    start_timeout_secs: u64,
    countdown_ticks: u32,
    temporal_queue: Option<String>,
    providers: Vec<lobby_server::auth_providers::ProviderConfig>,
) -> TestHarness {
    let mut config = TestConfig::default();
    config.pong_enabled = pong_enabled;
    config.gameserver_creator_url = creator_url.map(str::to_owned);
    config.turn_secret = turn_secret;
    config.start_timeout_secs = start_timeout_secs;
    config.pong_countdown_ticks = countdown_ticks;
    config.temporal_enabled = temporal_queue.is_some();
    config.provider_overrides = providers;
    setup_config_impl_with_queue(pool, config, temporal_queue).await
}

async fn setup_config_impl(pool: PgPool, config: TestConfig) -> TestHarness {
    let temporal_queue = config.temporal_enabled.then(unique_queue);
    setup_config_impl_with_queue(pool, config, temporal_queue).await
}

async fn setup_config_impl_with_queue(
    pool: PgPool,
    test_config: TestConfig,
    temporal_queue: Option<String>,
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
    // A receiver whose sender is already gone makes the ticker's shutdown arm
    // immediately ready, without sleeps or timing races in tests that need to
    // inspect the admitted-but-not-applied state.
    let ticker_shutdown = if test_config.ticker_enabled {
        shutdown_rx
    } else {
        let (ticker_tx, ticker_rx) = tokio::sync::watch::channel(false);
        drop(ticker_tx);
        ticker_rx
    };

    let config = AppConfig {
        db_url,
        steam_api_key: "test".into(),
        app_id: 480,
        jwt_secret: "integration-test-secret-0123456789abcdef".into(),
        host: "127.0.0.1".into(),
        port: 0,
        match_accept_timeout_secs: test_config.match_accept_timeout_secs,
        report_timeout_secs: test_config.report_timeout_secs,
        pair_cooldown_secs: 300,
        public_url: test_config.public_url,
        auth_dev_mode: test_config.auth_dev_mode,
        steam_backed_accounts_only: test_config.steam_backed_accounts_only,
        ranked_queue_enabled: test_config.ranked_queue_enabled,
        ranked_queue_lease_secs: test_config.ranked_queue_lease_secs,
        umvc3_trying_timeout_secs: test_config.umvc3_trying_timeout_secs,
        umvc3_connect_timeout_secs: test_config.umvc3_connect_timeout_secs,
        umvc3_ready_timeout_secs: test_config.umvc3_ready_timeout_secs,
        umvc3_play_timeout_secs: test_config.umvc3_play_timeout_secs,
        jwt_ttl_secs: 86400,
        cors_origins: test_config.cors_origins,
        game_modes: test_config.game_modes,
        gameserver_creator_url: test_config.gameserver_creator_url,
        gameserver_alloc_timeout_secs: 60,
        gameserver_result_timeout_secs: 300,
        pong_enabled: test_config.pong_enabled,
        start_timeout_secs: test_config.start_timeout_secs,
        pong_countdown_ticks: test_config.pong_countdown_ticks,
        turn_secret: test_config.turn_secret,
        turn_uris: vec!["turn:turn.john2143.com:3478?transport=udp".into()],
        temporal_address: "http://localhost:7233".into(),
        temporal_namespace: "pvp".into(),
        temporal_task_queue: temporal_queue
            .clone()
            .unwrap_or_else(|| "lobby-test".into()),
        ticker_shutdown: Some(ticker_shutdown),
        temporal_disabled: temporal_queue.is_none(),
        pool: Some(pool_clone),
        discord_client_id: None,
        discord_client_secret: None,
        au2143_client_id: None,
        au2143_client_secret: None,
        au2143_issuer: "https://au.2143.me".into(),
        au2143_authorize_url: None,
        au2143_token_url: None,
        au2143_userinfo_url: None,
        provider_overrides: test_config.provider_overrides,
    };

    let (app, state) = build_app(config).await; // runs migrations (no-op; already applied) + spawns ticker

    // Temporal harness: build_app already started the in-process worker on its
    // own OS thread (temporal_disabled=false). Wait for it to connect and hand
    // the client to state.temporal so the WS handlers take the signal path.
    if let Some(_q) = &temporal_queue {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
        while std::time::Instant::now() < deadline {
            if state.temporal.read().ok().is_some_and(|g| g.is_some()) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(
            state.temporal.read().ok().is_some_and(|g| g.is_some()),
            "temporal worker failed to connect within 15s (is `just temporal-up` running?)"
        );
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind port 0");
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
        state,
        _server: server,
        shutdown_tx,
    }
}
