//! Lobby server: `build_app` wires config + shared state into an axum Router.
//! Module map: `db/` PostgresStore impls, `gameserver` creator client,
//! `rate_limit` limiter, `routes` HTTP surface, `state` AppState, `steam_auth`
//! Steam ticket + JWT auth, `ticker` maintenance loop, `ws` WebSocket protocol.
use std::sync::Arc;

use axum::{Router, routing::get};
use tower_http::cors::CorsLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;

pub mod auth_providers;
mod db;
mod gameserver;
mod pong;
mod rate_limit;
mod routes;
mod state;
mod steam_auth;
mod temporal;
mod ticker;
mod turn;
mod ws;

use db::PostgresStore;
use rate_limit::RateLimiter;
use state::DefaultCallbacks;
use steam_auth::SteamAuthService;

use lobby_core::traits::QueueStore;
use lobby_core::types::GameType;
pub use state::AppState; // re-exported so integration tests can name the type
use state::RuntimeConfig;

pub struct AppConfig {
    pub db_url: String,
    pub steam_api_key: String,
    pub app_id: u32,
    pub jwt_secret: String,
    pub host: String,
    pub port: u16,
    pub match_accept_timeout_secs: u64,
    pub report_timeout_secs: u64,
    pub pair_cooldown_secs: u64, // LOBBY_PAIR_COOLDOWN_S; anti re-pair window after a match
    pub public_url: Option<String>, // PUBLIC_URL; None = relative return_to only
    pub auth_dev_mode: bool,     // AUTH_DEV_MODE; true = /auth/test-token enabled
    pub jwt_ttl_secs: u64,
    pub cors_origins: Vec<String>,
    pub game_modes: Vec<(String, GameType)>,
    pub gameserver_creator_url: Option<String>,
    pub gameserver_alloc_timeout_secs: u64,
    pub gameserver_result_timeout_secs: u64,
    pub pong_enabled: bool, // LOBBY_PONG; run p2p matches as server-authoritative pong
    pub start_timeout_secs: u64, // LOBBY_START_TIMEOUT_SECS; START window after both accept (forfeit)
    pub pong_countdown_ticks: u32, // LOBBY_PONG_COUNTDOWN_TICKS; 3-2-1 hold in 33ms ticks; 0 = disabled
    pub turn_secret: Option<String>, // LOBBY_TURN_SECRET; None => /internal/turn-credentials 503s
    pub turn_uris: Vec<String>,    // LOBBY_TURN_URIS; TURN URIs returned to clients
    pub temporal_address: String,  // TEMPORAL_ADDRESS; Temporal gRPC frontend (plaintext URI)
    pub temporal_namespace: String, // TEMPORAL_NAMESPACE; Temporal namespace for worker + client
    pub temporal_task_queue: String, // TEMPORAL_TASK_QUEUE; in-process worker's task queue
    pub ticker_shutdown: Option<tokio::sync::watch::Receiver<bool>>, // test-only: stop the maintenance loop
    pub temporal_disabled: bool, // test-only: skip spawning the in-process Temporal worker
    pub pool: Option<sqlx::PgPool>, // test-only: inject the per-test pool so sqlx's post-test close() tears down the server's connections
    // OAuth2/OIDC login providers (Step 8).
    pub discord_client_id: Option<String>,   // DISCORD_CLIENT_ID
    pub discord_client_secret: Option<String>, // DISCORD_CLIENT_SECRET
    pub au2143_client_id: Option<String>,    // AU2143_CLIENT_ID
    pub au2143_client_secret: Option<String>, // AU2143_CLIENT_SECRET
    pub au2143_issuer: String,               // AU2143_ISSUER, default https://au.2143.me
    pub au2143_authorize_url: Option<String>, // AU2143_AUTHORIZE_URL (discovery-failure override)
    pub au2143_token_url: Option<String>,    // AU2143_TOKEN_URL
    pub au2143_userinfo_url: Option<String>, // AU2143_USERINFO_URL
    /// Test-only: replace the env-built registry with these configs verbatim
    /// (the provider tests point them at a mock OAuth server).
    pub provider_overrides: Vec<crate::auth_providers::ProviderConfig>,
}

/// Build the full axum Router + shared state.
/// Caller binds a TcpListener and calls `axum::serve`.
pub async fn build_app(config: AppConfig) -> (Router, Arc<AppState>) {
    assert!(
        config.jwt_secret.len() >= 32,
        "JWT_SECRET must be at least 32 bytes"
    );
    assert!(
        config.jwt_secret != "dev-secret-change-me",
        "JWT_SECRET must not be the known placeholder"
    );

    if config.auth_dev_mode {
        tracing::info!("auth mode: TEST — /auth/test-token enabled");
    } else {
        match config.steam_api_key.as_str() {
            "" => tracing::warn!(
                "STEAM_API_KEY not set — OpenID auth will work, ticket auth will not"
            ),
            _ => tracing::info!(
                "auth mode: STEAM — ticket + OpenID verification against Steam (appid {})",
                config.app_id
            ),
        }
    }

    let pool = match config.pool.clone() {
        Some(p) => p,
        None => sqlx::postgres::PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(&config.db_url)
            .await
            .expect("database connection failed — check DATABASE_URL"),
    };
    sqlx::migrate!()
        .run(&pool)
        .await
        .expect("database migrations failed");

    let store = PostgresStore::new(pool);

    // Server-authoritative modes need a gameserver creator. With AUTH_DEV_MODE
    // and no explicit URL, fall back to the built-in dev mock creator.
    let (creator_url, mock_enabled) = match &config.gameserver_creator_url {
        Some(u) => (Some(u.clone()), false),
        None if config.auth_dev_mode
            && config
                .game_modes
                .iter()
                .any(|(_, t)| *t == GameType::Server) =>
        {
            (
                Some(format!(
                    "http://127.0.0.1:{}/internal/mock/allocate",
                    config.port
                )),
                true,
            )
        }
        None => (None, false),
    };
    if config
        .game_modes
        .iter()
        .any(|(_, t)| *t == GameType::Server)
        && creator_url.is_none()
    {
        tracing::warn!(
            "server-authoritative modes configured but GAMESERVER_CREATOR_URL is unset — their matches will all end Disputed"
        );
    }
    let callback_base = config
        .public_url
        .clone()
        .unwrap_or_else(|| format!("http://127.0.0.1:{}", config.port));
    if config
        .game_modes
        .iter()
        .any(|(_, t)| *t == GameType::Server)
        && config.public_url.is_none()
    {
        tracing::warn!(
            "PUBLIC_URL unset — result callbacks will use {callback_base}; set PUBLIC_URL in production"
        );
    }

    // The queue lives in Postgres and survives restarts; entries from a dead
    // session (no heartbeats) must not phantom-match on boot.
    if let Err(e) = store
        .remove_stale_queue_entries(chrono::Duration::zero())
        .await
    {
        tracing::warn!("failed to clear stale queue entries at startup: {e}");
    }
    // Built here: `config.jwt_secret` is moved into SteamAuthService below,
    // after which the whole `config` can no longer be borrowed.
    let cors = cors_layer(&config);
    let steam_auth = SteamAuthService::new(
        config.steam_api_key.clone(),
        config.app_id,
        config.jwt_secret,
    );

    let callbacks = DefaultCallbacks;
    let player_manager = lobby_core::player::PlayerManager::new(callbacks.clone());
    let match_manager = lobby_core::match_lifecycle::MatchManager::new(callbacks);
    let http = reqwest::Client::new();
    let auth_providers = if !config.provider_overrides.is_empty() {
        std::sync::Arc::new(crate::auth_providers::AuthProviderRegistry {
            providers: config.provider_overrides.clone(),
        })
    } else {
        std::sync::Arc::new(
            crate::auth_providers::build(
                config.discord_client_id.clone(),
                config.discord_client_secret.clone(),
                config.au2143_client_id.clone(),
                config.au2143_client_secret.clone(),
                config.au2143_issuer.clone(),
                match (
                    &config.au2143_authorize_url,
                    &config.au2143_token_url,
                    &config.au2143_userinfo_url,
                ) {
                    (Some(a), Some(t), Some(u)) => Some((a.clone(), t.clone(), u.clone())),
                    _ => None,
                },
                &http,
            )
            .await,
        )
    };
    let state = Arc::new(AppState {
        player_manager,
        match_manager,
        steam_auth,
        store,
        game_modes: config.game_modes.clone(),
        http: http.clone(),
        gameserver: crate::gameserver::GameserverClient {
            creator_url,
            callback_base,
            client: http,
        },
        auth_providers,
        gameserver_alloc_timeout_secs: config.gameserver_alloc_timeout_secs,
        gameserver_result_timeout_secs: config.gameserver_result_timeout_secs,
        connections: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        config: RuntimeConfig {
            public_url: config.public_url.clone(),
            auth_dev_mode: config.auth_dev_mode,
            jwt_ttl_secs: config.jwt_ttl_secs,
            cors_origins: config.cors_origins.clone(),
            pong_enabled: config.pong_enabled,
            start_timeout_secs: config.start_timeout_secs,
            pong_countdown_ticks: config.pong_countdown_ticks,
            turn_secret: config.turn_secret.clone(),
            turn_uris: config.turn_uris.clone(),
            match_accept_timeout_secs: config.match_accept_timeout_secs,
            report_timeout_secs: config.report_timeout_secs,
            pair_cooldown_secs: config.pair_cooldown_secs,
            temporal_address: config.temporal_address.clone(),
            temporal_namespace: config.temporal_namespace.clone(),
            temporal_task_queue: config.temporal_task_queue.clone(),
        },
        openid_states: parking_lot::Mutex::new(std::collections::HashMap::new()),
        pong_games: parking_lot::Mutex::new(std::collections::HashMap::new()),
        ticket_limiter: RateLimiter::new(10, std::time::Duration::from_secs(60)),
        test_token_limiter: RateLimiter::new(20, std::time::Duration::from_secs(60)),
        next_generation: std::sync::atomic::AtomicU64::new(0),
        temporal: std::sync::RwLock::new(None),
        temporal_shutdown: std::sync::RwLock::new(None),
    });

    tokio::spawn(ticker::tick_loop(state.clone(), config.ticker_shutdown));

    // The Temporal worker runs in-process on its own OS thread + multi-thread
    // tokio runtime (the SDK's Worker::run is !Send — LocalSet-based). On
    // connect failure it logs and exits, leaving state.temporal None. The
    // worker's shutdown handle is stored on AppState so the test harness can
    // stop it at teardown (production never touches it).
    if !config.temporal_disabled
        && let Some(rx) = crate::temporal::start_temporal(state.clone())
    {
        let holder = state.clone();
        tokio::spawn(async move {
            if let Ok(handle) = rx.await
                && let Ok(mut g) = holder.temporal_shutdown.write()
            {
                *g = Some(handle);
            }
        });
    }

    let mut router = Router::new()
        .route(
            "/auth/{provider}/login",
            get(routes::auth_login),
        )
        .route(
            "/auth/{provider}/callback",
            get(routes::auth_callback),
        )
        .route("/pong-wrtc.mjs", get(routes::pong_wrtc))
        .route("/", get(routes::index))
        .route("/pong-sim.mjs", get(routes::pong_sim))
        .route("/pong-rollback.mjs", get(routes::pong_rollback))
        .route("/health", get(routes::health))
        .route("/modes", get(routes::modes))
        .route("/auth/config", get(routes::auth_config))
        .route(
            "/internal/game-result/{token}/{secret}",
            axum::routing::post(routes::game_result),
        )
        .route("/auth/steam/login", get(routes::steam_login))
        .route("/auth/steam/callback", get(routes::steam_callback))
        .route("/auth/ticket", axum::routing::post(routes::ticket_auth))
        .route("/internal/turn-credentials", get(routes::turn_credentials))
        .route("/auth/logout", axum::routing::post(routes::logout))
        .route("/ws", get(ws::ws_route));

    if config.auth_dev_mode {
        router = router.route("/auth/test-token", axum::routing::post(routes::test_token));
    }

    if mock_enabled {
        router = router.route(
            "/internal/mock/allocate",
            axum::routing::post(routes::mock_allocate),
        );
    }

    let app = router
        .layer(cors)
        .layer(SetResponseHeaderLayer::overriding(
            axum::http::header::REFERRER_POLICY,
            axum::http::HeaderValue::from_static("no-referrer"),
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    (app, state)
}

/// CORS layer from config: GET/POST, content-type + authorization headers,
/// origins = cors_origins + the public_url origin; dev mode allows null origin.
fn cors_layer(config: &AppConfig) -> CorsLayer {
    let mut allowed: Vec<String> = config.cors_origins.clone();
    if let Some(pu) = &config.public_url
        && let Ok(u) = url::Url::parse(pu)
    {
        allowed.push(u.origin().ascii_serialization());
    }
    let allowed = Arc::new(allowed);
    let dev = config.auth_dev_mode;
    CorsLayer::new()
        .allow_methods([axum::http::Method::GET, axum::http::Method::POST])
        .allow_headers([
            axum::http::header::CONTENT_TYPE,
            axum::http::header::AUTHORIZATION,
        ])
        .allow_origin(tower_http::cors::AllowOrigin::predicate(
            move |origin: &axum::http::HeaderValue, _| {
                let s = origin.to_str().unwrap_or_default();
                allowed.iter().any(|a| a == s) || (dev && origin.as_bytes() == b"null")
            },
        ))
}
