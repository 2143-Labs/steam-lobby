use std::sync::Arc;

use axum::extract::ConnectInfo;
use axum::response::IntoResponse;
use axum::{extract::State, routing::get, Router};
use tower_http::cors::CorsLayer;
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;

mod db;
mod rate_limit;
mod routes;
mod state;
mod steam_auth;
mod ticker;
mod ws;

use db::PostgresStore;
use rate_limit::RateLimiter;
use state::DefaultCallbacks;
use steam_auth::SteamAuthService;

pub use state::AppState; // re-exported so integration tests can name the type

pub struct AppConfig {
    pub db_url: String,
    pub steam_api_key: String,
    pub app_id: u32,
    pub jwt_secret: String,
    pub host: String,
    pub port: u16,
    pub match_accept_timeout_secs: u64,
    pub report_timeout_secs: u64,
    pub public_url: Option<String>, // PUBLIC_URL; None = relative return_to only
    pub auth_dev_mode: bool,        // AUTH_DEV_MODE; true = /auth/test-token enabled
    pub jwt_ttl_secs: u64,
    pub cors_origins: Vec<String>,
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
            "" => tracing::warn!("STEAM_API_KEY not set — OpenID auth will work, ticket auth will not"),
            _ => tracing::info!(
                "auth mode: STEAM — ticket + OpenID verification against Steam (appid {})",
                config.app_id
            ),
        }
    }

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(10)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect(&config.db_url)
        .await
        .expect("database connection failed — check DATABASE_URL");
    sqlx::migrate!().run(&pool).await.expect("database migrations failed");

    let store = PostgresStore::new(pool);
    let steam_auth = SteamAuthService::new(config.steam_api_key.clone(), config.app_id, config.jwt_secret);
    let callbacks = DefaultCallbacks;
    let player_manager = lobby_core::player::PlayerManager::new(callbacks.clone());
    let matchmaking_queue = lobby_core::queue::MatchmakingQueue::new(callbacks.clone());
    let match_manager = lobby_core::match_lifecycle::MatchManager::new(
        callbacks,
        config.match_accept_timeout_secs,
        config.report_timeout_secs,
    );
    let state = Arc::new(AppState {
        player_manager,
        matchmaking_queue,
        match_manager,
        steam_auth,
        store,
        connections: tokio::sync::Mutex::new(std::collections::HashMap::new()),
        public_url: config.public_url.clone(),
        auth_dev_mode: config.auth_dev_mode,
        jwt_ttl_secs: config.jwt_ttl_secs,
        allowed_origins: config.cors_origins.clone(),
        openid_states: std::sync::Mutex::new(std::collections::HashMap::new()),
        ticket_limiter: RateLimiter::new(10, std::time::Duration::from_secs(60)),
        test_token_limiter: RateLimiter::new(20, std::time::Duration::from_secs(60)),
        next_generation: std::sync::atomic::AtomicU64::new(0),
    });

    tokio::spawn(ticker::tick_loop(state.clone()));

    let mut router = Router::new()
        .route("/", get(routes::index))
        .route("/health", get(routes::health))
        .route("/auth/steam/login", get(routes::steam_login))
        .route("/auth/steam/callback", get(routes::steam_callback))
        .route("/auth/ticket", axum::routing::post(routes::ticket_auth))
        .route("/auth/logout", axum::routing::post(routes::logout))
        .route(
            "/ws",
            get(|ws: axum::extract::WebSocketUpgrade,
                 State(app_state): axum::extract::State<Arc<AppState>>,
                 ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
                 headers: axum::http::HeaderMap| async move {
                if let Some(origin) = headers.get(axum::http::header::ORIGIN) {
                    let s = origin.to_str().unwrap_or_default();
                    let allowed = app_state.allowed_origins.iter().any(|a| a == s)
                        || (app_state.auth_dev_mode && origin.as_bytes() == b"null");
                    if !allowed {
                        return axum::http::StatusCode::FORBIDDEN.into_response();
                    }
                }
                ws.max_message_size(64 * 1024)
                    .max_frame_size(64 * 1024)
                    .on_upgrade(move |socket| ws::handle_ws(socket, app_state, peer))
            }),
        );

    if config.auth_dev_mode {
        router = router.route("/auth/test-token", axum::routing::post(routes::test_token));
    }

    let mut allowed: Vec<String> = config.cors_origins.clone();
    if let Some(pu) = &config.public_url {
        if let Ok(u) = url::Url::parse(pu) {
            allowed.push(u.origin().ascii_serialization());
        }
    }
    let allowed = Arc::new(allowed);
    let dev = config.auth_dev_mode;
    let cors = CorsLayer::new()
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
        ));

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
