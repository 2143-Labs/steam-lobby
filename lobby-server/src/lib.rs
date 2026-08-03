use std::sync::Arc;

use axum::{extract::State, routing::get, Router};
use sqlx::PgPool;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

mod db;
mod routes;
mod state;
mod steam_auth;
mod ticker;
mod ws;

use db::PostgresStore;
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
}

/// Build the full axum Router + shared state.
/// Caller binds a TcpListener and calls `axum::serve`.
pub async fn build_app(config: AppConfig) -> (Router, Arc<AppState>) {
    if config.steam_api_key.is_empty() {
        tracing::warn!("STEAM_API_KEY not set — OpenID auth will work, ticket auth will not");
    }
    let pool = PgPool::connect(&config.db_url).await.expect("DB connection");
    sqlx::migrate!().run(&pool).await.expect("Migrations");

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
    });

    tokio::spawn(ticker::tick_loop(state.clone()));

    let mut router = Router::new()
        .route("/health", get(routes::health))
        .route("/auth/steam/login", get(routes::steam_login))
        .route("/auth/steam/callback", get(routes::steam_callback))
        .route("/auth/ticket", axum::routing::post(routes::ticket_auth))
        .route(
            "/ws",
            get(|ws: axum::extract::WebSocketUpgrade,
                 State(app_state): axum::extract::State<Arc<AppState>>| async move {
                ws.on_upgrade(move |socket| ws::handle_ws(socket, app_state))
            }),
        );

    if config.steam_api_key == "test" {
        router = router.route("/auth/test-token", axum::routing::post(routes::test_token));
    }

    let app = router
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state.clone());

    (app, state)
}
