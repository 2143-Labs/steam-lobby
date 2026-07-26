use std::sync::Arc;

use axum::{extract::State, routing::get, Router};
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;

mod db;
mod routes;
mod state;
mod steam_auth;
mod ticker;
mod ws;

use db::PostgresStore;
use state::AppState;
use steam_auth::SteamAuthService;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv(); // load .env if present, silent skip otherwise
    tracing_subscriber::fmt::init();

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL");
    let steam_api_key = std::env::var("STEAM_API_KEY").unwrap_or_default();
    let app_id: u32 = std::env::var("STEAM_APP_ID")
        .unwrap_or_else(|_| "480".into())
        .parse()
        .expect("STEAM_APP_ID");
    let jwt_secret = std::env::var("JWT_SECRET").expect("JWT_SECRET");
    let host = std::env::var("LOBBY_HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port: u16 = std::env::var("LOBBY_PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .expect("LOBBY_PORT");
    let match_accept_timeout: u64 = std::env::var("MATCH_ACCEPT_TIMEOUT_S")
        .unwrap_or_else(|_| "30".into())
        .parse()
        .unwrap_or(30);
    let report_timeout: u64 = std::env::var("REPORT_TIMEOUT_S")
        .unwrap_or_else(|_| "300".into())
        .parse()
        .unwrap_or(300);

    if steam_api_key.is_empty() {
        tracing::warn!("STEAM_API_KEY not set — OpenID auth will work, ticket auth will not");
    }

    let pool = sqlx::PgPool::connect(&db_url).await.expect("DB connection");
    sqlx::migrate!().run(&pool).await.expect("Migrations");

    let store = PostgresStore::new(pool);
    let steam_auth = SteamAuthService::new(steam_api_key, app_id, jwt_secret);
    let callbacks = state::DefaultCallbacks;
    let player_manager = lobby_core::player::PlayerManager::new(callbacks.clone());
    let matchmaking_queue = lobby_core::queue::MatchmakingQueue::new(callbacks.clone());
    let match_manager = lobby_core::match_lifecycle::MatchManager::new(
        callbacks,
        match_accept_timeout,
        report_timeout,
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

    // Build all routes BEFORE with_state
    let mut router = Router::new()
        .route("/health", get(routes::health))
        .route("/auth/steam/login", get(routes::steam_login))
        .route("/auth/steam/callback", get(routes::steam_callback))
        .route("/auth/ticket", axum::routing::post(routes::ticket_auth))
        .route(
            "/ws",
            get(
                |ws: axum::extract::WebSocketUpgrade,
                 State(app_state): axum::extract::State<Arc<AppState>>| async move {
                    ws.on_upgrade(move |socket| ws::handle_ws(socket, app_state))
                },
            ),
        );

    // Dev-mode test token endpoint (before with_state)
    if std::env::var("STEAM_API_KEY").unwrap_or_default() == "test" {
        router = router.route("/auth/test-token", axum::routing::post(routes::test_token));
    }

    let app = router
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!("listening on {host}:{port}");
    axum::serve(listener, app).await?;
    Ok(())
}
