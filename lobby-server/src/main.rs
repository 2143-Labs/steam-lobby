//! Binary entrypoint: loads env-var config, builds the app, and serves it
//! with a graceful shutdown. Helpers: `parse_game_modes` and `shutdown_signal`.
use lobby_server::{AppConfig, build_app};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = dotenvy::dotenv();
    tracing_subscriber::fmt::init();

    let host = std::env::var("LOBBY_HOST").unwrap_or_else(|_| "0.0.0.0".into());
    let port: u16 = std::env::var("LOBBY_PORT")
        .unwrap_or_else(|_| "8080".into())
        .parse()
        .expect("LOBBY_PORT");

    let config = AppConfig {
        db_url: std::env::var("DATABASE_URL").expect("DATABASE_URL"),
        steam_api_key: std::env::var("STEAM_API_KEY").unwrap_or_default(),
        app_id: std::env::var("STEAM_APP_ID")
            .unwrap_or_else(|_| "480".into())
            .parse()
            .expect("STEAM_APP_ID"),
        jwt_secret: std::env::var("JWT_SECRET").expect("JWT_SECRET"),
        host: host.clone(),
        port,
        match_accept_timeout_secs: std::env::var("MATCH_ACCEPT_TIMEOUT_S")
            .unwrap_or_else(|_| "30".into())
            .parse()
            .unwrap_or(30),
        report_timeout_secs: std::env::var("REPORT_TIMEOUT_S")
            .unwrap_or_else(|_| "300".into())
            .parse()
            .unwrap_or(300),
        pair_cooldown_secs: std::env::var("LOBBY_PAIR_COOLDOWN_S")
            .unwrap_or_else(|_| "300".into())
            .parse()
            .unwrap_or(300),
        public_url: std::env::var("PUBLIC_URL").ok().filter(|s| !s.is_empty()),
        auth_dev_mode: std::env::var("AUTH_DEV_MODE")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(false),
        jwt_ttl_secs: std::env::var("JWT_TTL_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(86400),
        cors_origins: std::env::var("CORS_ORIGINS")
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_default(),
        game_modes: parse_game_modes(
            &std::env::var("GAME_MODES")
                .unwrap_or_else(|_| "ranked_1v1:p2p,server_arena:server".into()),
        ),
        gameserver_creator_url: std::env::var("GAMESERVER_CREATOR_URL")
            .ok()
            .filter(|s| !s.is_empty()),
        gameserver_alloc_timeout_secs: std::env::var("GAMESERVER_ALLOC_TIMEOUT_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60),
        gameserver_result_timeout_secs: std::env::var("GAMESERVER_RESULT_TIMEOUT_S")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300),
        pong_enabled: std::env::var("LOBBY_PONG")
            .map(|v| v == "true" || v == "1")
            .unwrap_or(true),
        start_timeout_secs: std::env::var("LOBBY_START_TIMEOUT_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(15),
        pong_countdown_ticks: std::env::var("LOBBY_PONG_COUNTDOWN_TICKS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(90),
        turn_secret: std::env::var("LOBBY_TURN_SECRET")
            .ok()
            .filter(|s| !s.is_empty()),
        turn_uris: std::env::var("LOBBY_TURN_URIS")
            .map(|v| {
                v.split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect()
            })
            .unwrap_or_else(|_| vec!["turn:turn.john2143.com:3478?transport=udp".into()]),
        temporal_address: std::env::var("TEMPORAL_ADDRESS")
            .unwrap_or_else(|_| "http://localhost:7233".into()),
        temporal_namespace: std::env::var("TEMPORAL_NAMESPACE").unwrap_or_else(|_| "pvp".into()),
        temporal_task_queue: std::env::var("TEMPORAL_TASK_QUEUE")
            .unwrap_or_else(|_| "lobby".into()),
        ticker_shutdown: None,
        temporal_disabled: false,
        pool: None,
        discord_client_id: std::env::var("DISCORD_CLIENT_ID").ok().filter(|s| !s.is_empty()),
        discord_client_secret: std::env::var("DISCORD_CLIENT_SECRET")
            .ok()
            .filter(|s| !s.is_empty()),
        au2143_client_id: std::env::var("AU2143_CLIENT_ID").ok().filter(|s| !s.is_empty()),
        au2143_client_secret: std::env::var("AU2143_CLIENT_SECRET")
            .ok()
            .filter(|s| !s.is_empty()),
        au2143_issuer: std::env::var("AU2143_ISSUER").unwrap_or_else(|_| "https://au.2143.me".into()),
        au2143_authorize_url: std::env::var("AU2143_AUTHORIZE_URL")
            .ok()
            .filter(|s| !s.is_empty()),
        au2143_token_url: std::env::var("AU2143_TOKEN_URL")
            .ok()
            .filter(|s| !s.is_empty()),
        au2143_userinfo_url: std::env::var("AU2143_USERINFO_URL")
            .ok()
            .filter(|s| !s.is_empty()),
        provider_overrides: vec![],
    };

    let (app, _state) = build_app(config).await;
    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!("listening on {host}:{port}");
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await?;
    Ok(())
}

/// Parse `GAME_MODES` (`mode:type,mode:type`) into (mode, GameType) pairs.
/// Unknown type tokens are logged and skipped — a bad mode must not kill the server.
fn parse_game_modes(s: &str) -> Vec<(String, lobby_core::types::GameType)> {
    s.split(',')
        .filter_map(|pair| {
            let mut it = pair.split(':');
            let name = it.next()?.trim();
            let ty = it.next()?.trim();
            if name.is_empty() {
                return None;
            }
            let game_type = match ty {
                "p2p" => lobby_core::types::GameType::P2p,
                "server" => lobby_core::types::GameType::Server,
                other => {
                    tracing::warn!(
                        "GAME_MODES: unknown game type '{other}' for mode '{name}' — skipping"
                    );
                    return None;
                }
            };
            Some((name.to_string(), game_type))
        })
        .collect()
}

/// Wait for SIGINT (ctrl-c) or SIGTERM, then let axum drain in-flight connections.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install ctrl-c handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received, draining in-flight connections");
}
