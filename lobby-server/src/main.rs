use lobby_server::{build_app, AppConfig};

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
    };

    let (app, _state) = build_app(config).await;
    let listener = tokio::net::TcpListener::bind(format!("{host}:{port}")).await?;
    tracing::info!("listening on {host}:{port}");
    axum::serve(listener, app).with_graceful_shutdown(shutdown_signal()).await?;
    Ok(())
}

/// Wait for SIGINT (ctrl-c) or SIGTERM, then let axum drain in-flight connections.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c().await.expect("failed to install ctrl-c handler");
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
