use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Redirect};
use axum::Json;
use serde::{Deserialize, Serialize};

use lobby_core::traits::PlayerStore;
use crate::state::AppState;

pub async fn health() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
pub struct LoginQuery {
    return_to: Option<String>,
}

pub async fn steam_login(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LoginQuery>,
) -> impl IntoResponse {
    let return_to = query.return_to.unwrap_or_else(|| "/".to_string());
    let redirect_url = state
        .steam_auth
        .openid_redirect_url(&return_to, &return_to);
    Redirect::temporary(&redirect_url)
}

#[derive(Deserialize)]
pub struct CallbackParams {
    #[serde(flatten)]
    params: HashMap<String, String>,
    return_to: Option<String>,
}

pub async fn steam_callback(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    tracing::info!("OpenID callback: {:?}", query.keys());

    let steam_id = match state.steam_auth.verify_openid(&query).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("OpenID verification failed: {e}");
            return axum::http::StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Upsert player
    let display_name = state
        .steam_auth
        .get_player_summary(steam_id)
        .await
        .unwrap_or_else(|_| "Unknown".into());

    let _ = state.store.upsert_player(steam_id, &display_name).await;

    // Generate JWT
    let token = match state.steam_auth.generate_session_token(steam_id) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("JWT generation failed: {e}");
            return axum::http::StatusCode::INTERNAL_SERVER_ERROR.into_response();
        }
    };

    let return_to = query
        .get("return_to")
        .cloned()
        .unwrap_or_else(|| "/".to_string());

    Redirect::temporary(&format!("{return_to}?token={token}")).into_response()
}

#[derive(Deserialize)]
pub struct TicketAuthBody {
    ticket: String,
}

#[derive(Serialize)]
pub struct TokenResponse {
    token: String,
}

pub async fn ticket_auth(
    State(state): State<Arc<AppState>>,
    Json(body): Json<TicketAuthBody>,
) -> impl IntoResponse {
    let steam_id = match state.steam_auth.verify_ticket(&body.ticket).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("Ticket verification failed: {e}");
            return (
                axum::http::StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "auth_failed"})),
            )
                .into_response();
        }
    };

    let token = match state.steam_auth.generate_session_token(steam_id) {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("JWT generation failed: {e}");
            return (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    (axum::http::StatusCode::OK, Json(TokenResponse { token })).into_response()
}

#[derive(Deserialize)]
pub struct TestTokenBody {
    steam_id: u64,
}

pub async fn test_token(
    State(state): State<Arc<AppState>>,
    Json(body): Json<TestTokenBody>,
) -> impl IntoResponse {
    let token = state
        .steam_auth
        .generate_session_token(body.steam_id)
        .unwrap();
    (axum::http::StatusCode::OK, Json(TokenResponse { token })).into_response()
}
