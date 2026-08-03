use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::response::{IntoResponse, Redirect};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::state::AppState;
use lobby_core::traits::PlayerStore;

pub async fn health() -> &'static str {
    "ok"
}

#[derive(Deserialize)]
pub struct LoginQuery {
    return_to: Option<String>,
}

/// True if `return_to` is safe to redirect to: a same-origin path, or an
/// absolute URL whose origin matches `public_url` (when set).
fn validate_return_to(return_to: &str, public_url: Option<&str>) -> bool {
    // Relative same-origin path: "/dashboard" ok; "//evil.com" and "/\evil.com" are not.
    if return_to.starts_with('/') && !return_to.starts_with("//") && !return_to.starts_with("/\\") {
        return true;
    }
    // Absolute: must match the configured public origin exactly.
    let Some(pub_url) = public_url else { return false };
    match (url::Url::parse(return_to), url::Url::parse(pub_url)) {
        (Ok(a), Ok(b)) => a.origin() == b.origin(),
        _ => false,
    }
}

pub async fn steam_login(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LoginQuery>,
) -> impl IntoResponse {
    let return_to = query.return_to.unwrap_or_else(|| "/".to_string());
    if !validate_return_to(&return_to, state.public_url.as_deref()) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid_return_to"})),
        )
            .into_response();
    }
    let realm = state.public_url.as_deref().unwrap_or(&return_to);
    let redirect_url = state.steam_auth.openid_redirect_url(&return_to, realm);
    Redirect::temporary(&redirect_url).into_response()
}

pub async fn steam_callback(
    State(state): State<Arc<AppState>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let return_to = query
        .get("return_to")
        .cloned()
        .unwrap_or_else(|| "/".to_string());
    if !validate_return_to(&return_to, state.public_url.as_deref()) {
        return (
            axum::http::StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid_return_to"})),
        )
            .into_response();
    }
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

#[cfg(test)]
mod tests {
    use super::validate_return_to;

    #[test]
    fn validate_return_to_table() {
        let cases: &[(&str, Option<&str>, bool)] = &[
            ("/dashboard", Some("https://lobby.example.com"), true),
            ("/dashboard", None, true),
            ("//evil.com/x", None, false),
            ("/\\evil.com", None, false),
            ("", None, false),
            ("javascript:alert(1)", None, false),
            ("https://evil.com/x", Some("https://lobby.example.com"), false),
            ("https://lobby.example.com/cb", Some("https://lobby.example.com"), true),
            ("https://lobby.example.com/cb", None, false),
            ("https://lobby.example.com/cb", Some("https://lobby.example.com/"), true),
        ];
        for (return_to, public_url, expected) in cases {
            assert_eq!(
                validate_return_to(return_to, *public_url),
                *expected,
                "return_to={return_to:?}, public_url={public_url:?}"
            );
        }
    }
}
