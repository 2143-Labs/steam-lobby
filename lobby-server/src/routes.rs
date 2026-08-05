//! HTTP surface: health + embedded demo index, Steam OpenID login/callback,
//! ticket auth, logout, the internal gameserver result webhook, and the
//! dev-only test-token + mock creator endpoints.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{Html, IntoResponse, Redirect};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::state::{AppState, OpenIdState};
use lobby_core::traits::{MatchStore, PlayerStore};

pub async fn health() -> &'static str {
    "ok"
}

/// The zero-dependency browser demo (`web/index.html`), embedded at build time
/// so it works from any CWD and inside the Docker image.
pub async fn index() -> Html<&'static str> {
    Html(include_str!("../../web/index.html"))
}

#[derive(Deserialize)]
pub struct LoginQuery {
    return_to: Option<String>,
}

/// True if `return_to` is safe to redirect to: a same-origin path without
/// query/fragment, or an absolute URL whose origin matches `public_url` and
/// which has no fragment (the token fragment is appended by the server).
fn validate_return_to(return_to: &str, public_url: Option<&str>) -> bool {
    // Relative same-origin path: "/dashboard" ok; "//evil.com" and "/\evil.com" are not.
    if return_to.starts_with('/') && !return_to.starts_with("//") && !return_to.starts_with("/\\") {
        // A `?` would smuggle params into the callback/redirect; a `#` would
        // swallow the fragment token.
        return !return_to.contains('?') && !return_to.contains('#');
    }
    // Absolute: must match the configured public origin exactly.
    let Some(pub_url) = public_url else { return false };
    if return_to.contains('#') {
        return false;
    }
    match (url::Url::parse(return_to), url::Url::parse(pub_url)) {
        (Ok(a), Ok(b)) => a.origin() == b.origin(),
        _ => false,
    }
}

/// Append the session token as a URL fragment — never a query param (not sent
/// in Referer, not in access logs, not in history as a server-visible value).
fn build_token_redirect(return_to: &str, token: &str) -> String {
    format!("{return_to}#token={token}")
}

pub async fn steam_login(
    State(state): State<Arc<AppState>>,
    Query(query): Query<LoginQuery>,
) -> impl IntoResponse {
    let return_to = query.return_to.unwrap_or_else(|| "/".to_string());
    if !validate_return_to(&return_to, state.config.public_url.as_deref()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid_return_to"})),
        )
            .into_response();
    }
    // Steam requires an absolute callback URL, so OpenID login needs PUBLIC_URL.
    let Some(public_url) = state.config.public_url.as_deref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "public_url_required"})),
        )
            .into_response();
    };

    // Issue a one-time login state bound to this return_to.
    let login_state = uuid::Uuid::new_v4().to_string();
    {
        let mut states = state.openid_states.lock().unwrap();
        states.retain(|_, s| s.created_at.elapsed() < Duration::from_secs(600));
        if states.len() >= 4096 {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(serde_json::json!({"error": "state_limit"})),
            )
                .into_response();
        }
        states.insert(
            login_state.clone(),
            OpenIdState {
                return_to: return_to.clone(),
                created_at: Instant::now(),
            },
        );
    }

    let redirect_url = state
        .steam_auth
        .openid_redirect_url(public_url, &login_state, &return_to);
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
    let state_param = query.get("state").cloned().unwrap_or_default();

    // Consume the one-time login state (prevents replay of a callback).
    let stored = {
        let mut states = state.openid_states.lock().unwrap();
        states.remove(&state_param)
    };
    let Some(stored) = stored else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid_state"})),
        )
            .into_response();
    };
    if stored.created_at.elapsed() >= Duration::from_secs(600) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "state_expired"})),
        )
            .into_response();
    }
    if stored.return_to != return_to {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid_return_to"})),
        )
            .into_response();
    }
    // Defense in depth: re-validate the (now state-bound) return_to.
    if !validate_return_to(&return_to, state.config.public_url.as_deref()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid_return_to"})),
        )
            .into_response();
    }

    tracing::info!("OpenID callback: {:?}", query.keys());

    let steam_id = match state.steam_auth.verify_openid(&query).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("OpenID verification failed: {e}");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    // Upsert player
    let display_name = state
        .steam_auth
        .get_player_summary(steam_id)
        .await
        .unwrap_or_else(|_| "Unknown".into());

    let _ = state.store.upsert_player(steam_id, &display_name).await;

    // Generate JWT bound to the current token_version (DB error fails closed).
    let version = match state.store.get_token_version(steam_id).await {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };
    let token = match state
        .steam_auth
        .generate_session_token(steam_id, version, state.config.jwt_ttl_secs)
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("JWT generation failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    Redirect::temporary(&build_token_redirect(&return_to, &token)).into_response()
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
    ConnectInfo(ip): ConnectInfo<std::net::SocketAddr>,
    Json(body): Json<TicketAuthBody>,
) -> impl IntoResponse {
    if !state.ticket_limiter.check(ip.ip()) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({"error": "rate_limited"})),
        )
            .into_response();
    }

    let steam_id = match state.steam_auth.verify_ticket(&body.ticket).await {
        Ok(id) => id,
        Err(e) => {
            tracing::error!("Ticket verification failed: {e}");
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "auth_failed"})),
            )
                .into_response();
        }
    };

    // DB error fails closed — never mint with version 0.
    let version = match state.store.get_token_version(steam_id).await {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    let token = match state
        .steam_auth
        .generate_session_token(steam_id, version, state.config.jwt_ttl_secs)
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("JWT generation failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    (StatusCode::OK, Json(TokenResponse { token })).into_response()
}

#[derive(Deserialize)]
pub struct TestTokenBody {
    // Browser clients send 17-digit IDs as strings; Rust clients as numbers.
    #[serde(deserialize_with = "lobby_core::types::deserialize_steam_id")]
    steam_id: u64,
}

pub async fn test_token(
    State(state): State<Arc<AppState>>,
    ConnectInfo(ip): ConnectInfo<std::net::SocketAddr>,
    Json(body): Json<TestTokenBody>,
) -> impl IntoResponse {
    if !state.test_token_limiter.check(ip.ip()) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(serde_json::json!({"error": "rate_limited"})),
        )
            .into_response();
    }

    let version = match state.store.get_token_version(body.steam_id).await {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    let token = match state
        .steam_auth
        .generate_session_token(body.steam_id, version, state.config.jwt_ttl_secs)
    {
        Ok(t) => t,
        Err(e) => {
            tracing::error!("JWT generation failed: {e}");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    (StatusCode::OK, Json(TokenResponse { token })).into_response()
}

/// Revoke the session token presented in `Authorization: Bearer <token>` by
/// bumping the player's token_version (all previously minted tokens die).
pub async fn logout(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(auth) = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    };
    let Some(token) = auth.strip_prefix("Bearer ") else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "unauthorized"})),
        )
            .into_response();
    };
    let (steam_id, _) = match state.steam_auth.validate_session_token(token) {
        Ok(pair) => pair,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "unauthorized"})),
            )
                .into_response();
        }
    };
    match state.store.bump_token_version(steam_id).await {
        Ok(()) => (),
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    }
    StatusCode::NO_CONTENT.into_response()
}

#[cfg(test)]
mod tests {
    use super::{build_token_redirect, validate_return_to};

    #[test]
    fn validate_return_to_table() {
        let cases: &[(&str, Option<&str>, bool)] = &[
            ("/dashboard", Some("https://lobby.example.com"), true),
            ("/dashboard", None, true),
            ("//evil.com/x", None, false),
            ("/\\evil.com", None, false),
            ("", None, false),
            ("javascript:alert(1)", None, false),
            ("/x?a=b", None, false),
            ("/x#frag", None, false),
            ("https://evil.com/x", Some("https://lobby.example.com"), false),
            ("https://lobby.example.com/cb", Some("https://lobby.example.com"), true),
            ("https://lobby.example.com/cb", None, false),
            ("https://lobby.example.com/cb", Some("https://lobby.example.com/"), true),
            ("https://lobby.example.com/cb?x=1", Some("https://lobby.example.com"), true),
            ("https://lobby.example.com/cb#frag", Some("https://lobby.example.com"), false),
        ];
        for (return_to, public_url, expected) in cases {
            assert_eq!(
                validate_return_to(return_to, *public_url),
                *expected,
                "return_to={return_to:?}, public_url={public_url:?}"
            );
        }
    }

    #[test]
    fn token_redirect_uses_fragment() {
        assert_eq!(
            build_token_redirect("/dashboard", "abc"),
            "/dashboard#token=abc"
        );
        assert_eq!(
            build_token_redirect("https://lobby.example.com/cb", "xyz"),
            "https://lobby.example.com/cb#token=xyz"
        );
    }
}

#[derive(Deserialize)]
pub struct GameResultBody {
    #[serde(default, deserialize_with = "lobby_core::types::deserialize_optional_steam_id")]
    pub winner: Option<u64>, // None = draw
}

/// The gameserver reports the match outcome. The URL itself is the
/// authentication: {token}/{secret} — 401 unless the secret matches.
pub async fn game_result(
    State(state): State<Arc<AppState>>,
    Path((token, secret)): Path<(String, String)>,
    Json(body): Json<GameResultBody>,
) -> impl IntoResponse {
    let m = match state.store.get_match(&token).await {
        Ok(Some(m)) => m,
        Ok(None) => return StatusCode::NOT_FOUND,
        Err(e) => {
            tracing::warn!("game_result db error: {e}");
            return StatusCode::INTERNAL_SERVER_ERROR;
        }
    };
    if m.result_secret.as_deref() != Some(secret.as_str()) {
        return StatusCode::UNAUTHORIZED;
    }
    match state
        .match_manager
        .resolve_from_gameserver(&token, body.winner, &state.store, &state.store)
        .await
    {
        Ok(outcome) => {
            tracing::info!("match {token} gameserver result resolved: {outcome:?}");
            crate::ws::notify_match_players(
                &state,
                &token,
                crate::ws::ServerMessage::MatchResult {
                    match_token: token.clone(),
                    outcome: serde_json::to_value(&outcome).unwrap(),
                },
            )
            .await;
            StatusCode::OK
        }
        Err(e) => {
            // wrong status (already resolved/disputed) or invalid winner
            tracing::warn!("game_result rejected for match {token}: {e}");
            StatusCode::CONFLICT
        }
    }
}

#[derive(Serialize)]
pub struct ModeInfo {
    pub name: String,
    pub game_type: lobby_core::types::GameType,
}

/// The modes the server actually runs — the demo populates its dropdown from this.
pub async fn modes(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "modes": state.game_modes.iter().map(|(n, t)| ModeInfo { name: n.clone(), game_type: *t }).collect::<Vec<_>>() }))
}

/// Dev-only fake creator: returns a (simulated) server address and auto-reports
/// player_a's win 3s after allocation, exercising the full webhook path.
pub async fn mock_allocate(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    let callback = body["result_callback_url"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let winner = body["player_a"]
        .as_str()
        .map(|s| s.to_string())
        .unwrap_or_default();
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
        let _ = state
            .http
            .post(&callback)
            .json(&serde_json::json!({ "winner": winner }))
            .send()
            .await;
    });
    Json(serde_json::json!({ "server_address": "127.0.0.1:25565", "join_token": "mock-join" }))
}
