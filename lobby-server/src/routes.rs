//! HTTP surface: health + embedded demo index, Steam OpenID login/callback,
//! ticket auth, logout, the internal gameserver result webhook, and the
//! dev-only test-token + mock creator endpoints.
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{Html, IntoResponse, Redirect, Response};
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

/// The rollback sim module (`web/pong-sim.mjs`), embedded and served as JS so
/// the demo's `<script type="module">` can import it from the same origin.
pub async fn pong_sim() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript")],
        include_str!("../../web/pong-sim.mjs"),
    )
}

/// The rollback session module (`web/pong-rollback.mjs`), same treatment.
pub async fn pong_rollback() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript")],
        include_str!("../../web/pong-rollback.mjs"),
    )
}

/// The WebRTC glue module (`web/pong-wrtc.mjs`), embedded and served as JS.
pub async fn pong_wrtc() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript")],
        include_str!("../../web/pong-wrtc.mjs"),
    )
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
    let Some(pub_url) = public_url else {
        return false;
    };
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
        let mut states = state.openid_states.lock();
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
                provider: "steam".to_string(),
                code_verifier: None,
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
        let mut states = state.openid_states.lock();
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

    // Find or create the account; the Steam ID is genuinely verified here, so
    // the ('steam', steam_id) identity row is attached. DB error fails closed.
    let user_id = match state
        .store
        .find_or_create_user("steam", &steam_id.to_string(), &display_name, true)
        .await
    {
        Ok(uid) => uid,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    // Generate JWT bound to the current token_version (DB error fails closed).
    let version = match state.store.get_token_version(user_id).await {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };
    let token = match state.steam_auth.generate_session_token(
        user_id,
        version,
        state.config.jwt_ttl_secs,
    ) {
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

/// Generic OAuth2/OIDC login start: 307 to the provider's authorization
/// endpoint with a one-time state (and, for PKCE providers, a code challenge).
pub async fn auth_login(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Query(query): Query<LoginQuery>,
) -> impl IntoResponse {
    let Some(cfg) = state.auth_providers.get(&provider) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "provider_not_found"})),
        )
            .into_response();
    };
    let return_to = query.return_to.unwrap_or_else(|| "/".to_string());
    if !validate_return_to(&return_to, state.config.public_url.as_deref()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "invalid_return_to"})),
        )
            .into_response();
    }
    let Some(public_url) = state.config.public_url.as_deref() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "public_url_required"})),
        )
            .into_response();
    };

    let (verifier, challenge) = if cfg.use_pkce {
        let (v, c) = crate::auth_providers::pkce_pair();
        (Some(v), c)
    } else {
        (None, String::new())
    };

    // Issue a one-time login state bound to this return_to + provider.
    let login_state = uuid::Uuid::new_v4().to_string();
    {
        let mut states = state.openid_states.lock();
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
                provider: provider.clone(),
                code_verifier: verifier.clone(),
            },
        );
    }

    let callback_url = format!("{public_url}/auth/{provider}/callback");
    let redirect_url = crate::auth_providers::authorization_url(
        cfg,
        &callback_url,
        &login_state,
        verifier.as_deref(),
        &challenge,
    );
    Redirect::temporary(&redirect_url).into_response()
}

/// Generic OAuth2/OIDC callback: consume the state, exchange the code,
/// fetch userinfo, find-or-create the account, mint the JWT, and 307 to
/// `return_to#token=...`. All failures fail closed with 401 auth_failed.
pub async fn auth_callback(
    State(state): State<Arc<AppState>>,
    Path(provider): Path<String>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let Some(cfg) = state.auth_providers.get(&provider) else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "provider_not_found"})),
        )
            .into_response();
    };
    let return_to = query
        .get("return_to")
        .cloned()
        .unwrap_or_else(|| "/".to_string());
    let state_param = query.get("state").cloned().unwrap_or_default();
    let code = query.get("code").cloned().unwrap_or_default();

    // Consume the one-time login state (prevents replay of a callback).
    let stored = {
        let mut states = state.openid_states.lock();
        states.remove(&state_param)
    };
    let Some(stored) = stored else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    };
    if stored.created_at.elapsed() >= Duration::from_secs(600) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    }
    if stored.provider != provider {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    }
    if stored.return_to != return_to {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    }
    if !validate_return_to(&return_to, state.config.public_url.as_deref()) {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    }
    let Some(public_url) = state.config.public_url.as_deref() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    };

    // Exchange the authorization code for an access token.
    let callback_url = format!("{public_url}/auth/{provider}/callback");
    let mut form = vec![
        ("grant_type", "authorization_code"),
        ("code", &code),
        ("redirect_uri", &callback_url),
        ("client_id", &cfg.client_id),
        ("client_secret", &cfg.client_secret),
    ];
    if let Some(verifier) = &stored.code_verifier {
        form.push(("code_verifier", verifier));
    }
    let token_resp = state
        .http
        .post(&cfg.token_endpoint)
        .form(&form)
        .send()
        .await;
    let token_json: serde_json::Value = match token_resp {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(v) => v,
            Err(_) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({"error": "auth_failed"})),
                )
                    .into_response();
            }
        },
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "auth_failed"})),
            )
                .into_response();
        }
    };
    let access_token = token_json["access_token"]
        .as_str()
        .ok_or_else(|| ())
        .map(|s| s.to_string());
    let Ok(access_token) = access_token else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    };

    // Fetch the userinfo document with the access token.
    let userinfo_resp = state
        .http
        .get(&cfg.userinfo_endpoint)
        .bearer_auth(&access_token)
        .send()
        .await;
    let userinfo: serde_json::Value = match userinfo_resp {
        Ok(r) if r.status().is_success() => match r.json().await {
            Ok(v) => v,
            Err(_) => {
                return (
                    StatusCode::UNAUTHORIZED,
                    Json(serde_json::json!({"error": "auth_failed"})),
                )
                    .into_response();
            }
        },
        _ => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "auth_failed"})),
            )
                .into_response();
        }
    };
    let Some(provider_uid) = userinfo[&cfg.id_field].as_str() else {
        return (
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "auth_failed"})),
        )
            .into_response();
    };
    let display_name = crate::auth_providers::userinfo_name(&userinfo, cfg);

    // Find or create the account; the provider genuinely verified the uid, so
    // the identity row is attached. DB error fails closed.
    let user_id = match state
        .store
        .find_or_create_user(&provider, provider_uid, &display_name, true)
        .await
    {
        Ok(uid) => uid,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "auth_failed"})),
            )
                .into_response();
        }
    };

    // Admin flag (au.2143.me only, storage-only): record whether the Pocket ID
    // `groups` claim contains `pvp_admin`. Written true or false on every
    // au2143 login so group removal self-heals at the next login; a missing
    // claim reads as false. Best-effort — a DB error logs and the login still
    // succeeds. No endpoint/JWT/UI consumes the flag yet.
    if provider == "au2143" {
        let is_admin = userinfo["groups"]
            .as_array()
            .is_some_and(|g| g.iter().any(|v| v.as_str() == Some("pvp_admin")));
        if let Err(e) = state.store.set_admin_flag(user_id, is_admin).await {
            tracing::warn!("failed to record admin flag for {user_id}: {e}");
        }
    }

    // Mint JWT bound to the current token_version (DB error fails closed).
    let version = match state.store.get_token_version(user_id).await {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "auth_failed"})),
            )
                .into_response();
        }
    };
    let token = match state
        .steam_auth
        .generate_session_token(user_id, version, state.config.jwt_ttl_secs)
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

    // Find or create the account; the ticket is genuinely verified, so the
    // ('steam', steam_id) identity row is attached. DB error fails closed.
    let user_id = match state.store.find_or_create_user("steam", &steam_id.to_string(), "", true).await {
        Ok(uid) => uid,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    // DB error fails closed — never mint with version 0.
    let version = match state.store.get_token_version(user_id).await {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };
    let token = match state.steam_auth.generate_session_token(
        user_id,
        version,
        state.config.jwt_ttl_secs,
    ) {
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
    // Dev-only: a numeric Steam ID, supplied directly. Plain u64 field — the
    // old string/number-tolerant serde helper no longer exists.
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

    // Find or create the account (dev test-token: NOT genuinely verified, so
    // no identity row is attached). DB error fails closed.
    let user_id = match state
        .store
        .find_or_create_user("steam", &body.steam_id.to_string(), "", false)
        .await
    {
        Ok(uid) => uid,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };

    let version = match state.store.get_token_version(user_id).await {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "internal"})),
            )
                .into_response();
        }
    };
    let token = match state.steam_auth.generate_session_token(
        user_id,
        version,
        state.config.jwt_ttl_secs,
    ) {
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
pub async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
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
    let (user_id, _) = match state.steam_auth.validate_session_token(token) {
        Ok(pair) => pair,
        Err(_) => {
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "unauthorized"})),
            )
                .into_response();
        }
    };
    match state.store.bump_token_version(user_id).await {
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
            (
                "https://evil.com/x",
                Some("https://lobby.example.com"),
                false,
            ),
            (
                "https://lobby.example.com/cb",
                Some("https://lobby.example.com"),
                true,
            ),
            ("https://lobby.example.com/cb", None, false),
            (
                "https://lobby.example.com/cb",
                Some("https://lobby.example.com/"),
                true,
            ),
            (
                "https://lobby.example.com/cb?x=1",
                Some("https://lobby.example.com"),
                true,
            ),
            (
                "https://lobby.example.com/cb#frag",
                Some("https://lobby.example.com"),
                false,
            ),
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
    #[serde(default)]
    pub winner: Option<uuid::Uuid>, // None = draw
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
        .resolve_from_gameserver(
            &token,
            body.winner,
            &state.store,
            &state.store,
            &state.store,
        )
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
    Json(
        serde_json::json!({ "modes": state.game_modes.iter().map(|(n, t)| ModeInfo { name: n.clone(), game_type: *t }).collect::<Vec<_>>() }),
    )
}

/// Auth surface capabilities the demo uses to gate its login UI:
/// `providers` lists the registered login providers ("steam" when a public
/// origin is configured — OpenID needs an absolute callback — plus the
/// registry's provider ids), `dev_mode` when the test-token endpoint is
/// exposed.
pub async fn auth_config(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let mut providers = Vec::new();
    if state.config.public_url.is_some() {
        providers.push("steam");
    }
    providers.extend(state.auth_providers.ids());
    Json(serde_json::json!({
        "providers": providers,
        "dev_mode": state.config.auth_dev_mode,
    }))
}

/// Return TURN REST-auth credentials for a WebRTC peer connection.
/// 503 when LOBBY_TURN_SECRET is unset (host candidates only).
pub async fn turn_credentials(State(state): State<Arc<AppState>>) -> Response {
    let Some(secret) = &state.config.turn_secret else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({"error": "turn not configured"})),
        )
            .into_response();
    };
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    Json(crate::turn::mint_turn_credentials(
        secret,
        3600,
        now,
        &state.config.turn_uris,
    ))
    .into_response()
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
