mod common;

use std::collections::HashMap;
use std::sync::Arc;

use axum::{Json, Router};
use axum::http::{StatusCode, header};
use lobby_server::auth_providers::{ProviderConfig, ProviderKind};
use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use common::{TestConfig, TestHarness, setup_with_config};

const PUBLIC_ORIGIN: &str = "https://lobby.example.com";
const ALLOWED_BEARER_ORIGIN: &str = "https://native-shell.example.com";

#[derive(Default)]
struct OAuthRecorder {
    token_forms: Mutex<Vec<HashMap<String, String>>>,
    userinfo: RwLock<serde_json::Value>,
}

async fn spawn_oauth_provider() -> (String, Arc<OAuthRecorder>) {
    let recorder = Arc::new(OAuthRecorder {
        token_forms: Mutex::new(Vec::new()),
        userinfo: RwLock::new(serde_json::json!({
            "id": "discord-subject-1",
            "global_name": "Ranked Discord",
            "sub": "pocket-subject-1",
            "preferred_username": "Ranked Pocket"
        })),
    });
    let token_recorder = recorder.clone();
    let userinfo_recorder = recorder.clone();
    let app = Router::new()
        .route(
            "/token",
            axum::routing::post(move |axum::Form(form): axum::Form<HashMap<String, String>>| {
                let recorder = token_recorder.clone();
                async move {
                    recorder.token_forms.lock().await.push(form);
                    Json(serde_json::json!({"access_token": "provider-access-token"}))
                }
            }),
        )
        .route(
            "/userinfo",
            axum::routing::get(move || {
                let recorder = userinfo_recorder.clone();
                async move { Json(recorder.userinfo.read().await.clone()) }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}"), recorder)
}

fn provider(id: &str, base: &str, pkce: bool) -> ProviderConfig {
    ProviderConfig {
        id: id.into(),
        kind: if pkce { ProviderKind::Oidc } else { ProviderKind::OAuth2 },
        client_id: format!("{id}-client"),
        client_secret: format!("{id}-secret"),
        authorization_endpoint: format!("{base}/authorize"),
        token_endpoint: format!("{base}/token"),
        userinfo_endpoint: format!("{base}/userinfo"),
        scopes: vec!["identify".into()],
        id_field: if id == "discord" { "id".into() } else { "sub".into() },
        name_field: if id == "discord" { "global_name".into() } else { "preferred_username".into() },
        use_pkce: pkce,
    }
}

fn identity_config(providers: Vec<ProviderConfig>, strict: bool) -> TestConfig {
    TestConfig {
        auth_dev_mode: true,
        steam_backed_accounts_only: strict,
        public_url: Some(PUBLIC_ORIGIN.into()),
        cors_origins: vec![ALLOWED_BEARER_ORIGIN.into()],
        provider_overrides: providers,
        ..TestConfig::default()
    }
}

fn set_cookie_value(response: &reqwest::Response, name: &str) -> String {
    response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find_map(|raw| {
            let pair = raw.split(';').next()?;
            let (key, value) = pair.split_once('=')?;
            (key == name).then(|| value.to_owned())
        })
        .unwrap_or_else(|| panic!("response did not set {name}"))
}

fn assert_cookie_attributes(response: &reqwest::Response, name: &str, path: &str) {
    let raw = response
        .headers()
        .get_all(header::SET_COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .find(|raw| raw.starts_with(&format!("{name}=")))
        .unwrap_or_else(|| panic!("response did not set {name}"));
    assert!(raw.contains(&format!("Path={path}")), "cookie path: {raw}");
    assert!(raw.contains("HttpOnly"), "cookie must be inaccessible to script: {raw}");
    assert!(raw.contains("SameSite=Lax"), "cookie must constrain cross-site requests: {raw}");
    assert!(raw.contains("Secure"), "HTTPS deployment cookie must be Secure: {raw}");
}

struct LoginStart {
    state: String,
    nonce: String,
}

async fn begin_provider_login(h: &TestHarness, provider: &str) -> LoginStart {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let response = client
        .get(format!("{}/auth/{provider}/login?return_to=/link", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_cookie_attributes(&response, "oauth_login_nonce", "/auth");
    let nonce = set_cookie_value(&response, "oauth_login_nonce");
    let location = response.headers()[header::LOCATION].to_str().unwrap();
    let url = url::Url::parse(location).unwrap();
    let state = url
        .query_pairs()
        .find(|(key, _)| key == "state")
        .map(|(_, value)| value.into_owned())
        .expect("authorization URL state");
    LoginStart { state, nonce }
}

async fn provider_callback(
    h: &TestHarness,
    provider: &str,
    login: &LoginStart,
    nonce: &str,
) -> reqwest::Response {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap()
        .get(format!(
            "{}/auth/{provider}/callback?code=accepted&state={}",
            h.base_url, login.state
        ))
        .header(header::COOKIE, format!("oauth_login_nonce={nonce}"))
        .send()
        .await
        .unwrap()
}

struct BrowserPrincipal {
    user_id: Uuid,
    session_id: Uuid,
    token: String,
    csrf: String,
}

async fn seed_steam_browser(h: &TestHarness, steam_id: i64) -> BrowserPrincipal {
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users(steam_id,display_name,primary_provider) VALUES($1,$2,'steam') RETURNING id",
    )
    .bind(steam_id)
    .bind(format!("Steam {steam_id}"))
    .fetch_one(&h.pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO accounts(provider,provider_uid,user_id,last_login_at,linked_at) VALUES('steam',$1,$2,NOW(),NOW())")
        .bind(steam_id.to_string())
        .bind(user_id)
        .execute(&h.pool)
        .await
        .unwrap();
    let csrf = format!("csrf-{steam_id}");
    let session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO browser_sessions(user_id,auth_provider,csrf_hash,expires_at) VALUES($1,'steam',$2,NOW()+INTERVAL '1 day') RETURNING session_id",
    )
    .bind(user_id)
    .bind(Sha256::digest(csrf.as_bytes()).to_vec())
    .fetch_one(&h.pool)
    .await
    .unwrap();
    let token = h
        .state
        .steam_auth
        .generate_browser_token(user_id, session_id, "steam", &csrf, 0, 86_400)
        .unwrap();
    BrowserPrincipal { user_id, session_id, token, csrf }
}

async fn seed_browser_for_user(h: &TestHarness, user_id: Uuid, provider: &str) -> BrowserPrincipal {
    let csrf = format!("csrf-{provider}-{}", Uuid::new_v4());
    let session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO browser_sessions(user_id,auth_provider,csrf_hash,expires_at) VALUES($1,$2,$3,NOW()+INTERVAL '1 day') RETURNING session_id",
    )
    .bind(user_id)
    .bind(provider)
    .bind(Sha256::digest(csrf.as_bytes()).to_vec())
    .fetch_one(&h.pool)
    .await
    .unwrap();
    let token = h
        .state
        .steam_auth
        .generate_browser_token(user_id, session_id, provider, &csrf, 0, 86_400)
        .unwrap();
    BrowserPrincipal { user_id, session_id, token, csrf }
}

async fn seed_native(h: &TestHarness, user_id: Uuid) -> (Uuid, String) {
    let session_id: Uuid = sqlx::query_scalar(
        "INSERT INTO native_sessions(user_id,expires_at) VALUES($1,NOW()+INTERVAL '1 hour') RETURNING session_id",
    )
    .bind(user_id)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    let token = h
        .state
        .steam_auth
        .generate_native_token(user_id, session_id, 0)
        .unwrap();
    (session_id, token)
}

async fn insert_proof(h: &TestHarness, subject: &str, name: &str) -> String {
    let proof = format!("proof-{}", Uuid::new_v4());
    sqlx::query("INSERT INTO provider_sessions(nonce_hash,provider,provider_uid,display_name,expires_at) VALUES($1,'discord',$2,$3,NOW()+INTERVAL '5 minutes')")
        .bind(Sha256::digest(proof.as_bytes()).to_vec())
        .bind(subject)
        .bind(name)
        .execute(&h.pool)
        .await
        .unwrap();
    proof
}

fn cookie_header(browser: &BrowserPrincipal) -> String {
    format!("lobby_session={}", browser.token)
}

async fn create_intent(h: &TestHarness, browser: &BrowserPrincipal, proof: &str) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/api/link/intent", h.base_url))
        .header(header::ORIGIN, PUBLIC_ORIGIN)
        .header("x-csrf-token", &browser.csrf)
        .header(
            header::COOKIE,
            format!("{}; discord_link_proof={proof}", cookie_header(browser)),
        )
        .send()
        .await
        .unwrap()
}

async fn confirm_intent(h: &TestHarness, browser: &BrowserPrincipal, intent_id: Uuid) -> reqwest::Response {
    reqwest::Client::new()
        .post(format!("{}/api/link/confirm", h.base_url))
        .header(header::ORIGIN, PUBLIC_ORIGIN)
        .header("x-csrf-token", &browser.csrf)
        .header(header::COOKIE, cookie_header(browser))
        .json(&serde_json::json!({"intent_id": intent_id}))
        .send()
        .await
        .unwrap()
}

#[sqlx::test]
async fn account_creation_policy_and_strict_discord_login(pool: sqlx::PgPool) {
    let (provider_base, _recorder) = spawn_oauth_provider().await;
    let discord = provider("discord", &provider_base, false);

    let non_strict = setup_with_config(
        pool.clone(),
        identity_config(vec![discord.clone()], false),
    )
    .await;
    let login = begin_provider_login(&non_strict, "discord").await;
    let callback = provider_callback(&non_strict, "discord", &login, &login.nonce).await;
    assert_eq!(callback.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_cookie_attributes(&callback, "lobby_session", "/");
    assert_eq!(callback.headers()[header::LOCATION], "/link");
    let created: (String, Option<i64>, String) = sqlx::query_as(
        "SELECT a.provider_uid,u.steam_id,u.primary_provider FROM accounts a JOIN users u ON u.id=a.user_id WHERE a.provider='discord'",
    )
    .fetch_one(&non_strict.pool)
    .await
    .unwrap();
    assert_eq!(created, ("discord-subject-1".into(), None, "discord".into()));
    drop(non_strict);

    let strict = setup_with_config(pool, identity_config(vec![discord], true)).await;
    for path in ["/auth/guest", "/auth/test-token"] {
        let response = reqwest::Client::new()
            .post(format!("{}{path}", strict.base_url))
            .json(&serde_json::json!({"steam_id": 42}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "strict route {path}");
    }

    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM users")
        .fetch_one(&strict.pool)
        .await
        .unwrap();
    let login = begin_provider_login(&strict, "discord").await;
    let callback = provider_callback(&strict, "discord", &login, &login.nonce).await;
    assert_eq!(callback.status(), StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(callback.headers()[header::LOCATION], "/link");
    assert_cookie_attributes(&callback, "discord_link_proof", "/api/link");
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM users")
        .fetch_one(&strict.pool)
        .await
        .unwrap();
    assert_eq!(after, before, "an unbound Discord subject must not create a strict-mode account");
}

#[sqlx::test]
async fn oauth_state_is_durable_nonce_bound_provider_bound_one_time_and_pkce(pool: sqlx::PgPool) {
    let (provider_base, recorder) = spawn_oauth_provider().await;
    let providers = vec![
        provider("discord", &provider_base, false),
        provider("au2143", &provider_base, true),
    ];
    let first = setup_with_config(pool.clone(), identity_config(providers.clone(), false)).await;
    let login = begin_provider_login(&first, "au2143").await;

    let persisted: (String, String) = sqlx::query_as(
        "SELECT provider,code_verifier FROM oauth_login_states WHERE consumed_at IS NULL",
    )
    .fetch_one(&first.pool)
    .await
    .unwrap();
    assert_eq!(persisted.0, "au2143");
    assert!(persisted.1.len() >= 43, "PKCE verifier must be persisted server-side");
    drop(first);

    let replacement = setup_with_config(pool.clone(), identity_config(providers, false)).await;
    let missing_nonce = provider_callback(&replacement, "au2143", &login, "").await;
    assert_eq!(missing_nonce.status(), StatusCode::UNAUTHORIZED);
    let wrong_provider = provider_callback(&replacement, "discord", &login, &login.nonce).await;
    assert_eq!(wrong_provider.status(), StatusCode::UNAUTHORIZED);

    let accepted = provider_callback(&replacement, "au2143", &login, &login.nonce).await;
    assert_eq!(accepted.status(), StatusCode::TEMPORARY_REDIRECT);
    let forms = recorder.token_forms.lock().await;
    assert_eq!(forms.len(), 1, "rejected callbacks must not reach token exchange");
    assert_eq!(forms[0].get("code_verifier"), Some(&persisted.1));
    drop(forms);

    let replay = provider_callback(&replacement, "au2143", &login, &login.nonce).await;
    assert_eq!(replay.status(), StatusCode::UNAUTHORIZED);

    let expired = begin_provider_login(&replacement, "au2143").await;
    sqlx::query("UPDATE oauth_login_states SET expires_at=NOW()-INTERVAL '1 second' WHERE consumed_at IS NULL")
        .execute(&replacement.pool)
        .await
        .unwrap();
    let expired_response = provider_callback(&replacement, "au2143", &expired, &expired.nonce).await;
    assert_eq!(expired_response.status(), StatusCode::UNAUTHORIZED);
}

#[sqlx::test]
async fn browser_cookie_requires_live_row_matching_csrf_and_exact_origin(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, identity_config(vec![], false)).await;
    let browser = seed_steam_browser(&h, 76561198000000001).await;
    let client = reqwest::Client::new();

    let session = client
        .get(format!("{}/api/session", h.base_url))
        .header(header::COOKIE, cookie_header(&browser))
        .send()
        .await
        .unwrap();
    assert_eq!(session.status(), StatusCode::OK);
    assert_eq!(session.headers()[header::CACHE_CONTROL], "no-store");
    let body: serde_json::Value = session.json().await.unwrap();
    assert_eq!(body["user_id"], browser.user_id.to_string());
    assert_eq!(body["auth_provider"], "steam");
    assert_eq!(body["csrf_token"], browser.csrf);

    for (origin, csrf) in [
        (ALLOWED_BEARER_ORIGIN, browser.csrf.as_str()),
        (PUBLIC_ORIGIN, "wrong-csrf"),
    ] {
        let rejected = client
            .post(format!("{}/api/logout", h.base_url))
            .header(header::COOKIE, cookie_header(&browser))
            .header(header::ORIGIN, origin)
            .header("x-csrf-token", csrf)
            .send()
            .await
            .unwrap();
        assert_eq!(rejected.status(), StatusCode::UNAUTHORIZED, "origin={origin}, csrf={csrf}");
    }

    sqlx::query("UPDATE browser_sessions SET csrf_hash=$1 WHERE session_id=$2")
        .bind(vec![0_u8; 32])
        .bind(browser.session_id)
        .execute(&h.pool)
        .await
        .unwrap();
    let hash_mismatch = client
        .post(format!("{}/api/logout", h.base_url))
        .header(header::COOKIE, cookie_header(&browser))
        .header(header::ORIGIN, PUBLIC_ORIGIN)
        .header("x-csrf-token", &browser.csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(hash_mismatch.status(), StatusCode::UNAUTHORIZED);

    sqlx::query("UPDATE browser_sessions SET csrf_hash=$1 WHERE session_id=$2")
        .bind(Sha256::digest(browser.csrf.as_bytes()).to_vec())
        .bind(browser.session_id)
        .execute(&h.pool)
        .await
        .unwrap();
    let logout = client
        .post(format!("{}/api/logout", h.base_url))
        .header(header::COOKIE, cookie_header(&browser))
        .header(header::ORIGIN, PUBLIC_ORIGIN)
        .header("x-csrf-token", &browser.csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    let revoked = client
        .get(format!("{}/api/session", h.base_url))
        .header(header::COOKIE, cookie_header(&browser))
        .send()
        .await
        .unwrap();
    assert_eq!(revoked.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(revoked.headers()[header::CACHE_CONTROL], "no-store");
}

async fn websocket_upgrade(h: &TestHarness, origin: &str, cookie: Option<&str>) -> StatusCode {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let mut request = client
        .get(format!("{}/ws", h.base_url))
        .header(header::CONNECTION, "Upgrade")
        .header(header::UPGRADE, "websocket")
        .header("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
        .header("Sec-WebSocket-Version", "13")
        .header(header::ORIGIN, origin);
    if let Some(cookie) = cookie {
        request = request.header(header::COOKIE, cookie);
    }
    request.send().await.unwrap().status()
}

#[sqlx::test]
async fn cookie_websocket_uses_public_origin_while_bearer_handshake_uses_allowlist(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, identity_config(vec![], false)).await;
    let browser = seed_steam_browser(&h, 76561198000000002).await;
    let cookie = cookie_header(&browser);

    assert_eq!(
        websocket_upgrade(&h, ALLOWED_BEARER_ORIGIN, Some(&cookie)).await,
        StatusCode::FORBIDDEN,
        "an allowlisted bearer origin must not authorize a cookie credential"
    );
    assert_eq!(
        websocket_upgrade(&h, PUBLIC_ORIGIN, Some(&cookie)).await,
        StatusCode::SWITCHING_PROTOCOLS
    );
    assert_eq!(
        websocket_upgrade(&h, ALLOWED_BEARER_ORIGIN, None).await,
        StatusCode::SWITCHING_PROTOCOLS,
        "token/native clients retain the configured CORS allowlist path"
    );
}

#[sqlx::test]
async fn discord_intents_enforce_session_ownership_expiry_conflicts_and_replay(pool: sqlx::PgPool) {
    let (provider_base, _recorder) = spawn_oauth_provider().await;
    let h = setup_with_config(
        pool,
        identity_config(vec![provider("discord", &provider_base, false)], true),
    )
    .await;
    let owner = seed_steam_browser(&h, 76561198000000003).await;
    let other = seed_steam_browser(&h, 76561198000000004).await;

    let proof = insert_proof(&h, "discord-owned", "Server Supplied Name").await;
    let response = create_intent(&h, &owner, &proof).await;
    assert_eq!(response.status(), StatusCode::CREATED);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["discord_display_name"], "Server Supplied Name");
    let intent_id = Uuid::parse_str(body["intent_id"].as_str().unwrap()).unwrap();

    let wrong_session = confirm_intent(&h, &other, intent_id).await;
    assert_eq!(wrong_session.status(), StatusCode::CONFLICT);
    assert_eq!(confirm_intent(&h, &owner, intent_id).await.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        confirm_intent(&h, &owner, intent_id).await.status(),
        StatusCode::CONFLICT,
        "a committed proof is one-time and replay is a conflict"
    );

    let linked_owner: Uuid = sqlx::query_scalar(
        "SELECT user_id FROM accounts WHERE provider='discord' AND provider_uid='discord-owned'",
    )
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(linked_owner, owner.user_id);

    let stolen_proof = insert_proof(&h, "discord-owned", "Stolen").await;
    let stolen: serde_json::Value = create_intent(&h, &other, &stolen_proof)
        .await
        .json()
        .await
        .unwrap();
    let stolen_id = Uuid::parse_str(stolen["intent_id"].as_str().unwrap()).unwrap();
    assert_eq!(confirm_intent(&h, &other, stolen_id).await.status(), StatusCode::CONFLICT);
    let status: String = sqlx::query_scalar("SELECT status FROM link_intents WHERE id=$1")
        .bind(stolen_id)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(status, "pending", "ownership conflicts must not consume the intent");

    let second_proof = insert_proof(&h, "discord-second", "Second").await;
    let second: serde_json::Value = create_intent(&h, &owner, &second_proof)
        .await
        .json()
        .await
        .unwrap();
    let second_id = Uuid::parse_str(second["intent_id"].as_str().unwrap()).unwrap();
    assert_eq!(confirm_intent(&h, &owner, second_id).await.status(), StatusCode::CONFLICT);

    let expiring_proof = insert_proof(&h, "discord-expiring", "Expired").await;
    let expiring: serde_json::Value = create_intent(&h, &other, &expiring_proof)
        .await
        .json()
        .await
        .unwrap();
    let expiring_id = Uuid::parse_str(expiring["intent_id"].as_str().unwrap()).unwrap();
    sqlx::query("UPDATE link_intents SET expires_at=NOW()-INTERVAL '1 second' WHERE id=$1")
        .bind(expiring_id)
        .execute(&h.pool)
        .await
        .unwrap();
    assert_eq!(confirm_intent(&h, &other, expiring_id).await.status(), StatusCode::GONE);
}

#[sqlx::test]
async fn unlink_revokes_only_discord_sessions_and_native_logout_is_session_scoped(pool: sqlx::PgPool) {
    let (provider_base, _recorder) = spawn_oauth_provider().await;
    let h = setup_with_config(
        pool,
        identity_config(vec![provider("discord", &provider_base, false)], true),
    )
    .await;
    let steam = seed_steam_browser(&h, 76561198000000005).await;
    sqlx::query("INSERT INTO accounts(provider,provider_uid,user_id,last_login_at,linked_at) VALUES('discord','to-unlink',$1,NOW(),NOW())")
        .bind(steam.user_id)
        .execute(&h.pool)
        .await
        .unwrap();
    let discord = seed_browser_for_user(&h, steam.user_id, "discord").await;
    let (_native_id, native_token) = seed_native(&h, steam.user_id).await;
    let claims = h.state.steam_auth.validate_native_token(&native_token).unwrap();
    assert_eq!(claims.typ, "native");
    assert_eq!(claims.aud, "steam-lobby-native");
    assert_eq!(claims.exp - claims.iat, 3600, "native JWT lifetime is fixed at one hour");

    let revoke = reqwest::Client::new()
        .post(format!("{}/api/link/revoke", h.base_url))
        .header(header::COOKIE, cookie_header(&steam))
        .header(header::ORIGIN, PUBLIC_ORIGIN)
        .header("x-csrf-token", &steam.csrf)
        .send()
        .await
        .unwrap();
    assert_eq!(revoke.status(), StatusCode::NO_CONTENT);
    let discord_account_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM accounts WHERE user_id=$1 AND provider='discord'",
    )
    .bind(steam.user_id)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(discord_account_count, 0);

    let client = reqwest::Client::new();
    assert_eq!(
        client
            .get(format!("{}/api/session", h.base_url))
            .header(header::COOKIE, cookie_header(&discord))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED
    );
    assert_eq!(
        client
            .get(format!("{}/api/session", h.base_url))
            .header(header::COOKIE, cookie_header(&steam))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "unlink must preserve the Steam browser session"
    );
    assert_eq!(
        client
            .post(format!("{}/api/link/native/start", h.base_url))
            .bearer_auth(&native_token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::CREATED,
        "unlink must preserve native sessions"
    );

    let logout = client
        .post(format!("{}/api/logout", h.base_url))
        .bearer_auth(&native_token)
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::NO_CONTENT);
    assert_eq!(
        client
            .post(format!("{}/api/link/native/start", h.base_url))
            .bearer_auth(&native_token)
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::UNAUTHORIZED,
        "revoked native session must fail live-row validation"
    );
    assert_eq!(
        client
            .get(format!("{}/api/session", h.base_url))
            .header(header::COOKIE, cookie_header(&steam))
            .send()
            .await
            .unwrap()
            .status(),
        StatusCode::OK,
        "single-session native logout must not revoke the browser session"
    );
}

#[sqlx::test]
async fn link_spa_routes_are_served_and_ticket_errors_are_mapped_without_provider_calls(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, identity_config(vec![], true)).await;
    for path in ["/link", "/link/native-complete?intent_id=00000000-0000-0000-0000-000000000000"] {
        let response = reqwest::Client::new()
            .get(format!("{}{path}", h.base_url))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "SPA route {path}");
        assert!(response.text().await.unwrap().contains("<!doctype html>"));
    }

    for (ticket, expected) in [("not-hex", StatusCode::BAD_REQUEST), ("abc", StatusCode::BAD_REQUEST)] {
        let response = reqwest::Client::new()
            .post(format!("{}/api/ticket", h.base_url))
            .json(&serde_json::json!({"ticket": ticket}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), expected, "ticket={ticket}");
    }
}

