//! Steam ticket + OpenID verification and session JWT issue/validate, with
//! token-version revocation (logout bumps the version, killing old tokens).
use std::collections::HashMap;
use std::time::Duration;

use jsonwebtoken::{Algorithm, DecodingKey, EncodingKey, Header, Validation, decode, encode};
use lobby_core::error::{LobbyError, Result};
use lobby_core::types::SteamId;
use reqwest::Client;
use serde::{Deserialize, Serialize};

pub struct SteamAuthService {
    api_key: String,
    app_id: u32,
    http_client: Client,
    jwt_encoding_key: EncodingKey,
    jwt_decoding_key: DecodingKey,
    display_name_cache: std::sync::Mutex<HashMap<SteamId, (String, std::time::Instant)>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionClaims {
    pub typ: String,
    pub sid: uuid::Uuid,
    pub sub: String,
    pub provider: Option<String>,
    pub csrf: Option<String>,
    pub iat: usize,
    pub exp: usize,
    pub iss: String,
    pub aud: String,
    pub token_version: u32,
}

#[derive(Debug, Clone)]
pub enum ValidatedSession {
    Browser(SessionClaims),
    Native(SessionClaims),
}

impl SteamAuthService {
    pub fn new(api_key: String, app_id: u32, jwt_secret: String) -> Self {
        Self {
            api_key,
            app_id,
            http_client: Client::builder()
                .timeout(Duration::from_secs(10))
                .build()
                .expect("http client"),
            jwt_encoding_key: EncodingKey::from_secret(jwt_secret.as_bytes()),
            jwt_decoding_key: DecodingKey::from_secret(jwt_secret.as_bytes()),
            display_name_cache: std::sync::Mutex::new(HashMap::new()),
        }
    }

    /// Build the Steam OpenID redirect URL.
    /// Every value is percent-encoded by `url::query_pairs_mut`, so a
    /// `return_to` containing `?`/`&`/`#` becomes data, not structure.
    pub fn openid_redirect_url(&self, public_url: &str, state: &str, return_to: &str) -> String {
        let mut url =
            url::Url::parse("https://steamcommunity.com/openid/login").expect("static URL");
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("openid.ns", "http://specs.openid.net/auth/2.0");
            q.append_pair("openid.mode", "checkid_setup");
            q.append_pair(
                "openid.return_to",
                &format!(
                    "{}/auth/steam/callback?return_to={}&state={}",
                    public_url.trim_end_matches('/'),
                    return_to,
                    state
                ),
            );
            q.append_pair("openid.realm", public_url.trim_end_matches('/'));
            q.append_pair(
                "openid.identity",
                "http://specs.openid.net/auth/2.0/identifier_select",
            );
            q.append_pair(
                "openid.claimed_id",
                "http://specs.openid.net/auth/2.0/identifier_select",
            );
        }
        url.to_string()
    }

    /// Verify OpenID callback params.
    pub async fn verify_openid(&self, params: &HashMap<String, String>) -> Result<SteamId> {
        if params.get("openid.mode").map(|m| m.as_str()) != Some("id_res") {
            return Err(LobbyError::SteamAuthFailed("unexpected openid.mode".into()));
        }

        let mut verify_params = params.clone();
        verify_params.insert(
            "openid.mode".to_string(),
            "check_authentication".to_string(),
        );

        let resp = self
            .http_client
            .post("https://steamcommunity.com/openid/login")
            .form(&verify_params)
            .send()
            .await
            .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))?;

        let body = resp
            .text()
            .await
            .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))?;

        if !body.lines().any(|l| l.trim() == "is_valid:true") {
            return Err(LobbyError::SteamAuthFailed(
                "OpenID validation failed".into(),
            ));
        }

        let claimed_id = params
            .get("openid.claimed_id")
            .ok_or_else(|| LobbyError::SteamAuthFailed("missing claimed_id".into()))?;

        let prefix = "https://steamcommunity.com/openid/id/";
        let steam_id_str = claimed_id.strip_prefix(prefix).ok_or_else(|| {
            LobbyError::SteamAuthFailed(format!("unexpected claimed_id: {claimed_id}"))
        })?;

        steam_id_str
            .parse::<u64>()
            .map_err(|e| LobbyError::SteamAuthFailed(format!("invalid steam id: {e}")))
    }

    /// Verify an in-game ticket via Steam Web API.
    pub async fn verify_ticket(&self, ticket_hex: &str, identity: &str) -> Result<SteamId> {
        if self.api_key.is_empty() {
            return Err(LobbyError::SteamAuthFailed("provider_unavailable".into()));
        }
        if ticket_hex.is_empty()
            || ticket_hex.len() > 8192
            || ticket_hex.len() % 2 != 0
            || !ticket_hex.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(LobbyError::SteamAuthFailed("malformed ticket".into()));
        }
        if identity != "matchmaking" {
            return Err(LobbyError::SteamAuthFailed("invalid ticket identity".into()));
        }

        // POST form fields so neither the ticket nor API key is embedded in a
        // URL that an HTTP client error or proxy access log might disclose.
        let resp = self
            .http_client
            .post("https://partner.steam-api.com/ISteamUserAuth/AuthenticateUserTicket/v1/")
            .form(&[
                ("key", self.api_key.as_str()),
                ("appid", &self.app_id.to_string()),
                ("ticket", ticket_hex),
                ("identity", identity),
            ])
            .send()
            .await
            .map_err(|_| LobbyError::SteamAuthFailed("ticket provider request failed".into()))?;
        if !resp.status().is_success() {
            return Err(LobbyError::SteamAuthFailed("ticket rejected".into()));
        }
        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|_| LobbyError::SteamAuthFailed("invalid ticket provider response".into()))?;
        let steam_id_str = json["response"]["params"]["steamid"]
            .as_str()
            .ok_or_else(|| LobbyError::SteamAuthFailed("ticket rejected".into()))?;
        steam_id_str
            .parse::<u64>()
            .map_err(|_| LobbyError::SteamAuthFailed("invalid Steam identity".into()))
    }

    /// Call GetPlayerSummaries to get display_name (cached 300s).
    pub async fn get_player_summary(&self, steam_id: SteamId) -> Result<String> {
        {
            let cache = self.display_name_cache.lock().unwrap();
            if let Some((name, at)) = cache.get(&steam_id)
                && at.elapsed() < Duration::from_secs(300)
            {
                return Ok(name.clone());
            }
        }

        let url = format!(
            "https://api.steampowered.com/ISteamUser/GetPlayerSummaries/v2/\
             ?key={}&steamids={steam_id}",
            self.api_key
        );

        let resp = self
            .http_client
            .get(&url)
            .send()
            .await
            .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))?;

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))?;

        let name = json["response"]["players"][0]["personaname"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| LobbyError::SteamAuthFailed("missing personaname".into()))?;
        self.display_name_cache
            .lock()
            .unwrap()
            .insert(steam_id, (name.clone(), std::time::Instant::now()));
        Ok(name)
    }

    fn generate_token(&self, claims: SessionClaims) -> Result<String> {
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &self.jwt_encoding_key,
        )
        .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))
    }

    pub fn generate_native_token(
        &self,
        user_id: uuid::Uuid,
        session_id: uuid::Uuid,
        token_version: u32,
    ) -> Result<String> {
        let now = unix_now();
        self.generate_token(SessionClaims {
            typ: "native".into(), sid: session_id, sub: user_id.to_string(),
            provider: None, csrf: None, iat: now, exp: now + 3600,
            iss: "steam-lobby".into(), aud: "steam-lobby-native".into(), token_version,
        })
    }

    pub fn generate_browser_token(
        &self,
        user_id: uuid::Uuid,
        session_id: uuid::Uuid,
        provider: &str,
        csrf: &str,
        token_version: u32,
        ttl_secs: u64,
    ) -> Result<String> {
        let now = unix_now();
        self.generate_token(SessionClaims {
            typ: "browser".into(), sid: session_id, sub: user_id.to_string(),
            provider: Some(provider.into()), csrf: Some(csrf.into()), iat: now,
            exp: now + ttl_secs as usize, iss: "steam-lobby".into(),
            aud: "steam-lobby-browser".into(), token_version,
        })
    }

    fn validate_for(&self, token: &str, audience: &str, typ: &str) -> Result<SessionClaims> {
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_audience(&[audience]);
        validation.set_issuer(&["steam-lobby"]);
        validation.set_required_spec_claims(&[
            "typ",
            "sid",
            "sub",
            "iat",
            "exp",
            "iss",
            "aud",
            "token_version",
        ]);
        let claims = decode::<SessionClaims>(token, &self.jwt_decoding_key, &validation)
            .map_err(|_| LobbyError::SteamAuthFailed("invalid session".into()))?
            .claims;
        let valid_kind_claims = match typ {
            "native" => claims.provider.is_none() && claims.csrf.is_none(),
            "browser" => {
                claims.provider.as_deref().is_some_and(|value| !value.is_empty())
                    && claims.csrf.as_deref().is_some_and(|value| !value.is_empty())
            }
            _ => false,
        };
        if claims.typ != typ || !valid_kind_claims {
            return Err(LobbyError::SteamAuthFailed("invalid session type".into()));
        }
        Ok(claims)
    }

    pub fn validate_native_token(&self, token: &str) -> Result<SessionClaims> {
        self.validate_for(token, "steam-lobby-native", "native")
    }

    pub fn validate_browser_token(&self, token: &str) -> Result<SessionClaims> {
        self.validate_for(token, "steam-lobby-browser", "browser")
    }

    pub fn validate_session_token(&self, token: &str) -> Result<ValidatedSession> {
        self.validate_native_token(token).map(ValidatedSession::Native)
            .or_else(|_| self.validate_browser_token(token).map(ValidatedSession::Browser))
    }
}

fn unix_now() -> usize {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .as_secs() as usize
}
