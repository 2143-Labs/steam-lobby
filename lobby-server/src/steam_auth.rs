use std::collections::HashMap;
use std::time::Duration;

use jsonwebtoken::{
    decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
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

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    iat: usize,
    exp: usize,
    iss: String,
    aud: String,
    token_version: u32,
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
    pub async fn verify_ticket(&self, ticket_hex: &str) -> Result<SteamId> {
        // Reject malformed tickets before spending a Steam API call.
        if ticket_hex.len() > 8192 || !ticket_hex.bytes().all(|b| b.is_ascii_hexdigit()) {
            return Err(LobbyError::SteamAuthFailed("malformed ticket".into()));
        }

        let url = format!(
            "https://partner.steam-api.com/ISteamUserAuth/AuthenticateUserTicket/v1/\
             ?key={}&appid={}&ticket={ticket_hex}&identity=matchmaking",
            self.api_key, self.app_id
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

        let steam_id_str = json["response"]["params"]["steamid"]
            .as_str()
            .ok_or_else(|| {
                LobbyError::SteamAuthFailed("unexpected ticket response format".into())
            })?;

        steam_id_str
            .parse::<u64>()
            .map_err(|e| LobbyError::SteamAuthFailed(format!("invalid steam id: {e}")))
    }

    /// Call GetPlayerSummaries to get display_name (cached 300s).
    pub async fn get_player_summary(&self, steam_id: SteamId) -> Result<String> {
        {
            let cache = self.display_name_cache.lock().unwrap();
            if let Some((name, at)) = cache.get(&steam_id) {
                if at.elapsed() < Duration::from_secs(300) {
                    return Ok(name.clone());
                }
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

    /// Generate a JWT session token bound to the player's current token_version.
    pub fn generate_session_token(
        &self,
        steam_id: SteamId,
        token_version: u32,
        ttl_secs: u64,
    ) -> Result<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let claims = Claims {
            sub: steam_id.to_string(),
            iat: now,
            exp: now + ttl_secs as usize,
            iss: "steam-lobby".into(),
            aud: "steam-lobby-client".into(),
            token_version,
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &self.jwt_encoding_key,
        )
        .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))
    }

    /// Validate a JWT session token; returns (steam_id, token_version).
    pub fn validate_session_token(&self, token: &str) -> Result<(SteamId, u32)> {
        let mut v = Validation::new(Algorithm::HS256);
        v.validate_exp = true;
        v.validate_aud = true;
        v.aud = Some(std::collections::HashSet::from([
            "steam-lobby-client".to_string(),
        ]));
        v.iss = Some(std::collections::HashSet::from(["steam-lobby".to_string()]));
        v.validate_nbf = true;

        let data = decode::<Claims>(token, &self.jwt_decoding_key, &v)
            .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))?;
        let steam_id = data
            .claims
            .sub
            .parse::<u64>()
            .map_err(|e| LobbyError::SteamAuthFailed(format!("invalid sub: {e}")))?;
        Ok((steam_id, data.claims.token_version))
    }
}
