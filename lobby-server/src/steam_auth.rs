use std::collections::HashMap;

use jsonwebtoken::{decode, encode, DecodingKey, EncodingKey, Header, Validation};
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
}

#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    sub: String,
    iat: usize,
    exp: usize,
}

impl SteamAuthService {
    pub fn new(api_key: String, app_id: u32, jwt_secret: String) -> Self {
        Self {
            api_key,
            app_id,
            http_client: Client::new(),
            jwt_encoding_key: EncodingKey::from_secret(jwt_secret.as_bytes()),
            jwt_decoding_key: DecodingKey::from_secret(jwt_secret.as_bytes()),
        }
    }

    /// Build the Steam OpenID redirect URL.
    pub fn openid_redirect_url(&self, return_to: &str, realm: &str) -> String {
        format!(
            "https://steamcommunity.com/openid/login?\
             openid.ns=http://specs.openid.net/auth/2.0&\
             openid.mode=checkid_setup&\
             openid.return_to={return_to}&\
             openid.realm={realm}&\
             openid.identity=http://specs.openid.net/auth/2.0/identifier_select&\
             openid.claimed_id=http://specs.openid.net/auth/2.0/identifier_select"
        )
    }

    /// Verify OpenID callback params.
    pub async fn verify_openid(&self, params: &HashMap<String, String>) -> Result<SteamId> {
        let mut verify_params = params.clone();
        verify_params.insert("openid.mode".to_string(), "check_authentication".to_string());

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

        if !body.contains("is_valid:true") {
            return Err(LobbyError::SteamAuthFailed("OpenID validation failed".into()));
        }

        let claimed_id = params
            .get("openid.claimed_id")
            .ok_or_else(|| LobbyError::SteamAuthFailed("missing claimed_id".into()))?;

        let prefix = "https://steamcommunity.com/openid/id/";
        let steam_id_str = claimed_id
            .strip_prefix(prefix)
            .ok_or_else(|| {
                LobbyError::SteamAuthFailed(format!("unexpected claimed_id: {claimed_id}"))
            })?;

        steam_id_str
            .parse::<u64>()
            .map_err(|e| LobbyError::SteamAuthFailed(format!("invalid steam id: {e}")))
    }

    /// Verify an in-game ticket via Steam Web API.
    pub async fn verify_ticket(&self, ticket_hex: &str) -> Result<SteamId> {
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

    /// Call GetPlayerSummaries to get display_name.
    pub async fn get_player_summary(&self, steam_id: SteamId) -> Result<String> {
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

        json["response"]["players"][0]["personaname"]
            .as_str()
            .map(|s| s.to_string())
            .ok_or_else(|| LobbyError::SteamAuthFailed("missing personaname".into()))
    }

    /// Generate a JWT session token.
    pub fn generate_session_token(&self, steam_id: SteamId) -> Result<String> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as usize;
        let claims = Claims {
            sub: steam_id.to_string(),
            iat: now,
            exp: now + 86400, // 24 hours
        };
        encode(&Header::default(), &claims, &self.jwt_encoding_key)
            .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))
    }

    /// Validate a JWT session token and return the SteamID.
    pub fn validate_session_token(&self, token: &str) -> Result<SteamId> {
        let data = decode::<Claims>(
            token,
            &self.jwt_decoding_key,
            &Validation::default(),
        )
        .map_err(|e| LobbyError::SteamAuthFailed(e.to_string()))?;
        data.claims
            .sub
            .parse::<u64>()
            .map_err(|e| LobbyError::SteamAuthFailed(format!("invalid sub: {e}")))
    }
}
