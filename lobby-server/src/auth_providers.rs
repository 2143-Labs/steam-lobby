//! OAuth2/OIDC provider registry: static config for Discord, runtime OIDC
//! discovery for au.2143.me (Pocket ID), PKCE helpers, and the authorization
//! URL builder. Steam is NOT here — its OpenID 2.0 flow stays bespoke.
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use serde::Deserialize;

/// OIDC discovery document (RFC 8414), the subset we consume.
#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
    userinfo_endpoint: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProviderKind {
    OAuth2,
    Oidc,
}

#[derive(Debug, Clone)]
pub struct ProviderConfig {
    pub id: String, // "discord" | "au2143"
    pub kind: ProviderKind,
    pub client_id: String,
    pub client_secret: String,
    pub authorization_endpoint: String,
    pub token_endpoint: String,
    pub userinfo_endpoint: String,
    pub scopes: Vec<String>,
    pub id_field: String,   // "id" (discord) | "sub" (au2143)
    pub name_field: String, // "global_name" (discord) | "preferred_username" (au2143)
    pub use_pkce: bool,     // false for discord (its OAuth2 has no PKCE)
}

impl ProviderConfig {
    /// The `{provider}` label shown on the demo login button.
    pub fn label(&self) -> &str {
        match self.id.as_str() {
            "discord" => "Discord",
            "au2143" => "au.2143.me",
            other => other,
        }
    }
}

pub struct AuthProviderRegistry {
    pub providers: Vec<ProviderConfig>,
}

impl AuthProviderRegistry {
    pub fn get(&self, id: &str) -> Option<&ProviderConfig> {
        self.providers.iter().find(|p| p.id == id)
    }
    pub fn ids(&self) -> Vec<&str> {
        self.providers.iter().map(|p| p.id.as_str()).collect()
    }
}

/// Build the registry from AppConfig provider fields. Discord is static;
/// au2143 runs runtime OIDC discovery and is DISABLED (with a log) when
/// discovery fails — never a boot error. Env overrides short-circuit
/// discovery (an operator with a blocked network sets them).
pub async fn build(
    discord_client_id: Option<String>,
    discord_client_secret: Option<String>,
    au2143_client_id: Option<String>,
    au2143_client_secret: Option<String>,
    au2143_issuer: String,
    au2143_overrides: Option<(String, String, String)>, // (authorize, token, userinfo)
    http: &reqwest::Client,
) -> AuthProviderRegistry {
    let mut providers = Vec::new();

    if let (Some(client_id), Some(client_secret)) = (discord_client_id, discord_client_secret) {
        providers.push(ProviderConfig {
            id: "discord".into(),
            kind: ProviderKind::OAuth2,
            client_id,
            client_secret,
            authorization_endpoint: "https://discord.com/oauth2/authorize".into(),
            token_endpoint: "https://discord.com/api/oauth2/token".into(),
            userinfo_endpoint: "https://discord.com/api/users/@me".into(),
            scopes: vec!["identify".into()],
            id_field: "id".into(),
            name_field: "global_name".into(),
            // Discord's OAuth2 documents only `state` + `client_secret`
            // protections (no PKCE); its token exchange accepts
            // client_id+client_secret in the form body, which this flow sends.
            use_pkce: false,
        });
    }

    if let Some(client_id) = au2143_client_id {
        let mut discovered = None;
        if let Some((auth, token, userinfo)) = au2143_overrides {
            discovered = Some((auth, token, userinfo));
        } else {
            // Browser-like UA: au.2143.me's bot protection 403s default
            // reqwest/curl UAs. Verified during planning — the live
            // document advertises S256 PKCE + the claim names in this file.
            let discovery_url = format!("{issuer}/.well-known/openid-configuration", issuer = au2143_issuer.trim_end_matches('/'));
            let resp = http
                .get(&discovery_url)
                .header(reqwest::header::USER_AGENT, "Mozilla/5.0 (X11; Linux x86_64; rv:126.0) Gecko/20100101 Firefox/126.0")
                .send()
                .await;
            match resp {
                Ok(r) if r.status().is_success() => match r.json::<OidcDiscovery>().await {
                    Ok(d) => discovered = Some((d.authorization_endpoint, d.token_endpoint, d.userinfo_endpoint)),
                    Err(e) => tracing::warn!("au2143 discovery JSON parse failed: {e}"),
                },
                Ok(r) => tracing::warn!(
                    "au2143 discovery returned HTTP {} — provider disabled; set AU2143_*_URL overrides",
                    r.status()
                ),
                Err(e) => tracing::warn!(
                    "au2143 discovery request failed: {e} — provider disabled; set AU2143_*_URL overrides"
                ),
            }
        }
        if let Some((authorization_endpoint, token_endpoint, userinfo_endpoint)) = discovered {
            providers.push(ProviderConfig {
                id: "au2143".into(),
                kind: ProviderKind::Oidc,
                client_id,
                client_secret: au2143_client_secret.unwrap_or_default(),
                authorization_endpoint,
                token_endpoint,
                userinfo_endpoint,
                // "groups" is required for the admin-flag capture (Step 8):
                // Pocket ID emits the `groups` claim only when requested.
                scopes: vec![
                    "openid".into(),
                    "profile".into(),
                    "email".into(),
                    "groups".into(),
                ],
                id_field: "sub".into(),
                name_field: "preferred_username".into(),
                use_pkce: true,
            });
        }
    }

    AuthProviderRegistry { providers }
}

/// PKCE pair: verifier = base64url(3 × uuid bytes); challenge = base64url(sha256(verifier)).
pub fn pkce_pair() -> (String, String) {
    let mut bytes = Vec::with_capacity(48);
    for _ in 0..3 {
        bytes.extend_from_slice(uuid::Uuid::new_v4().as_bytes());
    }
    let verifier = URL_SAFE_NO_PAD.encode(&bytes);
    use sha2::{Digest, Sha256};
    let challenge = URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()));
    (verifier, challenge)
}

/// Build the provider's authorization URL. `verifier` is Some only when
/// `cfg.use_pkce` — the challenge param is appended then. The caller stores
/// the verifier in the OpenIdState for the callback's token exchange.
pub fn authorization_url(
    cfg: &ProviderConfig,
    redirect_uri: &str,
    state: &str,
    verifier: Option<&str>,
    challenge: &str,
) -> String {
    let scopes = cfg.scopes.join("%20");
    let mut url = format!(
        "{ep}?response_type=code&client_id={cid}&redirect_uri={cb}&scope={scopes}&state={state}",
        ep = cfg.authorization_endpoint,
        cid = cfg.client_id,
        cb = redirect_uri,
    );
    if verifier.is_some() {
        url.push_str(&format!(
            "&code_challenge={challenge}&code_challenge_method=S256"
        ));
    }
    url
}

/// The player's display name from provider userinfo: `name_field` first, then
/// `username`, then `name`, then "Unknown".
pub fn userinfo_name(json: &serde_json::Value, cfg: &ProviderConfig) -> String {
    json[&cfg.name_field]
        .as_str()
        .or_else(|| json["username"].as_str())
        .or_else(|| json["name"].as_str())
        .unwrap_or("Unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pkce_pair_produces_distinct_verifier_and_challenge() {
        let (v1, c1) = pkce_pair();
        let (v2, c2) = pkce_pair();
        assert_ne!(v1, v2, "verifiers must be random");
        // verifier 64 chars for 48 bytes at 6 bits/char — within the RFC 7636
        // 43..=128 range; challenge likewise.
        assert!((43..=128).contains(&v1.len()), "verifier len {}", v1.len());
        assert!((43..=128).contains(&c1.len()), "challenge len {}", c1.len());
        assert_ne!(c1, c2, "challenges must be random");
    }

    #[test]
    fn authorization_url_contains_challenge_state_and_scopes() {
        let cfg = ProviderConfig {
            id: "au2143".into(),
            kind: ProviderKind::Oidc,
            client_id: "cid".into(),
            client_secret: "sec".into(),
            authorization_endpoint: "https://au.2143.me/oauth2/authorize".into(),
            token_endpoint: "https://au.2143.me/oauth2/token".into(),
            userinfo_endpoint: "https://au.2143.me/oauth2/userinfo".into(),
            scopes: vec!["openid".into(), "groups".into()],
            id_field: "sub".into(),
            name_field: "preferred_username".into(),
            use_pkce: true,
        };
        let (verifier, challenge) = pkce_pair();
        let url = crate::auth_providers::authorization_url(
            &cfg,
            "https://lobby.example/auth/au2143/callback",
            "state-123",
            Some(&verifier),
            &challenge,
        );
        assert!(url.contains("response_type=code"));
        assert!(url.contains("client_id=cid"));
        assert!(url.contains("redirect_uri="));
        assert!(url.contains("scope=openid%20groups"));
        assert!(url.contains("state=state-123"));
        assert!(url.contains("code_challenge="));
        assert!(url.contains("code_challenge_method=S256"));
    }

    #[test]
    fn userinfo_name_uses_name_field_with_fallbacks() {
        let cfg = ProviderConfig {
            id: "discord".into(),
            kind: ProviderKind::OAuth2,
            client_id: "c".into(),
            client_secret: "s".into(),
            authorization_endpoint: "a".into(),
            token_endpoint: "t".into(),
            userinfo_endpoint: "u".into(),
            scopes: vec!["identify".into()],
            id_field: "id".into(),
            name_field: "global_name".into(),
            use_pkce: false,
        };
        let j = serde_json::json!({"global_name": "Alice", "username": "alice#1"});
        assert_eq!(userinfo_name(&j, &cfg), "Alice");
        let j2 = serde_json::json!({"username": "alice#1"});
        assert_eq!(userinfo_name(&j2, &cfg), "alice#1");
        let j3 = serde_json::json!({"name": "bob"});
        assert_eq!(userinfo_name(&j3, &cfg), "bob");
        let j4 = serde_json::json!({});
        assert_eq!(userinfo_name(&j4, &cfg), "Unknown");
    }
}
