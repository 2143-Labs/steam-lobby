/// Client for the external gameserver creator — the service that provisions a
/// game server for a server-authoritative match and returns its address.
///
/// The creator contract (documented in README):
///   POST {creator_url} { match_token, game_mode, player_a, player_b, result_callback_url }
///   -> { server_address, join_token? }
///
/// Failure handling: any failure (no creator configured, request timeout,
/// non-200 response, unparseable body) returns `Err(String)`; the ticker logs
/// and retries on the next tick (a 2s-retry loop bounded by the alloc timeout).
pub struct GameserverClient {
    /// Absolute URL of the creator's allocate endpoint (None = no creator).
    pub creator_url: Option<String>,
    /// Base URL of the coordinator as the gameserver can reach it — used to
    /// build the result callback URL.
    pub callback_base: String,
}

#[derive(Debug, Clone, serde::Deserialize)]
pub struct Allocation {
    pub server_address: String,
    pub join_token: Option<String>,
}

impl GameserverClient {
    /// POST the match to the creator; the creator returns the server endpoint.
    pub async fn allocate(
        &self,
        m: &lobby_core::types::MatchInfo,
    ) -> Result<Allocation, String> {
        let creator_url = self
            .creator_url
            .as_deref()
            .ok_or_else(|| "no gameserver creator configured".to_string())?;
        let result_callback_url = format!(
            "{}/internal/game-result/{}/{}",
            self.callback_base,
            m.match_token,
            m.result_secret.as_deref().unwrap_or_default()
        );
        // Steam IDs travel as decimal strings on the wire (JS-safe, matches the
        // rest of the protocol); the creator/mocks read them with .as_str().
        let body = serde_json::json!({
            "match_token": m.match_token,
            "game_mode": m.game_mode,
            "player_a": m.player_a.to_string(),
            "player_b": m.player_b.to_string(),
            "result_callback_url": result_callback_url,
        });
        let resp = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            reqwest::Client::new().post(creator_url).json(&body).send(),
        )
        .await
        .map_err(|_| format!("gameserver creator request timed out for match {}", m.match_token))?
        .map_err(|e| format!("gameserver creator request failed for match {}: {e}", m.match_token))?;
        if !resp.status().is_success() {
            return Err(format!(
                "gameserver creator returned {} for match {}",
                resp.status(),
                m.match_token
            ));
        }
        let alloc: Allocation = resp
            .json()
            .await
            .map_err(|e| format!("gameserver creator bad response for match {}: {e}", m.match_token))?;
        Ok(alloc)
    }
}
