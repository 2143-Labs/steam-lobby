use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::Result;
use crate::types::{
    MatchDifficulty, MatchInfo, MatchReport, MatchStatus, OpenSkillRating, PlayerInfo,
    PlayerState, QueueEntry, SteamId,
};

/// Game-specific callbacks. Implement for your game type.
/// All methods have default no-op implementations so partial impls work.
#[async_trait]
pub trait GameCallbacks: Send + Sync {
    async fn on_player_in_menu(&self, _steam_id: SteamId) -> Result<()> {
        Ok(())
    }
    async fn on_player_queueing(
        &self,
        _steam_id: SteamId,
        _mode: &str,
        _difficulty: MatchDifficulty,
    ) -> Result<()> {
        Ok(())
    }
    async fn on_player_cancel_queue(&self, _steam_id: SteamId) -> Result<()> {
        Ok(())
    }
    /// Return false to reject the match for this player (game-specific logic).
    async fn on_match_found(&self, _match: &MatchInfo) -> Result<bool> {
        Ok(true)
    }
    async fn on_match_accepted(&self, _match: &MatchInfo) -> Result<()> {
        Ok(())
    }
    async fn on_players_connected(&self, _match: &MatchInfo) -> Result<()> {
        Ok(())
    }
    async fn on_match_ended(
        &self,
        _match: &MatchInfo,
        _outcome: &crate::types::MatchOutcome,
    ) -> Result<()> {
        Ok(())
    }
    async fn on_player_disconnected(&self, _steam_id: SteamId) -> Result<()> {
        Ok(())
    }
    async fn on_heartbeat(&self, _steam_id: SteamId) -> Result<()> {
        Ok(())
    }
    async fn on_generate_match_token(&self, _match: &MatchInfo) -> Result<String> {
        Ok(uuid::Uuid::new_v4().to_string())
    }
}

/// Storage abstraction — swap impls for testing vs production.
#[async_trait]
pub trait PlayerStore: Send + Sync {
    async fn upsert_player(&self, steam_id: SteamId, display_name: &str) -> Result<()>;
    async fn get_player_state(&self, steam_id: SteamId) -> Result<Option<PlayerInfo>>;
    async fn get_rating(&self, steam_id: SteamId, mode: &str) -> Result<OpenSkillRating>;
    async fn update_rating(
        &self,
        steam_id: SteamId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()>;
    async fn set_player_state(&self, steam_id: SteamId, state: PlayerState) -> Result<()>;
    async fn update_heartbeat(&self, steam_id: SteamId) -> Result<()>;
}

#[async_trait]
pub trait MatchStore: Send + Sync {
    async fn create_match(&self, match_info: &MatchInfo) -> Result<()>;
    async fn get_match(&self, token: &str) -> Result<Option<MatchInfo>>;
    async fn update_match_status(&self, token: &str, status: MatchStatus) -> Result<()>;
    async fn submit_report(&self, report: &MatchReport) -> Result<()>;
    async fn get_reports(&self, token: &str) -> Result<Vec<MatchReport>>;
    /// Get matches in a given status for expiry scanning.
    async fn get_matches_by_status(&self, status: MatchStatus) -> Result<Vec<MatchInfo>>;
    /// Update full match record (for resolving).
    async fn update_match(
        &self,
        token: &str,
        status: MatchStatus,
        ended_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<()>;
    /// Record the first player's acceptance timestamp.
    async fn mark_accepted(&self, token: &str) -> Result<()>;
    /// Record the first player's P2P connection timestamp.
    async fn mark_started(&self, token: &str) -> Result<()>;
}

#[async_trait]
pub trait QueueStore: Send + Sync {
    async fn enqueue(&self, entry: &QueueEntry) -> Result<()>;
    async fn dequeue(&self, steam_id: SteamId, mode: &str) -> Result<()>;
    async fn get_queue(&self, mode: &str) -> Result<Vec<QueueEntry>>;
    async fn remove_stale_queue_entries(
        &self,
        timeout: chrono::Duration,
    ) -> Result<()>;
}

#[async_trait]
pub trait RatingStore: Send + Sync {
    async fn get_rating(&self, steam_id: SteamId, mode: &str) -> Result<OpenSkillRating>;
    async fn update_rating(
        &self,
        steam_id: SteamId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()>;
}

#[async_trait]
pub trait SteamAuth: Send + Sync {
    /// Verify an OpenID callback and return the SteamID.
    async fn verify_openid(&self, params: &HashMap<String, String>) -> Result<SteamId>;
    /// Verify an in-game auth session ticket and return the SteamID.
    async fn verify_ticket(&self, ticket_hex: &str, identity: &str) -> Result<SteamId>;
    /// Fetch a player's Steam community display name.
    async fn get_player_summary(&self, steam_id: SteamId) -> Result<String>;
}
