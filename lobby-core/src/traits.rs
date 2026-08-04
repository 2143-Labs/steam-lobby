use async_trait::async_trait;
use std::collections::HashMap;

use crate::error::Result;
use crate::types::{
    MatchDifficulty, MatchInfo, MatchReport, MatchStatus, OpenSkillRating, PlayerInfo, PlayerState,
    QueueEntry, SteamId,
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
    async fn get_token_version(&self, steam_id: SteamId) -> Result<u32>;
    async fn bump_token_version(&self, steam_id: SteamId) -> Result<()>;
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
    /// Record one player's acceptance.
    /// Returns true when, after this call, BOTH players have accepted.
    async fn mark_accepted(&self, token: &str, steam_id: SteamId) -> Result<bool>;
    /// Record one player's P2P connection.
    /// Returns true when, after this call, BOTH players have connected.
    async fn mark_started(&self, token: &str, steam_id: SteamId) -> Result<bool>;
    /// Persist a resolved match outcome record.
    async fn write_match_result(
        &self,
        token: &str,
        outcome: &str,
        mu_change_a: Option<f64>,
        mu_change_b: Option<f64>,
    ) -> Result<()>;
    /// True if the pair has a Resolved match that ended at/after `since`.
    async fn recent_match_between(
        &self,
        a: SteamId,
        b: SteamId,
        since: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool>;
    /// Atomically apply a full match resolution (ratings, result record, status).
    #[allow(clippy::too_many_arguments)]
    async fn resolve_match(
        &self,
        token: &str,
        game_mode: &str,
        player_a: SteamId,
        player_b: SteamId,
        outcome: &str,
        mu_change_a: Option<f64>,
        mu_change_b: Option<f64>,
        rating_a: &OpenSkillRating,
        rating_b: &OpenSkillRating,
    ) -> Result<()>;
}

#[async_trait]
pub trait QueueStore: Send + Sync {
    async fn enqueue(&self, entry: &QueueEntry) -> Result<()>;
    async fn dequeue(&self, steam_id: SteamId, mode: &str) -> Result<()>;
    async fn get_queue(&self, mode: &str) -> Result<Vec<QueueEntry>>;
    /// Remove stale entries; returns the steam_ids that were removed.
    async fn remove_stale_queue_entries(&self, timeout: chrono::Duration) -> Result<Vec<SteamId>>;
}

#[async_trait]
pub trait RatingStore: Send + Sync {
    async fn get_rating(&self, steam_id: SteamId, mode: &str) -> Result<OpenSkillRating>;
    /// All ratings, ordered by display rating (`mu - 3*sigma`) descending.
    async fn list_ratings(&self) -> Result<Vec<(SteamId, OpenSkillRating)>>;
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
