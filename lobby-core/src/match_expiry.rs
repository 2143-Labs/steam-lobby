//! Ticker-driven maintenance: expiring matches that never reached a terminal
//! state. Same [`MatchManager`] as the player-facing actions in
//! [`crate::match_lifecycle`], but one concern per file.

use chrono::Utc;

use crate::error::Result;
use crate::match_lifecycle::MatchManager;
use crate::traits::{GameCallbacks, MatchStore, PlayerStore};
use crate::types::{MatchStatus, PlayerState};

impl<CB: GameCallbacks> MatchManager<CB> {
    /// Expire server-authoritative matches whose gameserver never reported.
    /// Returns the tokens that were flipped to Disputed.
    pub async fn expire_playing_matches(
        &self,
        match_store: &dyn MatchStore,
        timeout_secs: u64,
        player_store: &dyn PlayerStore,
    ) -> Result<Vec<String>> {
        let matches = match_store.get_matches_by_status(MatchStatus::Playing).await?;
        let now = Utc::now();
        let mut expired = Vec::new();
        for m in &matches {
            if let Some(started) = m.started_at {
                if (now - started).num_seconds().max(0) as u64 > timeout_secs {
                    tracing::info!(
                        "match {} no result from gameserver within {}s -> Disputed",
                        m.match_token, timeout_secs
                    );
                    match_store
                        .update_match_status(&m.match_token, MatchStatus::Disputed)
                        .await?;
                    // Terminal: both players are free to queue again.
                    player_store.set_player_state(m.player_a, PlayerState::InMenus).await?;
                    player_store.set_player_state(m.player_b, PlayerState::InMenus).await?;
                    expired.push(m.match_token.clone());
                }
            }
        }
        Ok(expired)
    }
}
