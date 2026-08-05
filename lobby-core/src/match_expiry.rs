//! Ticker-driven maintenance: expiring matches that never reached a terminal
//! state. Same [`MatchManager`] as the player-facing actions in
//! [`crate::match_lifecycle`], but one concern per file.

use chrono::Utc;

use crate::error::Result;
use crate::match_lifecycle::MatchManager;
use crate::traits::{GameCallbacks, MatchStore, RatingStore};
use crate::types::{MatchOutcome, MatchStatus};

impl<CB: GameCallbacks> MatchManager<CB> {
    /// Expire server-authoritative matches whose gameserver never reported.
    /// Returns the tokens that were flipped to Disputed.
    pub async fn expire_playing_matches(
        &self,
        match_store: &dyn MatchStore,
        timeout_secs: u64,
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
                    expired.push(m.match_token.clone());
                }
            }
        }
        Ok(expired)
    }

    /// Expire matches whose 30s accept window elapsed with at least one player
    /// still undecided. Returns the tokens that were flipped to Disputed.
    pub async fn expire_pending_accepts(&self, match_store: &dyn MatchStore) -> Result<Vec<String>> {
        let matches = match_store
            .get_matches_by_status(MatchStatus::PendingAccept)
            .await?;
        let now = Utc::now();
        let mut expired = Vec::new();
        for m in &matches {
            if (now - m.created_at).num_seconds().max(0) as u64 > self.match_accept_timeout_secs {
                tracing::info!(
                    "match {} expired: no accept within {}s ({}, {})",
                    m.match_token,
                    self.match_accept_timeout_secs,
                    m.player_a,
                    m.player_b
                );
                match_store
                    .update_match_status(&m.match_token, MatchStatus::Disputed)
                    .await?;
                expired.push(m.match_token.clone());
            }
        }
        Ok(expired)
    }

    /// Resolve matches stuck in Reporting: no reports -> Disputed, a lone
    /// report -> the reported outcome (validated against the participants).
    /// Returns the resolved outcomes per token.
    pub async fn expire_pending_reports(
        &self,
        match_store: &dyn MatchStore,
        rating_store: &dyn RatingStore,
    ) -> Result<Vec<(String, MatchOutcome)>> {
        let matches = match_store
            .get_matches_by_status(MatchStatus::Reporting)
            .await?;
        let now = Utc::now();
        let mut resolved = Vec::new();
        for m in &matches {
            if let Some(ended_at) = m.ended_at {
                if (now - ended_at).num_seconds().max(0) as u64 > self.report_timeout_secs {
                    let reports = match_store.get_reports(&m.match_token).await?;
                    if reports.is_empty() {
                        tracing::info!(
                            "match {} report window expired with no reports -> Disputed",
                            m.match_token
                        );
                        match_store
                            .update_match_status(&m.match_token, MatchStatus::Disputed)
                            .await?;
                        resolved.push((m.match_token.clone(), MatchOutcome::Disputed));
                    } else if reports.len() == 1 {
                        let report = &reports[0];
                        let winner_ok = match report.winner {
                            None => true,
                            Some(w) => w == m.player_a || w == m.player_b,
                        };
                        if !winner_ok {
                            // A lone report claiming a non-participant winner cannot be trusted.
                            tracing::info!(
                                "match {} lone report claims non-participant winner -> Disputed",
                                m.match_token
                            );
                            match_store
                                .update_match_status(&m.match_token, MatchStatus::Disputed)
                                .await?;
                            resolved.push((m.match_token.clone(), MatchOutcome::Disputed));
                            continue;
                        }
                        let outcome = self
                            .resolve_agreed(m, report.winner, match_store, rating_store)
                            .await?;
                        let outcome_str = match &outcome {
                            MatchOutcome::Win { .. } => "Win",
                            MatchOutcome::Loss { .. } => "Loss",
                            MatchOutcome::Draw { .. } => "Draw",
                            _ => "Disputed",
                        };
                        tracing::info!(
                            "match {} report window expired, single report -> {} for {}",
                            m.match_token,
                            outcome_str,
                            m.player_a
                        );
                        resolved.push((m.match_token.clone(), outcome));
                    }
                }
            }
        }
        Ok(resolved)
    }
}
