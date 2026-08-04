use chrono::Utc;
use skillratings::Outcomes;

use crate::error::{LobbyError, Result};
use crate::mmr;
use crate::traits::{GameCallbacks, MatchStore, RatingStore};
use crate::types::{MatchOutcome, MatchReport, MatchStatus, SteamId};

pub struct MatchManager<CB: GameCallbacks> {
    callbacks: CB,
    match_accept_timeout_secs: u64,
    report_timeout_secs: u64,
}

impl<CB: GameCallbacks> MatchManager<CB> {
    pub fn new(callbacks: CB, match_accept_timeout_secs: u64, report_timeout_secs: u64) -> Self {
        Self {
            callbacks,
            match_accept_timeout_secs,
            report_timeout_secs,
        }
    }

    pub async fn accept_match(
        &self,
        token: &str,
        steam_id: SteamId,
        match_store: &dyn MatchStore,
    ) -> Result<()> {
        let m = match_store
            .get_match(token)
            .await?
            .ok_or_else(|| LobbyError::MatchNotFound(token.to_string()))?;
        if m.status != MatchStatus::PendingAccept {
            return Err(LobbyError::MatchStateMismatch(token.to_string()));
        }
        if steam_id != m.player_a && steam_id != m.player_b {
            return Err(LobbyError::NotParticipant(token.to_string()));
        }
        if match_store.mark_accepted(token, steam_id).await? {
            // Both players have now accepted — transition to InProgress.
            tracing::info!(
                "match {} both players accepted -> InProgress ({}, {})",
                m.match_token,
                m.player_a,
                m.player_b
            );
            match_store
                .update_match_status(token, MatchStatus::InProgress)
                .await?;
            self.callbacks.on_match_accepted(&m).await?;
        }
        Ok(())
    }

    pub async fn mark_connected(
        &self,
        token: &str,
        steam_id: SteamId,
        match_store: &dyn MatchStore,
    ) -> Result<()> {
        let m = match_store
            .get_match(token)
            .await?
            .ok_or_else(|| LobbyError::MatchNotFound(token.to_string()))?;
        if m.status != MatchStatus::InProgress {
            return Err(LobbyError::MatchStateMismatch(token.to_string()));
        }
        if steam_id != m.player_a && steam_id != m.player_b {
            return Err(LobbyError::NotParticipant(token.to_string()));
        }
        if match_store.mark_started(token, steam_id).await? {
            // Both players have now connected — transition to Reporting.
            // Use update_match (sets ended_at) so the report-timeout expiry path is reachable.
            tracing::info!(
                "match {} both players P2P-connected -> Reporting ({}, {})",
                m.match_token,
                m.player_a,
                m.player_b
            );
            match_store
                .update_match(&m.match_token, MatchStatus::Reporting, Utc::now())
                .await?;
            self.callbacks.on_players_connected(&m).await?;
        }
        Ok(())
    }

    pub async fn submit_report(
        &self,
        report: MatchReport,
        match_store: &dyn MatchStore,
        rating_store: &dyn RatingStore,
    ) -> Result<MatchOutcome> {
        let token = &report.match_token;
        let m = match_store
            .get_match(token)
            .await?
            .ok_or_else(|| LobbyError::MatchNotFound(token.to_string()))?;
        if m.status != MatchStatus::Reporting {
            return Err(LobbyError::MatchStateMismatch(token.to_string()));
        }
        if report.reporting_player != m.player_a && report.reporting_player != m.player_b {
            return Err(LobbyError::NotParticipant(token.to_string()));
        }
        if let Some(h) = &report.demo_hash {
            if h.len() > 64 {
                return Err(LobbyError::InvalidReport(token.clone()));
            }
        }
        if let Some(w) = report.winner {
            if w != m.player_a && w != m.player_b {
                return Err(LobbyError::InvalidReport(token.clone()));
            }
        }
        match_store.submit_report(&report).await?;
        let reports = match_store.get_reports(token).await?;
        if reports.len() < 2 {
            return Ok(MatchOutcome::Disputed);
        }
        let ra = &reports[0];
        let rb = &reports[1];
        let winner_a = ra.winner;
        let winner_b = rb.winner;
        let agree = winner_a == winner_b;
        let hashes_differ =
            ra.demo_hash.is_some() && rb.demo_hash.is_some() && ra.demo_hash != rb.demo_hash;

        if agree && !hashes_differ {
            let rating_a = rating_store.get_rating(m.player_a, &m.game_mode).await?;
            let rating_b = rating_store.get_rating(m.player_b, &m.game_mode).await?;
            let sk = if winner_a == Some(m.player_a) {
                Outcomes::WIN
            } else if winner_a == Some(m.player_b) {
                Outcomes::LOSS
            } else {
                Outcomes::DRAW
            };
            let (new_a, new_b) = mmr::update_ratings(&rating_a, &rating_b, sk);
            let outcome_str = if winner_a == Some(m.player_a) { "Win" } else if winner_a == Some(m.player_b) { "Loss" } else { "Draw" };
            let mu_change_b = new_b.mu - rating_b.mu;
            let mu_change_a = new_a.mu - rating_a.mu;
            match_store
                .resolve_match(
                    token,
                    &m.game_mode,
                    m.player_a,
                    m.player_b,
                    outcome_str,
                    Some(mu_change_a),
                    Some(mu_change_b),
                    &new_a,
                    &new_b,
                )
                .await?;
            let outcome = if winner_a == Some(m.player_a) {
                MatchOutcome::Win {
                    mu_change: mu_change_a,
                }
            } else if winner_a == Some(m.player_b) {
                MatchOutcome::Loss {
                    mu_change: mu_change_a,
                }
            } else {
                MatchOutcome::Draw {
                    mu_change: mu_change_a,
                }
            };
            self.callbacks.on_match_ended(&m, &outcome).await?;
            Ok(outcome)
        } else if agree && hashes_differ {
            match_store
                .update_match(token, MatchStatus::Disputed, Utc::now())
                .await?;
            Ok(MatchOutcome::Disputed)
        } else {
            let missing = ra.demo_hash.is_none() || rb.demo_hash.is_none();
            match_store
                .update_match(token, MatchStatus::Disputed, Utc::now())
                .await?;
            if missing {
                Ok(MatchOutcome::UnreviewableDispute)
            } else {
                Ok(MatchOutcome::Disputed)
            }
        }
    }
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
                        let rating_a = rating_store.get_rating(m.player_a, &m.game_mode).await?;
                        let rating_b = rating_store.get_rating(m.player_b, &m.game_mode).await?;
                        let sk = if report.winner == Some(m.player_a) {
                            Outcomes::WIN
                        } else if report.winner == Some(m.player_b) {
                            Outcomes::LOSS
                        } else {
                            Outcomes::DRAW
                        };
                        let (new_a, new_b) = mmr::update_ratings(&rating_a, &rating_b, sk);
                        let outcome_str = if report.winner == Some(m.player_a) {
                            "Win"
                        } else if report.winner == Some(m.player_b) {
                            "Loss"
                        } else {
                            "Draw"
                        };
                        let mu_change_a = new_a.mu - rating_a.mu;
                        let mu_change_b = new_b.mu - rating_b.mu;
                        tracing::info!(
                            "match {} report window expired, single report -> {} for {}",
                            m.match_token,
                            outcome_str,
                            m.player_a
                        );
                        match_store
                            .resolve_match(
                                &m.match_token,
                                &m.game_mode,
                                m.player_a,
                                m.player_b,
                                outcome_str,
                                Some(mu_change_a),
                                Some(mu_change_b),
                                &new_a,
                                &new_b,
                            )
                            .await?;
                        let outcome = if report.winner == Some(m.player_a) {
                            MatchOutcome::Win {
                                mu_change: mu_change_a,
                            }
                        } else if report.winner == Some(m.player_b) {
                            MatchOutcome::Loss {
                                mu_change: mu_change_a,
                            }
                        } else {
                            MatchOutcome::Draw {
                                mu_change: mu_change_a,
                            }
                        };
                        resolved.push((m.match_token.clone(), outcome));
                    }
                }
            }
        }
        Ok(resolved)
    }
}
