use chrono::Utc;
use skillratings::Outcomes;

use crate::error::{LobbyError, Result};
use crate::mmr;
use crate::traits::{GameCallbacks, MatchStore, RatingStore};
use crate::types::{MatchInfo, MatchOutcome, MatchReport, MatchStatus, SteamId};

pub struct MatchManager<CB: GameCallbacks> {
    callbacks: CB,
    match_accept_timeout_secs: u64,
    report_timeout_secs: u64,
}

impl<CB: GameCallbacks> MatchManager<CB> {
    pub fn new(callbacks: CB, match_accept_timeout_secs: u64, report_timeout_secs: u64) -> Self {
        Self { callbacks, match_accept_timeout_secs, report_timeout_secs }
    }

    pub async fn accept_match(&self, token: &str, steam_id: SteamId, match_store: &dyn MatchStore) -> Result<()> {
        let m = match_store.get_match(token).await?.ok_or_else(|| LobbyError::MatchNotFound(token.to_string()))?;
        if m.status != MatchStatus::PendingAccept {
            return Err(LobbyError::MatchStateMismatch(token.to_string()));
        }
        if steam_id != m.player_a && steam_id != m.player_b {
            return Err(LobbyError::NotParticipant(token.to_string()));
        }
        if m.accepted_at.is_some() {
            match_store.update_match_status(token, MatchStatus::InProgress).await?;
            self.callbacks.on_match_accepted(&m).await?;
        } else {
            match_store.mark_accepted(token).await?;
        }
        Ok(())
    }

    pub async fn mark_connected(&self, token: &str, steam_id: SteamId, match_store: &dyn MatchStore) -> Result<()> {
        let m = match_store.get_match(token).await?.ok_or_else(|| LobbyError::MatchNotFound(token.to_string()))?;
        if m.status != MatchStatus::InProgress {
            return Err(LobbyError::MatchStateMismatch(token.to_string()));
        }
        if steam_id != m.player_a && steam_id != m.player_b {
            return Err(LobbyError::NotParticipant(token.to_string()));
        }
        if m.started_at.is_some() {
            match_store.update_match_status(token, MatchStatus::Reporting).await?;
            self.callbacks.on_players_connected(&m).await?;
        }
        Ok(())
    }

    pub async fn submit_report(&self, report: MatchReport, match_store: &dyn MatchStore, rating_store: &dyn RatingStore) -> Result<MatchOutcome> {
        let token = &report.match_token;
        let m = match_store.get_match(token).await?.ok_or_else(|| LobbyError::MatchNotFound(token.to_string()))?;
        if m.status != MatchStatus::Reporting {
            return Err(LobbyError::MatchStateMismatch(token.to_string()));
        }
        if report.reporting_player != m.player_a && report.reporting_player != m.player_b {
            return Err(LobbyError::NotParticipant(token.to_string()));
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
        let hashes_differ = ra.demo_hash.is_some() && rb.demo_hash.is_some() && ra.demo_hash != rb.demo_hash;

        if agree && !hashes_differ {
            let rating_a = rating_store.get_rating(m.player_a, &m.game_mode).await?;
            let rating_b = rating_store.get_rating(m.player_b, &m.game_mode).await?;
            let sk = if winner_a == Some(m.player_a) { Outcomes::WIN } else if winner_a == Some(m.player_b) { Outcomes::LOSS } else { Outcomes::DRAW };
            let (new_a, new_b) = mmr::update_ratings(&rating_a, &rating_b, sk);
            let mu_change_a = new_a.mu - rating_a.mu;
            match_store.update_match(token, MatchStatus::Resolved, Utc::now()).await?;
            let outcome = if winner_a == Some(m.player_a) {
                MatchOutcome::Win { mu_change: mu_change_a }
            } else if winner_a == Some(m.player_b) {
                MatchOutcome::Loss { mu_change: mu_change_a }
            } else {
                MatchOutcome::Draw { mu_change: mu_change_a }
            };
            self.callbacks.on_match_ended(&m, &outcome).await?;
            Ok(outcome)
        } else if agree && hashes_differ {
            match_store.update_match(token, MatchStatus::Disputed, Utc::now()).await?;
            Ok(MatchOutcome::Disputed)
        } else if !agree {
            let missing = ra.demo_hash.is_none() || rb.demo_hash.is_none();
            match_store.update_match(token, MatchStatus::Disputed, Utc::now()).await?;
            if missing { Ok(MatchOutcome::UnreviewableDispute) } else { Ok(MatchOutcome::Disputed) }
        } else {
            let rating_a = rating_store.get_rating(m.player_a, &m.game_mode).await?;
            let rating_b = rating_store.get_rating(m.player_b, &m.game_mode).await?;
            let sk = if winner_a == Some(m.player_a) { Outcomes::WIN } else if winner_a == Some(m.player_b) { Outcomes::LOSS } else { Outcomes::DRAW };
            let (new_a, new_b) = mmr::update_ratings(&rating_a, &rating_b, sk);
            let mu_change_a = new_a.mu - rating_a.mu;
            match_store.update_match(token, MatchStatus::Resolved, Utc::now()).await?;
            let outcome = if winner_a == Some(m.player_a) {
                MatchOutcome::Win { mu_change: mu_change_a }
            } else if winner_a == Some(m.player_b) {
                MatchOutcome::Loss { mu_change: mu_change_a }
            } else {
                MatchOutcome::Draw { mu_change: mu_change_a }
            };
            self.callbacks.on_match_ended(&m, &outcome).await?;
            Ok(outcome)
        }
    }

    pub async fn expire_pending_accepts(&self, match_store: &dyn MatchStore) -> Result<()> {
        let matches = match_store.get_matches_by_status(MatchStatus::PendingAccept).await?;
        let now = Utc::now();
        for m in &matches {
            if (now - m.created_at).num_seconds() as u64 > self.match_accept_timeout_secs {
                match_store.update_match_status(&m.match_token, MatchStatus::Disputed).await?;
            }
        }
        Ok(())
    }

    pub async fn expire_pending_reports(&self, match_store: &dyn MatchStore) -> Result<()> {
        let matches = match_store.get_matches_by_status(MatchStatus::Reporting).await?;
        let now = Utc::now();
        for m in &matches {
            if let Some(ended_at) = m.ended_at {
                if (now - ended_at).num_seconds() as u64 > self.report_timeout_secs {
                    let reports = match_store.get_reports(&m.match_token).await?;
                    if reports.is_empty() {
                        match_store.update_match_status(&m.match_token, MatchStatus::Disputed).await?;
                    } else if reports.len() == 1 {
                        match_store.update_match_status(&m.match_token, MatchStatus::Resolved).await?;
                    }
                }
            }
        }
        Ok(())
    }
}
