use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use lobby_core::error::Result;
use lobby_core::traits::{GameCallbacks, MatchStore, PlayerStore, QueueStore, RatingStore};
use lobby_core::types::{
    MatchEvent, MatchInfo, MatchReport, MatchStatus, OpenSkillRating, PlayerId, PlayerInfo,
    PlayerState, QueueEntry,
};
use parking_lot::Mutex;
use std::collections::HashMap;

// ── Mock Stores ──────────────────────────────────────────

pub type StoredResult = (String, Option<f64>, Option<f64>);

pub struct MockStore {
    pub players: Mutex<HashMap<PlayerId, PlayerInfo>>,
    pub ratings: Mutex<HashMap<(PlayerId, String), OpenSkillRating>>,
    pub matches: Mutex<HashMap<String, MatchInfo>>,
    pub reports: Mutex<HashMap<String, Vec<MatchReport>>>,
    pub queue: Mutex<HashMap<(PlayerId, String), QueueEntry>>,
    pub results: Mutex<HashMap<String, StoredResult>>,
    pub token_versions: Mutex<HashMap<PlayerId, u32>>,
    pub events: Mutex<Vec<(String, MatchEvent, Option<PlayerId>)>>,
}

impl MockStore {
    pub fn new() -> Self {
        Self {
            players: Mutex::new(HashMap::new()),
            ratings: Mutex::new(HashMap::new()),
            matches: Mutex::new(HashMap::new()),
            reports: Mutex::new(HashMap::new()),
            queue: Mutex::new(HashMap::new()),
            results: Mutex::new(HashMap::new()),
            token_versions: Mutex::new(HashMap::new()),
            events: Mutex::new(Vec::new()),
        }
    }
}

#[async_trait]
impl PlayerStore for MockStore {
    async fn upsert_player(&self, user_id: PlayerId, display_name: &str) -> Result<()> {
        let mut p = self.players.lock();
        p.entry(user_id)
            .and_modify(|pi| {
                pi.display_name = display_name.to_string();
            })
            .or_insert_with(|| PlayerInfo {
                user_id,
                display_name: display_name.to_string(),
                state: PlayerState::InMenus,
                last_heartbeat: Utc::now(),
            });
        Ok(())
    }

    async fn get_player_state(&self, user_id: PlayerId) -> Result<Option<PlayerInfo>> {
        Ok(self.players.lock().get(&user_id).cloned())
    }

    async fn get_rating(&self, user_id: PlayerId, mode: &str) -> Result<OpenSkillRating> {
        let r = self.ratings.lock();
        Ok(r.get(&(user_id, mode.to_string()))
            .cloned()
            .unwrap_or(OpenSkillRating {
                mu: 25.0,
                sigma: 25.0 / 3.0,
                last_updated: Utc::now(),
            }))
    }

    async fn update_rating(
        &self,
        user_id: PlayerId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        self.ratings
            .lock()
            .insert((user_id, mode.to_string()), rating.clone());
        Ok(())
    }

    async fn set_player_state(&self, user_id: PlayerId, state: PlayerState) -> Result<()> {
        let mut p = self.players.lock();
        if let Some(info) = p.get_mut(&user_id) {
            info.state = state;
        }
        Ok(())
    }

    async fn update_heartbeat(&self, user_id: PlayerId) -> Result<()> {
        let mut p = self.players.lock();
        if let Some(info) = p.get_mut(&user_id) {
            info.last_heartbeat = Utc::now();
        }
        Ok(())
    }

    async fn get_token_version(&self, user_id: PlayerId) -> Result<u32> {
        Ok(self
            .token_versions
            .lock()
            .get(&user_id)
            .copied()
            .unwrap_or(0))
    }

    async fn bump_token_version(&self, user_id: PlayerId) -> Result<()> {
        let mut v = self.token_versions.lock();
        *v.entry(user_id).or_insert(0) += 1;
        Ok(())
    }
}

#[async_trait]
impl MatchStore for MockStore {
    async fn create_match(&self, match_info: &MatchInfo) -> Result<()> {
        self.matches
            .lock()
            .insert(match_info.match_token.clone(), match_info.clone());
        Ok(())
    }

    async fn get_match(&self, token: &str) -> Result<Option<MatchInfo>> {
        Ok(self.matches.lock().get(token).cloned())
    }

    async fn update_match_status(&self, token: &str, status: MatchStatus) -> Result<()> {
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            mi.status = status;
            if status == MatchStatus::InProgress {
                mi.accepted_at = Some(Utc::now());
            }
            if status == MatchStatus::Reporting {
                mi.started_at = Some(Utc::now());
            }
        }
        Ok(())
    }

    async fn submit_report(&self, report: &MatchReport) -> Result<()> {
        self.reports
            .lock()
            .entry(report.match_token.clone())
            .or_default()
            .push(report.clone());
        Ok(())
    }

    async fn get_reports(&self, token: &str) -> Result<Vec<MatchReport>> {
        Ok(self.reports.lock().get(token).cloned().unwrap_or_default())
    }

    async fn get_matches_by_status(&self, status: MatchStatus) -> Result<Vec<MatchInfo>> {
        Ok(self
            .matches
            .lock()
            .values()
            .filter(|m| m.status == status)
            .cloned()
            .collect())
    }

    async fn update_match(
        &self,
        token: &str,
        status: MatchStatus,
        ended_at: DateTime<Utc>,
    ) -> Result<()> {
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            mi.status = status;
            mi.ended_at = Some(ended_at);
        }
        Ok(())
    }

    async fn mark_accepted(&self, token: &str, user_id: PlayerId) -> Result<bool> {
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            if mi.player_a == user_id {
                mi.accepted_a = true;
            } else if mi.player_b == user_id {
                mi.accepted_b = true;
            }
            if mi.accepted_a && mi.accepted_b {
                mi.accepted_at = Some(Utc::now());
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn mark_started(&self, token: &str, user_id: PlayerId) -> Result<bool> {
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            if mi.player_a == user_id {
                mi.connected_a = true;
            } else if mi.player_b == user_id {
                mi.connected_b = true;
            }
            if mi.connected_a && mi.connected_b {
                mi.started_at = Some(Utc::now());
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn mark_server_ready(
        &self,
        token: &str,
        address: &str,
        join_token: Option<&str>,
    ) -> Result<()> {
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            mi.status = MatchStatus::Playing;
            mi.server_address = Some(address.to_string());
            mi.join_token = join_token.map(|s| s.to_string());
            mi.started_at = Some(Utc::now());
        }
        Ok(())
    }

    async fn record_match_event(
        &self,
        match_token: &str,
        event: MatchEvent,
        actor: Option<PlayerId>,
    ) -> Result<()> {
        self.events
            .lock()
            .push((match_token.to_string(), event, actor));
        Ok(())
    }

    async fn write_match_result(
        &self,
        token: &str,
        outcome: &str,
        mu_change_a: Option<f64>,
        mu_change_b: Option<f64>,
    ) -> Result<()> {
        self.results.lock().insert(
            token.to_string(),
            (outcome.to_string(), mu_change_a, mu_change_b),
        );
        Ok(())
    }

    async fn recent_match_between(
        &self,
        a: PlayerId,
        b: PlayerId,
        since: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self.matches.lock().values().any(|m| {
            m.status == MatchStatus::Resolved
                && ((m.player_a == a && m.player_b == b) || (m.player_a == b && m.player_b == a))
                && m.ended_at.is_some_and(|e| e >= since)
        }))
    }

    async fn resolve_match(
        &self,
        token: &str,
        game_mode: &str,
        player_a: PlayerId,
        player_b: PlayerId,
        outcome: &str,
        mu_change_a: Option<f64>,
        mu_change_b: Option<f64>,
        rating_a: &OpenSkillRating,
        rating_b: &OpenSkillRating,
    ) -> Result<()> {
        {
            let mut r = self.ratings.lock();
            r.insert((player_a, game_mode.to_string()), rating_a.clone());
            r.insert((player_b, game_mode.to_string()), rating_b.clone());
        }
        self.results.lock().insert(
            token.to_string(),
            (outcome.to_string(), mu_change_a, mu_change_b),
        );
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            mi.status = MatchStatus::Resolved;
            mi.ended_at = Some(Utc::now());
        }
        Ok(())
    }
}

#[async_trait]
impl QueueStore for MockStore {
    async fn enqueue(&self, entry: &QueueEntry) -> Result<()> {
        self.queue
            .lock()
            .insert((entry.user_id, entry.game_mode.clone()), entry.clone());
        Ok(())
    }

    async fn dequeue(&self, user_id: PlayerId, mode: &str) -> Result<()> {
        self.queue.lock().remove(&(user_id, mode.to_string()));
        Ok(())
    }

    async fn get_queue(&self, mode: &str) -> Result<Vec<QueueEntry>> {
        Ok(self
            .queue
            .lock()
            .values()
            .filter(|e| e.game_mode == mode)
            .cloned()
            .collect())
    }

    async fn remove_stale_queue_entries(&self, timeout: Duration) -> Result<Vec<PlayerId>> {
        let cutoff = Utc::now() - timeout;
        let mut queue = self.queue.lock();
        let stale: Vec<PlayerId> = queue
            .values()
            .filter(|e| e.queued_at < cutoff)
            .map(|e| e.user_id)
            .collect();
        for id in &stale {
            queue.retain(|(uid, _mode), _e| uid != id);
        }
        Ok(stale)
    }

    async fn is_queued(&self, user_id: PlayerId) -> Result<bool> {
        Ok(self.queue.lock().keys().any(|(uid, _)| *uid == user_id))
    }

    async fn get_queued_entry(&self, user_id: PlayerId) -> Result<Option<QueueEntry>> {
        Ok(self
            .queue
            .lock()
            .values()
            .filter(|e| e.user_id == user_id)
            .max_by_key(|e| e.queued_at)
            .cloned())
    }
}

#[async_trait]
impl RatingStore for MockStore {
    async fn get_rating(&self, user_id: PlayerId, mode: &str) -> Result<OpenSkillRating> {
        <Self as PlayerStore>::get_rating(self, user_id, mode).await
    }

    async fn list_ratings(&self, mode: &str) -> Result<Vec<(PlayerId, OpenSkillRating)>> {
        let r = self.ratings.lock();
        let mut all: Vec<(PlayerId, OpenSkillRating)> = r
            .iter()
            .filter(|((_id, m), _r)| m == mode)
            .map(|((id, _mode), rating)| (*id, rating.clone()))
            .collect();
        all.sort_by(|a, b| {
            (b.1.mu - 3.0 * b.1.sigma)
                .partial_cmp(&(a.1.mu - 3.0 * a.1.sigma))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        Ok(all)
    }

    async fn update_rating(
        &self,
        user_id: PlayerId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        self.ratings
            .lock()
            .insert((user_id, mode.to_string()), rating.clone());
        Ok(())
    }
}

#[derive(Clone, Default)]
pub struct TestCallbacks;
impl GameCallbacks for TestCallbacks {}

// ── Helpers ──────────────────────────────────────────────

/// Deterministic small UUIDs for tests: n=0 → 0x1, n=1 → 0x2, …
pub fn pid(n: u64) -> PlayerId {
    uuid::Uuid::from_u128(n as u128 + 1)
}

#[allow(dead_code)] // only some test binaries use it
pub fn queued_player(id: PlayerId, _mu: f64) -> PlayerInfo {
    PlayerInfo {
        user_id: id,
        display_name: format!("P{id}"),
        state: PlayerState::Queueing,
        last_heartbeat: Utc::now(),
    }
}
