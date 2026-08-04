use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use lobby_core::error::{LobbyError, Result};
use lobby_core::traits::{GameCallbacks, MatchStore, PlayerStore, QueueStore, RatingStore};
use lobby_core::types::{
    MatchDifficulty, MatchInfo, MatchReport, MatchStatus, OpenSkillRating, PlayerInfo, PlayerState,
    QueueEntry, SteamId,
};
use parking_lot::Mutex;
use std::collections::HashMap;

// ── Mock Stores ──────────────────────────────────────────

type StoredResult = (String, Option<f64>, Option<f64>);

struct MockStore {
    players: Mutex<HashMap<SteamId, PlayerInfo>>,
    ratings: Mutex<HashMap<(SteamId, String), OpenSkillRating>>,
    matches: Mutex<HashMap<String, MatchInfo>>,
    reports: Mutex<HashMap<String, Vec<MatchReport>>>,
    queue: Mutex<HashMap<(SteamId, String), QueueEntry>>,
    results: Mutex<HashMap<String, StoredResult>>,
    token_versions: Mutex<HashMap<SteamId, u32>>,
}

impl MockStore {
    fn new() -> Self {
        Self {
            players: Mutex::new(HashMap::new()),
            ratings: Mutex::new(HashMap::new()),
            matches: Mutex::new(HashMap::new()),
            reports: Mutex::new(HashMap::new()),
            queue: Mutex::new(HashMap::new()),
            results: Mutex::new(HashMap::new()),
            token_versions: Mutex::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl PlayerStore for MockStore {
    async fn upsert_player(&self, steam_id: SteamId, display_name: &str) -> Result<()> {
        let mut p = self.players.lock();
        p.entry(steam_id)
            .and_modify(|pi| {
                pi.display_name = display_name.to_string();
            })
            .or_insert_with(|| PlayerInfo {
                steam_id,
                display_name: display_name.to_string(),
                state: PlayerState::InMenus,
                last_heartbeat: Utc::now(),
            });
        Ok(())
    }

    async fn get_player_state(&self, steam_id: SteamId) -> Result<Option<PlayerInfo>> {
        Ok(self.players.lock().get(&steam_id).cloned())
    }

    async fn get_rating(&self, steam_id: SteamId, mode: &str) -> Result<OpenSkillRating> {
        let r = self.ratings.lock();
        Ok(r.get(&(steam_id, mode.to_string()))
            .cloned()
            .unwrap_or(OpenSkillRating {
                mu: 25.0,
                sigma: 25.0 / 3.0,
                last_updated: Utc::now(),
            }))
    }

    async fn update_rating(
        &self,
        steam_id: SteamId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        self.ratings
            .lock()
            .insert((steam_id, mode.to_string()), rating.clone());
        Ok(())
    }

    async fn set_player_state(&self, steam_id: SteamId, state: PlayerState) -> Result<()> {
        let mut p = self.players.lock();
        if let Some(info) = p.get_mut(&steam_id) {
            info.state = state;
        }
        Ok(())
    }

    async fn update_heartbeat(&self, steam_id: SteamId) -> Result<()> {
        let mut p = self.players.lock();
        if let Some(info) = p.get_mut(&steam_id) {
            info.last_heartbeat = Utc::now();
        }
        Ok(())
    }

    async fn get_token_version(&self, steam_id: SteamId) -> Result<u32> {
        Ok(self.token_versions.lock().get(&steam_id).copied().unwrap_or(0))
    }

    async fn bump_token_version(&self, steam_id: SteamId) -> Result<()> {
        let mut v = self.token_versions.lock();
        *v.entry(steam_id).or_insert(0) += 1;
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

    async fn mark_accepted(&self, token: &str, steam_id: SteamId) -> Result<bool> {
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            if mi.player_a == steam_id {
                mi.accepted_a = true;
            } else if mi.player_b == steam_id {
                mi.accepted_b = true;
            }
            if mi.accepted_a && mi.accepted_b {
                mi.accepted_at = Some(Utc::now());
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn mark_started(&self, token: &str, steam_id: SteamId) -> Result<bool> {
        let mut m = self.matches.lock();
        if let Some(mi) = m.get_mut(token) {
            if mi.player_a == steam_id {
                mi.connected_a = true;
            } else if mi.player_b == steam_id {
                mi.connected_b = true;
            }
            if mi.connected_a && mi.connected_b {
                mi.started_at = Some(Utc::now());
                return Ok(true);
            }
        }
        Ok(false)
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
        a: SteamId,
        b: SteamId,
        since: DateTime<Utc>,
    ) -> Result<bool> {
        Ok(self
            .matches
            .lock()
            .values()
            .any(|m| {
                m.status == MatchStatus::Resolved
                    && ((m.player_a == a && m.player_b == b)
                        || (m.player_a == b && m.player_b == a))
                    && m.ended_at.is_some_and(|e| e >= since)
            }))
    }

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
            .insert((entry.steam_id, entry.game_mode.clone()), entry.clone());
        Ok(())
    }

    async fn dequeue(&self, steam_id: SteamId, mode: &str) -> Result<()> {
        self.queue.lock().remove(&(steam_id, mode.to_string()));
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

    async fn remove_stale_queue_entries(&self, _timeout: Duration) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl RatingStore for MockStore {
    async fn get_rating(&self, steam_id: SteamId, mode: &str) -> Result<OpenSkillRating> {
        <Self as PlayerStore>::get_rating(self, steam_id, mode).await
    }

    async fn list_ratings(&self) -> Result<Vec<(SteamId, OpenSkillRating)>> {
        let r = self.ratings.lock();
        let mut all: Vec<(SteamId, OpenSkillRating)> = r
            .iter()
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
        steam_id: SteamId,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        self.ratings
            .lock()
            .insert((steam_id, mode.to_string()), rating.clone());
        Ok(())
    }
}

#[derive(Clone, Default)]
struct TestCallbacks;
impl GameCallbacks for TestCallbacks {}

// ── Helpers ──────────────────────────────────────────────

fn queued_player(id: SteamId, _mu: f64) -> PlayerInfo {
    PlayerInfo {
        steam_id: id,
        display_name: format!("P{id}"),
        state: PlayerState::Queueing,
        last_heartbeat: Utc::now(),
    }
}

// ── Tests ────────────────────────────────────────────────

#[tokio::test]
async fn full_match_lifecycle() {
    let store = MockStore::new();

    // Insert queueing players
    store.players.lock().insert(100, queued_player(100, 25.0));
    store.players.lock().insert(200, queued_player(200, 25.0));

    // Enqueue both
    store
        .enqueue(&QueueEntry {
            steam_id: 100,
            game_mode: "ranked_1v1".into(),
            difficulty: MatchDifficulty::Normal,
            mu: 25.0,
            queued_at: Utc::now(),
        })
        .await
        .unwrap();
    store
        .enqueue(&QueueEntry {
            steam_id: 200,
            game_mode: "ranked_1v1".into(),
            difficulty: MatchDifficulty::Normal,
            mu: 25.0,
            queued_at: Utc::now(),
        })
        .await
        .unwrap();

    // Tick
    let queue = lobby_core::queue::MatchmakingQueue::new(TestCallbacks);
    let result = queue
        .tick("ranked_1v1", &store, &store, &store, &store)
        .await
        .unwrap();
    assert!(result.is_some());
    let m = result.unwrap();

    // Accept
    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks, 30, 300);
    mgr.accept_match(&m.match_token, 100, &store).await.unwrap();
    mgr.accept_match(&m.match_token, 200, &store).await.unwrap();

    // Connect both players via P2P — real two-phase flow
    mgr.mark_connected(&m.match_token, 100, &store)
        .await
        .unwrap();
    mgr.mark_connected(&m.match_token, 200, &store)
        .await
        .unwrap();

    // Verify match transitioned to Reporting
    let updated = store.get_match(&m.match_token).await.unwrap().unwrap();
    assert_eq!(updated.status, MatchStatus::Reporting);

    // First report — Alice says she won
    let first = mgr
        .submit_report(
            MatchReport {
                match_token: m.match_token.clone(),
                reporting_player: 100,
                winner: Some(100),
                demo_hash: Some("abc".into()),
            },
            &store,
            &store,
        )
        .await
        .unwrap();
    let _ = first; // not final

    // Second report — Bob agrees Alice won
    let outcome = mgr
        .submit_report(
            MatchReport {
                match_token: m.match_token.clone(),
                reporting_player: 200,
                winner: Some(100),
                demo_hash: Some("abc".into()),
            },
            &store,
            &store,
        )
        .await
        .unwrap();

    match outcome {
        lobby_core::types::MatchOutcome::Win { mu_change } => assert!(mu_change > 0.0),
        _ => panic!("expected Win, got {outcome:?}"),
    }
}

#[tokio::test]
async fn dispute_on_winner_mismatch() {
    let store = MockStore::new();
    let token = "dispute-test".to_string();
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "ranked_1v1".into(),
            status: MatchStatus::Reporting,
            created_at: Utc::now(),
            accepted_at: Some(Utc::now()),
            started_at: Some(Utc::now()),
            ended_at: Some(Utc::now()),
            accepted_a: true,
            accepted_b: true,
            connected_a: true,
            connected_b: true,
        })
        .await
        .unwrap();

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks, 30, 300);

    mgr.submit_report(
        MatchReport {
            match_token: token.clone(),
            reporting_player: 100,
            winner: Some(100),
            demo_hash: Some("h1".into()),
        },
        &store,
        &store,
    )
    .await
    .unwrap();
    let outcome = mgr
        .submit_report(
            MatchReport {
                match_token: token.clone(),
                reporting_player: 200,
                winner: Some(200),
                demo_hash: Some("h2".into()),
            },
            &store,
            &store,
        )
        .await
        .unwrap();

    assert!(matches!(outcome, lobby_core::types::MatchOutcome::Disputed));
}

#[tokio::test]
async fn auto_loss_on_timeout() {
    let store = MockStore::new();
    let token = "timeout-test".to_string();
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "ranked_1v1".into(),
            status: MatchStatus::Reporting,
            created_at: Utc::now(),
            accepted_at: Some(Utc::now()),
            started_at: Some(Utc::now()),
            ended_at: Some(Utc::now() - Duration::seconds(400)),
            accepted_a: true,
            accepted_b: true,
            connected_a: true,
            connected_b: true,
        })
        .await
        .unwrap();

    store
        .submit_report(&MatchReport {
            match_token: token.clone(),
            reporting_player: 100,
            winner: Some(100),
            demo_hash: None,
        })
        .await
        .unwrap();

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks, 30, 300);
    mgr.expire_pending_reports(&store, &store).await.unwrap();

    let m = store.get_match(&token).await.unwrap().unwrap();
    assert_eq!(m.status, MatchStatus::Resolved);
    // The lone report's winner (100) gets the win: their mu must have increased,
    // and the match must be Resolved (the auto-loss the audit found missing).
    let rating_100 = store.ratings.lock().get(&(100, "ranked_1v1".into())).cloned().unwrap();
    let rating_200 = store.ratings.lock().get(&(200, "ranked_1v1".into())).cloned().unwrap();
    assert!(rating_100.mu > 25.0, "winner's mu should increase, got {}", rating_100.mu);
    assert!(rating_200.mu < 25.0, "loser's mu should decrease, got {}", rating_200.mu);
}

#[tokio::test]
async fn winner_must_be_participant() {
    let store = MockStore::new();
    let token = "winner-gate".to_string();
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "ranked_1v1".into(),
            status: MatchStatus::Reporting,
            created_at: Utc::now(),
            accepted_at: Some(Utc::now()),
            started_at: Some(Utc::now()),
            ended_at: Some(Utc::now()),
            accepted_a: true,
            accepted_b: true,
            connected_a: true,
            connected_b: true,
        })
        .await
        .unwrap();

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks, 30, 300);
    let err = mgr
        .submit_report(
            MatchReport {
                match_token: token.clone(),
                reporting_player: 100,
                winner: Some(999), // a third steam_id — not a participant
                demo_hash: None,
            },
            &store,
            &store,
        )
        .await
        .unwrap_err();
    assert!(matches!(err, LobbyError::InvalidReport(_)));

    // Match must be untouched.
    let m = store.get_match(&token).await.unwrap().unwrap();
    assert_eq!(m.status, MatchStatus::Reporting);
}

#[tokio::test]
async fn double_accept_requires_both() {
    let store = MockStore::new();
    let token = "double-accept".to_string();
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "ranked_1v1".into(),
            status: MatchStatus::PendingAccept,
            created_at: Utc::now(),
            accepted_at: None,
            started_at: None,
            ended_at: None,
            accepted_a: false,
            accepted_b: false,
            connected_a: false,
            connected_b: false,
        })
        .await
        .unwrap();

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks, 30, 300);
    // Player A accepts — match must still be PendingAccept.
    mgr.accept_match(&token, 100, &store).await.unwrap();
    // Player A accepts again — a double-accept must NOT advance the match.
    mgr.accept_match(&token, 100, &store).await.unwrap();
    let m = store.get_match(&token).await.unwrap().unwrap();
    assert_eq!(m.status, MatchStatus::PendingAccept);
    // Only once B accepts does the match transition.
    mgr.accept_match(&token, 200, &store).await.unwrap();
    let m = store.get_match(&token).await.unwrap().unwrap();
    assert_eq!(m.status, MatchStatus::InProgress);
}
