mod common;

use chrono::{Duration, Utc};
use common::{queued_player, MockStore, TestCallbacks};
use lobby_core::error::LobbyError;
use lobby_core::traits::{MatchStore, QueueStore};
use lobby_core::types::{
    GameType, MatchDifficulty, MatchEvent, MatchInfo, MatchReport, MatchStatus, PlayerState,
    QueueEntry,
};

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

    // Pairing now lives in lobby-server's `PostgresStore::pair_next_match`
    // (a `FOR UPDATE` transaction — not available against MockStore), so the
    // lifecycle under test starts at the formed match.
    let m = MatchInfo {
        match_token: "lifecycle-token".into(),
        player_a: 100,
        player_a_difficulty: MatchDifficulty::Normal,
        player_b: 200,
        player_b_difficulty: MatchDifficulty::Normal,
        game_mode: "ranked_1v1".into(),
        game_type: GameType::P2p,
        status: MatchStatus::PendingAccept,
        created_at: Utc::now(),
        accepted_at: None,
        started_at: None,
        ended_at: None,
        server_address: None,
        join_token: None,
        result_secret: None,
        accepted_a: false,
        accepted_b: false,
        connected_a: false,
        connected_b: false,
    };
    store.create_match(&m).await.unwrap();
    store
        .record_match_event(&m.match_token, MatchEvent::Paired, None)
        .await
        .unwrap();

    // Accept
    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);
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
            &store,
        )
        .await
        .unwrap();

    match outcome {
        lobby_core::types::MatchOutcome::Win { mu_change } => assert!(mu_change > 0.0),
        _ => panic!("expected Win, got {outcome:?}"),
    }

    // Terminal: both players must be back in the menus so they can re-queue.
    assert_eq!(
        store.players.lock().get(&100).unwrap().state,
        PlayerState::InMenus
    );
    assert_eq!(
        store.players.lock().get(&200).unwrap().state,
        PlayerState::InMenus
    );
}

#[tokio::test]
async fn dispute_on_winner_mismatch() {
    let store = MockStore::new();
    let token = "dispute-test".to_string();
    store.players.lock().insert(100, queued_player(100, 25.0));
    store.players.lock().insert(200, queued_player(200, 25.0));
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "ranked_1v1".into(),
            game_type: GameType::P2p,
            server_address: None,
            join_token: None,
            result_secret: None,
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

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);

    mgr.submit_report(
        MatchReport {
            match_token: token.clone(),
            reporting_player: 100,
            winner: Some(100),
            demo_hash: Some("h1".into()),
        },
        &store,
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
            &store,
        )
        .await
        .unwrap();

    assert!(matches!(outcome, lobby_core::types::MatchOutcome::Disputed));
    // Terminal: both players must be back in the menus so they can re-queue.
    assert_eq!(
        store.players.lock().get(&100).unwrap().state,
        PlayerState::InMenus
    );
    assert_eq!(
        store.players.lock().get(&200).unwrap().state,
        PlayerState::InMenus
    );
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
            game_type: GameType::P2p,
            server_address: None,
            join_token: None,
            result_secret: None,
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

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);
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
            game_type: GameType::P2p,
            server_address: None,
            join_token: None,
            result_secret: None,
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

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);
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

#[tokio::test]
async fn server_result_resolves() {
    let store = MockStore::new();
    let token = "server-resolve".to_string();
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "server_arena".into(),
            game_type: GameType::Server,
            status: MatchStatus::Playing,
            created_at: Utc::now(),
            accepted_at: Some(Utc::now()),
            started_at: Some(Utc::now()),
            ended_at: None,
            server_address: Some("gs:27015".into()),
            join_token: Some("tok".into()),
            result_secret: Some("secret".into()),
            accepted_a: true,
            accepted_b: true,
            connected_a: false,
            connected_b: false,
        })
        .await
        .unwrap();

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);
    let outcome = mgr
        .resolve_from_gameserver(&token, Some(100), &store, &store, &store)
        .await
        .unwrap();
    match outcome {
        lobby_core::types::MatchOutcome::Win { mu_change } => assert!(mu_change > 0.0),
        _ => panic!("expected Win, got {outcome:?}"),
    }
    let m = store.get_match(&token).await.unwrap().unwrap();
    assert_eq!(m.status, MatchStatus::Resolved);
    let rating_100 = store
        .ratings
        .lock()
        .get(&(100, "server_arena".into()))
        .cloned()
        .unwrap();
    assert!(
        rating_100.mu > 25.0,
        "winner's mu should increase, got {}",
        rating_100.mu
    );
}

#[tokio::test]
async fn pong_resolve_declares_winner() {
    let store = MockStore::new();
    let token = "pong-resolve".to_string();
    store.players.lock().insert(100, queued_player(100, 25.0));
    store.players.lock().insert(200, queued_player(200, 25.0));
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "ranked_1v1".into(),
            game_type: GameType::P2p,
            status: MatchStatus::Reporting,
            created_at: Utc::now(),
            accepted_at: Some(Utc::now()),
            started_at: Some(Utc::now()),
            ended_at: None,
            server_address: None,
            join_token: None,
            result_secret: None,
            accepted_a: true,
            accepted_b: true,
            connected_a: true,
            connected_b: true,
        })
        .await
        .unwrap();

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);
    let outcome = mgr
        .resolve_pong(&token, 100, &store, &store, &store)
        .await
        .unwrap();
    match outcome {
        lobby_core::types::MatchOutcome::Win { mu_change } => assert!(mu_change > 0.0),
        _ => panic!("expected Win, got {outcome:?}"),
    }
    let m = store.get_match(&token).await.unwrap().unwrap();
    assert_eq!(m.status, MatchStatus::Resolved);
    let rating_100 = store
        .ratings
        .lock()
        .get(&(100, "ranked_1v1".into()))
        .cloned()
        .unwrap();
    assert!(
        rating_100.mu > 25.0,
        "winner's mu should increase, got {}",
        rating_100.mu
    );
    assert_eq!(
        store.players.lock().get(&100).unwrap().state,
        PlayerState::InMenus,
        "winner reset to InMenus"
    );
    assert_eq!(
        store.players.lock().get(&200).unwrap().state,
        PlayerState::InMenus,
        "loser reset to InMenus"
    );
}

#[tokio::test]
async fn playing_match_expires() {
    let store = MockStore::new();
    let token = "playing-expire".to_string();
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "server_arena".into(),
            game_type: GameType::Server,
            status: MatchStatus::Playing,
            created_at: Utc::now(),
            accepted_at: Some(Utc::now()),
            started_at: Some(Utc::now() - Duration::seconds(2)),
            ended_at: None,
            server_address: Some("gs:27015".into()),
            join_token: Some("tok".into()),
            result_secret: Some("secret".into()),
            accepted_a: true,
            accepted_b: true,
            connected_a: false,
            connected_b: false,
        })
        .await
        .unwrap();

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);
    let expired = mgr.expire_playing_matches(&store, 1, &store).await.unwrap();
    assert_eq!(expired, vec![token.clone()]);
    let m = store.get_match(&token).await.unwrap().unwrap();
    assert_eq!(m.status, MatchStatus::Disputed);
}

#[tokio::test]
async fn duplicate_report_rejected() {
    let store = MockStore::new();
    let token = "dup-report".to_string();
    store.players.lock().insert(100, queued_player(100, 25.0));
    store.players.lock().insert(200, queued_player(200, 25.0));
    store
        .create_match(&MatchInfo {
            match_token: token.clone(),
            player_a: 100,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 200,
            player_b_difficulty: MatchDifficulty::Normal,
            game_mode: "ranked_1v1".into(),
            game_type: GameType::P2p,
            server_address: None,
            join_token: None,
            result_secret: None,
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

    let mgr = lobby_core::match_lifecycle::MatchManager::new(TestCallbacks);
    let report_a = MatchReport {
        match_token: token.clone(),
        reporting_player: 100,
        winner: Some(100),
        demo_hash: Some("demo-a".into()),
    };
    let report_b = MatchReport {
        match_token: token.clone(),
        reporting_player: 200,
        winner: Some(100),
        demo_hash: Some("demo-a".into()),
    };

    mgr.submit_report(report_a.clone(), &store, &store, &store)
        .await
        .unwrap();

    // A second report from the same player must be rejected, not stored.
    let err = mgr
        .submit_report(report_a, &store, &store, &store)
        .await
        .unwrap_err();
    assert!(matches!(err, LobbyError::InvalidReport(_)));
    assert_eq!(store.get_reports(&token).await.unwrap().len(), 1);

    // The opponent's report still resolves the match.
    let outcome = mgr
        .submit_report(report_b, &store, &store, &store)
        .await
        .unwrap();
    assert!(matches!(
        outcome,
        lobby_core::types::MatchOutcome::Win { .. }
    ));
    assert_eq!(
        store.players.lock().get(&100).unwrap().state,
        PlayerState::InMenus
    );
    assert_eq!(
        store.players.lock().get(&200).unwrap().state,
        PlayerState::InMenus
    );
}
