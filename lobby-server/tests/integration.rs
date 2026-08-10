use std::sync::Arc;
use std::time::Duration;

use axum::{Json, Router, http::StatusCode, routing::post};
use lobby_client::{LobbyClient, ServerEvent};
use sqlx::PgPool;
use tokio::sync::Mutex;
use tokio::time::timeout;

mod common; // lobby-server/tests/common.rs — TestHarness + setup()
use common::{
    setup, setup_temporal, setup_temporal_pong, setup_temporal_pong_countdown,
    setup_temporal_pong_start_timeout, setup_temporal_with_auth, setup_temporal_with_creator,
};
use lobby_server::auth_providers::{ProviderConfig, ProviderKind};

/// Poll `query` (a fresh sqlx query for `token`) until it returns a row or 5s elapses.
/// The server processes WS reports asynchronously, so a straight fetch could race.
async fn wait_for_row(pool: &PgPool, token: &str, query: &'static str) -> Option<(String, f64)> {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let row = sqlx::query_as::<_, (String, f64)>(query)
            .bind(token)
            .fetch_optional(pool)
            .await
            .expect("query match_results");
        if let Some(row) = row {
            return Some(row);
        }
        if std::time::Instant::now() >= deadline {
            return None;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll a scalar column until it equals `expected` or 5s elapses.
async fn wait_for_status(pool: &PgPool, token: &str, expected: &str) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM matches WHERE match_token = $1")
                .bind(token)
                .fetch_optional(pool)
                .await
                .expect("query matches");
        if status.as_deref() == Some(expected) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Like `wait_for_status` but with a caller-supplied deadline (seconds).
/// The pong tests need up to 30s: 3 points take ~2s at base speed plus tick
/// latency, far past `wait_for_status`'s 5s window.
async fn wait_for_status_for(pool: &PgPool, token: &str, expected: &str, secs: u64) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM matches WHERE match_token = $1")
                .bind(token)
                .fetch_optional(pool)
                .await
                .expect("query matches");
        if status.as_deref() == Some(expected) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `match_events` until every expected (event_type, count) lands, or 5s
/// elapses. The server writes match status and event rows as separate
/// statements (status first, events after), so a single-shot count read can
/// race the accept/decline event inserts and see a partial set.
async fn wait_for_event_counts(pool: &PgPool, token: &str, expected: &[(&str, i64)]) -> bool {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let counts: Vec<(String, i64)> = sqlx::query_as(
            "SELECT event_type, COUNT(*) FROM match_events WHERE match_token = $1 GROUP BY event_type",
        )
        .bind(token)
        .fetch_all(pool)
        .await
        .expect("query match_events");
        let map: std::collections::HashMap<String, i64> = counts.into_iter().collect();
        if expected.iter().all(|(k, v)| map.get(*k) == Some(v)) {
            return true;
        }
        if std::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Connect two clients, auth with distinct test tokens, queue, and get a shared match.
/// Returns the clients, the shared match token, and both players' player_ids
/// (UUIDs from `AuthOk` — the report winner / DB identity key).
async fn pair_up(
    h: &common::TestHarness,
    p1_id: u64,
    p2_id: u64,
    mode: &str,
) -> (LobbyClient, LobbyClient, String, String, String) {
    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let a1 = p1.authenticate_test_token(p1_id, &h.base_url).await.unwrap();
    let a2 = p2.authenticate_test_token(p2_id, &h.base_url).await.unwrap();

    p1.begin_matchmaking(mode, "normal").await.unwrap();
    p2.begin_matchmaking(mode, "normal").await.unwrap();

    // Ticker runs every 2s; wait up to 15s.
    let m1 = timeout(Duration::from_secs(15), p1.wait_for_match())
        .await
        .expect("p1 match within 15s")
        .unwrap()
        .unwrap();
    let m2 = timeout(Duration::from_secs(15), p2.wait_for_match())
        .await
        .expect("p2 match within 15s")
        .unwrap()
        .unwrap();
    assert_eq!(
        m1.match_token, m2.match_token,
        "both players must get the same match"
    );
    (p1, p2, m1.match_token, a1.player_id, a2.player_id)
}

/// Both clients accept, then both signal START, synchronizing on server
/// state between stages. The client messages are fire-and-forget: a start_match
/// arriving before the match is InProgress is rejected by the server (state
/// mismatch), so we poll the DB before each stage like real clients would wait
/// for their own coordination.
async fn accept_and_connect(
    h: &common::TestHarness,
    p1: &mut LobbyClient,
    p2: &mut LobbyClient,
    token: &str,
) {
    p1.accept_match(token).await.unwrap();
    p2.accept_match(token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, token, "InProgress").await,
        "both accepts must transition the match to InProgress"
    );

    p1.start_match(token).await.unwrap();
    p2.start_match(token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, token, "Reporting").await,
        "both connections must transition the match to Reporting"
    );
}

#[sqlx::test]
async fn full_match_lifecycle(pool: sqlx::PgPool) {
    let h = setup_temporal_pong(pool).await;
    let (mut p1, mut p2, token, pid1, _pid2) = pair_up(&h, 100, 200, "ranked_1v1").await;

    accept_and_connect(&h, &mut p1, &mut p2, &token).await;

    p1.submit_report(&token, Some(&pid1), Some("demo-a"))
        .await
        .unwrap();
    p2.submit_report(&token, Some(&pid1), Some("demo-a"))
        .await
        .unwrap();

    // Verify the match resolved AND a match_results row was written.
    // The stored outcome is from player_a's perspective; the queue pairing
    // order is racy, so learn which side p1 (100) landed on before asserting.
    let row = wait_for_row(
        &h.pool,
        &token,
        "SELECT outcome, mu_change_a FROM match_results WHERE match_token = $1",
    )
    .await
    .expect("match_results row exists");
    let player_a: uuid::Uuid =
        sqlx::query_scalar("SELECT player_a FROM matches WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let expected = if player_a.to_string() == pid1 { "Win" } else { "Loss" };
    assert_eq!(row.0, expected, "outcome must match player_a's perspective");
    // The winner's mu increases regardless of which side they landed on.
    let winner_mu: f64 = sqlx::query_scalar(
        "SELECT mu FROM ratings WHERE user_id = $1::uuid AND game_mode = 'ranked_1v1'",
    )
    .bind(&pid1)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert!(
        winner_mu > 25.0,
        "winner's mu should increase, got {winner_mu}"
    );

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn dispute_on_winner_mismatch(pool: sqlx::PgPool) {
    let h = setup_temporal_pong(pool).await;
    let (mut p1, mut p2, token, pid1, pid2) = pair_up(&h, 100, 200, "ranked_1v1").await;

    accept_and_connect(&h, &mut p1, &mut p2, &token).await;

    // p1 claims p1 won; p2 claims p2 won — a dispute.
    p1.submit_report(&token, Some(&pid1), Some("demo-a"))
        .await
        .unwrap();
    p2.submit_report(&token, Some(&pid2), Some("demo-b"))
        .await
        .unwrap();

    // After both reports are in, the !agree branch sets status=Disputed.
    assert!(
        wait_for_status(&h.pool, &token, "Disputed").await,
        "match must be Disputed"
    );

    let result_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM match_results WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(result_count, 0, "no outcome recorded for a disputed match");

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn queue_cancel(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    p1.authenticate_test_token(300, &h.base_url).await.unwrap();

    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
    p1.cancel_matchmaking().await.unwrap();

    // No partner queued, so no MatchFound should ever arrive. A stale
    // queue_status pushed by the ticker can land just after the cancel, so
    // drain non-match events instead of expecting an empty channel.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut saw_match = false;
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::MatchFound { .. }))) => {
                saw_match = true;
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(_))) | Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(!saw_match, "cancelled player must not receive a MatchFound");

    drop(p1);
}

#[sqlx::test]
async fn queue_stats_received(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = p1.authenticate_test_token(901, &h.base_url).await.unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // The ticker pushes queue_status every ~2s while the player is queued.
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    let mut got = false;
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::QueueStatus {
                my_mu,
                queue_size,
                leaderboard,
                ..
            }))) => {
                assert!(
                    my_mu > 24.0 && my_mu < 26.0,
                    "fresh player should sit at the default mu 25.0, got {my_mu}"
                );
                assert!(queue_size >= 1, "queue must contain the queued player");
                assert!(
                    leaderboard.iter().any(|e| e.player_id == auth.player_id),
                    "leaderboard must include the queued player"
                );
                got = true;
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => panic!("client error while queueing: {e}"),
            Ok(None) => panic!("connection closed while queueing"),
            Err(_) => continue,
        }
    }
    assert!(got, "queue_status must arrive within 6s");

    p1.cancel_matchmaking().await.unwrap();
    drop(p1);
}

#[sqlx::test]
async fn p2p_and_report_visibility(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let (mut p1, mut p2, token, pid1, _pid2) = pair_up(&h, 100, 200, "ranked_1v1").await;
    accept_and_connect(&h, &mut p1, &mut p2, &token).await;

    // accept_and_connect sends both start_match signals; p1 must learn that
    // the opponent started once p2's signal is processed.
    let mut saw_opponent = false;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::OpponentConnected { .. }))) => {
                saw_opponent = true;
                break;
            }
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
        }
    }
    assert!(
        saw_opponent,
        "p1 must learn that the opponent P2P-connected"
    );

    // p1 reports first. The workflow resolves only when BOTH who_won signals
    // are in, so the match must NOT resolve yet (no premature match_result).
    p1.submit_report(&token, Some(&pid1), Some("demo-a"))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    let status: String = sqlx::query_scalar("SELECT status FROM matches WHERE match_token = $1")
        .bind(&token)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(
        status, "Reporting",
        "one report alone must not resolve the match"
    );

    // p2 agrees; the match resolves and BOTH players receive match_result.
    p2.submit_report(&token, Some(&pid1), Some("demo-a"))
        .await
        .unwrap();

    for (name, client) in [("p1", &mut p1), ("p2", &mut p2)] {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        let mut saw_result = false;
        while std::time::Instant::now() < deadline {
            match timeout(Duration::from_secs(2), client.next_event()).await {
                Ok(Some(Ok(lobby_client::ServerEvent::MatchResult { .. }))) => {
                    saw_result = true;
                    break;
                }
                Ok(Some(Ok(_))) | Err(_) => continue,
                Ok(Some(Err(e))) => panic!("{name} client error: {e}"),
                Ok(None) => break,
            }
        }
        assert!(
            saw_result,
            "{name} must receive match_result after resolution"
        );
    }

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn decline_notifies_opponent(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let (mut p1, mut p2, token, _pid1, _pid2) = pair_up(&h, 100, 200, "ranked_1v1").await;

    p1.decline_match(&token).await.unwrap();

    // The opponent must learn the match was declined…
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut p2_told = false;
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p2.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::MatchDeclined { match_token }))) => {
                assert_eq!(match_token, token);
                p2_told = true;
                break;
            }
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
        }
    }
    assert!(p2_told, "opponent must receive match_declined");

    // …and the decliner's own UI resets on the same ack.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let mut p1_acked = false;
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::MatchDeclined { .. }))) => {
                p1_acked = true;
                break;
            }
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
        }
    }
    assert!(p1_acked, "decliner must receive its own match_declined ack");

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn match_events_logged(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    // Scenario 1: pairing + both accepts are logged.
    let (mut p1, mut p2, token, _pid1, _pid2) = pair_up(&h, 400, 401, "ranked_1v1").await;
    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "accepts must land"
    );
    assert!(wait_for_event_counts(&h.pool, &token, &[("paired", 1), ("accepted", 2)]).await,);
    drop(p1);
    drop(p2);

    // Scenario 2: decline is logged and the match is immediately terminal.
    let (mut q1, _q2, token2, q1_pid, _q2_pid) = pair_up(&h, 402, 403, "ranked_1v1").await;
    q1.decline_match(&token2).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token2, "Disputed").await,
        "declined match must be Disputed immediately (no 30s linger)"
    );
    assert!(
        wait_for_event_counts(&h.pool, &token2, &[("paired", 1), ("declined", 1)]).await,
        "pairing + decline events must be logged"
    );
    let actor: Option<String> = sqlx::query_scalar(
        "SELECT user_id::text FROM match_events WHERE match_token = $1 AND event_type = 'declined'",
    )
    .bind(&token2)
    .fetch_optional(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        actor.as_deref(),
        Some(q1_pid.as_str()),
        "decline event records the declining player"
    );
    drop(q1);
}

#[sqlx::test]
async fn auth_ok_reports_state(pool: sqlx::PgPool) {
    use lobby_core::types::PlayerState;

    let h = setup_temporal(pool).await;

    let mut c1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = c1.authenticate_test_token(601, &h.base_url).await.unwrap();
    assert_eq!(
        auth.state,
        PlayerState::InMenus,
        "fresh player starts in menus"
    );

    c1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // The queue signal is async (workflow round-trip): wait for the state to
    // land before asserting the reconnect view.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM player_state WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if state.as_deref() == Some("Queueing") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "player never reached Queueing, got {:?}",
            state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // A second connection for the same player reports the persisted state.
    let mut c2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth2 = c2.authenticate_test_token(601, &h.base_url).await.unwrap();
    assert_eq!(
        auth2.state,
        PlayerState::Queueing,
        "reconnecting while queueing must report queueing"
    );

    drop(c1);
    drop(c2);
}

#[sqlx::test]
async fn reconnect_reports_queueing(pool: sqlx::PgPool) {
    use lobby_core::types::PlayerState;

    let h = setup_temporal(pool).await;

    let mut c1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth1 = c1.authenticate_test_token(602, &h.base_url).await.unwrap();
    c1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // begin_matchmaking is fire-and-forget: wait for the entry to land before
    // dropping the connection, so the drop happens while the player is queued.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth1.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never appeared for the queued player"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Dropping the connection must NOT reset the player: the queue entry is
    // still alive, so a reconnect within the stale window stays queued.
    drop(c1);

    let mut c2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = c2.authenticate_test_token(602, &h.base_url).await.unwrap();
    assert_eq!(
        auth.state,
        PlayerState::Queueing,
        "close-then-reconnect while queued must report queueing"
    );

    drop(c2);
}

#[sqlx::test]
async fn queue_expired_notifies_player(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = p1.authenticate_test_token(701, &h.base_url).await.unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // Pretend the player idled past the 30s stale window; the next tick
    // removes the entry and must tell the still-connected client.

    // begin_matchmaking is fire-and-forget: wait for the server to actually
    // enqueue the player before backdating, or the UPDATE races the INSERT.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never appeared for the queued player"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sqlx::query("UPDATE player_state SET last_heartbeat = NOW() - INTERVAL '40 seconds' WHERE user_id = $1::uuid")
        .bind(&auth.player_id)
        .execute(&h.pool)
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    let mut told = false;
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::QueueExpired))) => {
                told = true;
                break;
            }
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
        }
    }
    assert!(told, "queued player must be told when its entry expires");

    drop(p1);
}

#[sqlx::test]
async fn stale_entry_resets_player_state(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = p1.authenticate_test_token(703, &h.base_url).await.unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // Wait for the entry, then pretend the player idled past the 30s stale
    // window. The next tick evicts the entry AND must reset the owner to the
    // menus so a reconnect reports "in_menus", not a stale "queueing".
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never appeared for the queued player"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sqlx::query("UPDATE player_state SET last_heartbeat = NOW() - INTERVAL '40 seconds' WHERE user_id = $1::uuid")
        .bind(&auth.player_id)
        .execute(&h.pool)
        .await
        .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    loop {
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM player_state WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if state.as_deref() == Some("InMenus") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "evicted player must be reset to the menus, got {:?}",
            state
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(p1);
}

#[sqlx::test]
async fn heartbeat_keeps_queued_alive(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = p1.authenticate_test_token(702, &h.base_url).await.unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // Wait for the server to enqueue the player (fire-and-forget message).
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never appeared for the queued player"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // The player's heartbeat is 40s old: without heartbeats the next tick
    // would drop the entry (see queue_expired_notifies_player). Keep the
    // client heartbeating across several 2s ticker cycles instead.
    sqlx::query("UPDATE player_state SET last_heartbeat = NOW() - INTERVAL '40 seconds' WHERE user_id = $1::uuid")
        .bind(&auth.player_id)
        .execute(&h.pool)
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(8);
    while std::time::Instant::now() < deadline {
        p1.heartbeat().await.unwrap();
        tokio::time::sleep(Duration::from_millis(1200)).await;
    }

    // One more full tick without a heartbeat (it ages to ~4s) — still well
    // inside the 30s window, so the entry must have survived cleanup.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let still: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
            .bind(&auth.player_id)
            .fetch_optional(&h.pool)
            .await
            .unwrap();
    assert!(still.is_some(), "heartbeating player must stay queued");

    // And the client must never have been told its entry expired.
    let deadline = std::time::Instant::now() + Duration::from_secs(3);
    let mut expired = false;
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::QueueExpired))) => {
                expired = true;
                break;
            }
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
        }
    }
    assert!(
        !expired,
        "heartbeating player must not be told queue_expired"
    );

    drop(p1);
}

#[sqlx::test]
async fn openid_return_to_validation(pool: sqlx::PgPool) {
    let h = setup(pool).await;

    // No-redirect client so the redirect (307) isn't followed away by reqwest.
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let base = &h.base_url;

    // Same-origin relative return_to: accepted, redirect to Steam.
    let status = client
        .get(format!("{base}/auth/steam/login?return_to=/dashboard"))
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::TEMPORARY_REDIRECT);

    // Absolute foreign origin and protocol-relative URL: rejected with 400.
    for evil in ["https://evil.com", "//evil.com"] {
        let status = client
            .get(format!("{base}/auth/steam/login?return_to={evil}"))
            .send()
            .await
            .unwrap()
            .status();
        assert_eq!(status, reqwest::StatusCode::BAD_REQUEST, "return_to={evil}");
    }
}

#[sqlx::test]
async fn rate_limited_test_token(pool: sqlx::PgPool) {
    let h = setup(pool).await;
    let client = reqwest::Client::new();
    let (mut ok, mut limited) = (0, 0);
    for i in 0..25u64 {
        let status = client
            .post(format!("{}/auth/test-token", h.base_url))
            .json(&serde_json::json!({"steam_id": i + 5000}))
            .send()
            .await
            .unwrap()
            .status();
        match status.as_u16() {
            200 => ok += 1,
            429 => limited += 1,
            other => panic!("unexpected status {other}"),
        }
    }
    assert_eq!(ok, 20, "first 20 test-token calls per IP succeed");
    assert_eq!(limited, 5, "the rest are rate-limited");
}

#[sqlx::test]
async fn logout_revokes_token(pool: sqlx::PgPool) {
    let h = setup(pool).await;
    let client = reqwest::Client::new();

    // Mint a raw token so we can present it to both /auth/logout and the WS.
    let resp = client
        .post(format!("{}/auth/test-token", h.base_url))
        .json(&serde_json::json!({"steam_id": 400}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = resp.json().await.unwrap();
    let token = body["token"].as_str().unwrap().to_string();

    // Token works over WS before logout.
    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    p1.authenticate(&token).await.unwrap();
    drop(p1);

    // Logout bumps token_version.
    let resp = client
        .post(format!("{}/auth/logout", h.base_url))
        .header("Authorization", format!("Bearer {token}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NO_CONTENT);

    // The same token is now rejected on a fresh connection.
    let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    assert!(
        p2.authenticate(&token).await.is_err(),
        "a revoked token must be rejected"
    );
}

#[sqlx::test]
async fn ws_frame_size_limit(pool: sqlx::PgPool) {
    use futures_util::SinkExt;
    use futures_util::StreamExt;

    let h = setup(pool).await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&h.ws_url).await.unwrap();

    // First message: a ~2 MiB text frame — the 64 KiB cap must reject it.
    let big = "x".repeat(2 * 1024 * 1024);
    ws.send(tokio_tungstenite::tungstenite::Message::Text(big.into()))
        .await
        .unwrap();

    // The server may send an error frame (e.g. auth_required) before closing.
    let outcome: Option<String>;
    loop {
        match timeout(Duration::from_secs(5), ws.next()).await {
            Ok(Some(Ok(tokio_tungstenite::tungstenite::Message::Close(frame)))) => {
                outcome = Some(format!(
                    "close:{}",
                    frame.map(|f| u16::from(f.code)).unwrap_or(0)
                ));
                break;
            }
            Ok(Some(Ok(_))) => continue,
            Ok(Some(Err(e))) => {
                outcome = Some(format!("err:{e}"));
                break;
            }
            other => panic!("oversized frame must close the connection, got {other:?}"),
        }
    }
    let outcome = outcome.expect("connection must close or error");
    assert!(
        outcome.starts_with("close:") || outcome.starts_with("err:"),
        "unexpected outcome {outcome}"
    );
    // With the 64 KiB cap the oversized frame is rejected; without it the
    // server would wait for a valid auth message instead.
    assert!(
        outcome != "close:0",
        "server must close with an error code, got {outcome}"
    );
}

#[sqlx::test]
async fn replaced_connection_keeps_new(pool: sqlx::PgPool) {
    let h = setup(pool).await;

    let mut first = LobbyClient::connect(&h.ws_url).await.unwrap();
    first
        .authenticate_test_token(500, &h.base_url)
        .await
        .unwrap();

    // Second connection for the same steam_id replaces the first.
    let mut second = LobbyClient::connect(&h.ws_url).await.unwrap();
    second
        .authenticate_test_token(500, &h.base_url)
        .await
        .unwrap();

    // The first connection must be closed (possibly after a "replaced" notice).
    let mut closed = false;
    for _ in 0..5 {
        match timeout(Duration::from_secs(5), first.next_event()).await {
            Ok(None) => {
                closed = true;
                break;
            }
            Ok(Some(Ok(lobby_client::ServerEvent::Error { .. }))) => continue,
            Ok(Some(Err(_))) => {
                closed = true;
                break;
            }
            other => panic!("unexpected event from replaced connection: {other:?}"),
        }
    }
    assert!(closed, "first connection must be closed after replacement");

    // The second connection stays functional.
    second
        .begin_matchmaking("ranked_1v1", "normal")
        .await
        .unwrap();
    drop(first);
    drop(second);
}

#[sqlx::test]
async fn ws_origin_restriction(pool: sqlx::PgPool) {
    let h = setup(pool).await;
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let key = "dGhlIHNhbXBsZSBub25jZQ==";

    // Non-allowlisted browser origin → 403 before the upgrade.
    let status = client
        .get(format!("{}/ws", h.base_url))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Key", key)
        .header("Sec-WebSocket-Version", "13")
        .header("Origin", "https://evil.com")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::FORBIDDEN);

    // Allowlisted origin → 101 Switching Protocols.
    let status = client
        .get(format!("{}/ws", h.base_url))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Key", key)
        .header("Sec-WebSocket-Version", "13")
        .header("Origin", "https://lobby.example.com")
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::SWITCHING_PROTOCOLS);

    // Same origin (the host that served the page) → 101.
    let status = client
        .get(format!("{}/ws", h.base_url))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Key", key)
        .header("Sec-WebSocket-Version", "13")
        .header("Origin", h.base_url.clone())
        .send()
        .await
        .unwrap()
        .status();
    assert_eq!(status, reqwest::StatusCode::SWITCHING_PROTOCOLS);
}

// ── Server-authoritative game helpers ──────────────────────────────────────

/// Spawn a mock gameserver creator (axum app on a random port) and return
/// (allocate_url, recorded_request_bodies, callback_base_slot). `/allocate`
/// records each request body and replies `{ server_address, join_token }`.
/// When `report` is true it also auto-posts the result (player_a's win) to the
/// callback URL 300ms after allocation, exercising the full webhook path.
///
/// The callback URL from the coordinator points at the configured `PUBLIC_URL`
/// (an unreachable fake in tests), so the test fills `callback_base_slot` with
/// the harness's real `base_url` and the mock re-targets the callback there.
async fn spawn_mock_creator(
    report: bool,
) -> (
    String,
    Arc<Mutex<Vec<serde_json::Value>>>,
    Arc<Mutex<Option<String>>>,
) {
    let recorded: Arc<Mutex<Vec<serde_json::Value>>> = Arc::new(Mutex::new(Vec::new()));
    let callback_base: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let recorded_route = recorded.clone();
    let base_route = callback_base.clone();
    let app = Router::new().route(
        "/allocate",
        post(move |Json(body): Json<serde_json::Value>| {
            let rec = recorded_route.clone();
            let base = base_route.clone();
            async move {
                rec.lock().await.push(body.clone());
                if report {
                    let cb = body["result_callback_url"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string();
                    let winner = body["player_a"]
                        .as_str()
                        .map(|s| s.to_string())
                        .unwrap_or_default();
                    tokio::spawn(async move {
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                        if let Some(b) = base.lock().await.clone()
                            && let Ok(u) = url::Url::parse(&cb)
                        {
                            let target = format!("{}{}", b.trim_end_matches('/'), u.path());
                            let client = reqwest::Client::new();
                            let _ = client
                                .post(&target)
                                .json(&serde_json::json!({ "winner": winner }))
                                .send()
                                .await;
                        }
                    });
                }
                Json(serde_json::json!({ "server_address": "mock-gs:27015", "join_token": "tok" }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{addr}/allocate"), recorded, callback_base)
}

/// Spawn a mock creator that always returns 500 (allocation can never succeed).
async fn spawn_failing_mock_creator() -> String {
    let app = Router::new().route(
        "/allocate",
        post(|| async { StatusCode::INTERNAL_SERVER_ERROR }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    format!("http://{addr}/allocate")
}

/// Consume events until `pred` matches, skipping everything else (mirror the
/// `queue_expired_notifies_player` loop). Panics with the events seen if
/// nothing matches by deadline.
async fn wait_for_event<E>(
    label: &str,
    p: &mut LobbyClient,
    deadline: std::time::Instant,
    mut pred: impl FnMut(ServerEvent) -> Option<E>,
) -> E {
    let mut seen: Vec<String> = Vec::new();
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p.next_event()).await {
            Ok(Some(Ok(ev))) => {
                seen.push(format!("{ev:?}"));
                if let Some(out) = pred(ev) {
                    return out;
                }
            }
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    panic!("{label}: event not received within deadline; saw: {seen:?}");
}

#[sqlx::test]
async fn server_game_full_lifecycle(pool: sqlx::PgPool) {
    let (mock_url, recorded, base_slot) = spawn_mock_creator(true).await;
    let h = setup_temporal_with_creator(pool, Some(&mock_url)).await;
    *base_slot.lock().await = Some(h.base_url.clone());
    let (mut p1, mut p2, token, _pid1, _pid2) = pair_up(&h, 300, 301, "server_arena").await;

    // Accept only — server matches reject start_match (mark_connected guard).
    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "both accepts must transition the match to InProgress"
    );

    // The ticker allocates ~2s after the match is InProgress.
    let _ = wait_for_event(
        "gs-ready p1",
        &mut p1,
        std::time::Instant::now() + Duration::from_secs(15),
        |ev| match ev {
            ServerEvent::GameServerReady { address, .. } if address == "mock-gs:27015" => Some(()),
            _ => None,
        },
    )
    .await;
    let _ = wait_for_event(
        "gs-ready p2",
        &mut p2,
        std::time::Instant::now() + Duration::from_secs(15),
        |ev| match ev {
            ServerEvent::GameServerReady { address, .. } if address == "mock-gs:27015" => Some(()),
            _ => None,
        },
    )
    .await;

    // The creator received the match and a callback URL carrying token + secret.
    let alloc_body = recorded
        .lock()
        .await
        .iter()
        .find(|b| b["match_token"].as_str() == Some(token.as_str()))
        .expect("allocate request recorded for the match")
        .clone();
    assert_eq!(alloc_body["game_mode"].as_str(), Some("server_arena"));
    let cb = alloc_body["result_callback_url"]
        .as_str()
        .unwrap_or_default();
    assert!(
        cb.contains(&format!("/internal/game-result/{token}/")),
        "callback URL must carry token + secret, got {cb}"
    );
    assert!(
        !cb.ends_with('/'),
        "callback URL must have a non-empty secret, got {cb}"
    );

    // The mock auto-reports player_a's win; both players get the same outcome.
    let outcome1 = wait_for_event(
        "match-result p1",
        &mut p1,
        std::time::Instant::now() + Duration::from_secs(15),
        |ev| match ev {
            ServerEvent::MatchResult { outcome, .. } => Some(outcome),
            _ => None,
        },
    )
    .await;
    let outcome2 = wait_for_event(
        "match-result p2",
        &mut p2,
        std::time::Instant::now() + Duration::from_secs(15),
        |ev| match ev {
            ServerEvent::MatchResult { outcome, .. } => Some(outcome),
            _ => None,
        },
    )
    .await;
    assert!(
        outcome1.get("Win").is_some() && outcome2.get("Win").is_some(),
        "both players must receive Win (player_a's perspective), got {outcome1} / {outcome2}"
    );

    assert!(
        wait_for_status(&h.pool, &token, "Resolved").await,
        "match must end Resolved"
    );
    let row = wait_for_row(
        &h.pool,
        &token,
        "SELECT outcome, mu_change_a FROM match_results WHERE match_token = $1",
    )
    .await
    .expect("match_results row exists");
    assert_eq!(row.0, "Win", "stored outcome must be Win");
    assert!(
        row.1 > 0.0,
        "winner's mu change must be positive, got {}",
        row.1
    );
    let server_address: Option<String> =
        sqlx::query_scalar("SELECT server_address FROM matches WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(server_address.as_deref(), Some("mock-gs:27015"));

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn server_game_alloc_timeout(pool: sqlx::PgPool) {
    let mock_url = spawn_failing_mock_creator().await;
    let h = setup_temporal_with_creator(pool, Some(&mock_url)).await;
    let (mut p1, mut p2, token, _pid1, _pid2) = pair_up(&h, 300, 301, "server_arena").await;

    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "accepts must land"
    );

    // Backdate past the 60s allocation timeout; the next tick disputes it.
    sqlx::query(
        "UPDATE matches SET accepted_at = NOW() - INTERVAL '90 seconds' WHERE match_token = $1",
    )
    .bind(&token)
    .execute(&h.pool)
    .await
    .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let err1 = wait_for_event("gs-error p1", &mut p1, deadline, |ev| match ev {
        ServerEvent::GameServerError { message, .. } => Some(message),
        _ => None,
    })
    .await;
    assert!(
        err1.contains("timed out"),
        "unexpected error message: {err1}"
    );
    let _ = wait_for_event("gs-error p2", &mut p2, deadline, |ev| match ev {
        ServerEvent::GameServerError { .. } => Some(()),
        _ => None,
    })
    .await;
    let _ = wait_for_event("disputed-result p1", &mut p1, deadline, |ev| match ev {
        ServerEvent::MatchResult { outcome, .. } if outcome.as_str() == Some("Disputed") => {
            Some(())
        }
        _ => None,
    })
    .await;
    let _ = wait_for_event("disputed-result p2", &mut p2, deadline, |ev| match ev {
        ServerEvent::MatchResult { outcome, .. } if outcome.as_str() == Some("Disputed") => {
            Some(())
        }
        _ => None,
    })
    .await;
    assert!(
        wait_for_status(&h.pool, &token, "Disputed").await,
        "match must be Disputed"
    );

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn server_game_result_timeout(pool: sqlx::PgPool) {
    let (mock_url, _recorded, _base_slot) = spawn_mock_creator(false).await;
    let h = setup_temporal_with_creator(pool, Some(&mock_url)).await;
    let (mut p1, mut p2, token, _pid1, _pid2) = pair_up(&h, 300, 301, "server_arena").await;

    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "accepts must land"
    );

    // Allocation succeeds; wait for server-ready so started_at is set.
    let _ = wait_for_event(
        "gs-ready p1",
        &mut p1,
        std::time::Instant::now() + Duration::from_secs(15),
        |ev| match ev {
            ServerEvent::GameServerReady { .. } => Some(()),
            _ => None,
        },
    )
    .await;
    let _ = wait_for_event(
        "gs-ready p2",
        &mut p2,
        std::time::Instant::now() + Duration::from_secs(15),
        |ev| match ev {
            ServerEvent::GameServerReady { .. } => Some(()),
            _ => None,
        },
    )
    .await;

    // Backdate past the 300s result timeout; the next tick disputes it.
    sqlx::query(
        "UPDATE matches SET started_at = NOW() - INTERVAL '320 seconds' WHERE match_token = $1",
    )
    .bind(&token)
    .execute(&h.pool)
    .await
    .unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let _ = wait_for_event("disputed-result p1", &mut p1, deadline, |ev| match ev {
        ServerEvent::MatchResult { outcome, .. } if outcome.as_str() == Some("Disputed") => {
            Some(())
        }
        _ => None,
    })
    .await;
    let _ = wait_for_event("disputed-result p2", &mut p2, deadline, |ev| match ev {
        ServerEvent::MatchResult { outcome, .. } if outcome.as_str() == Some("Disputed") => {
            Some(())
        }
        _ => None,
    })
    .await;
    assert!(
        wait_for_status(&h.pool, &token, "Disputed").await,
        "match must be Disputed"
    );

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn game_result_callback_security(pool: sqlx::PgPool) {
    let (mock_url, _recorded, base_slot) = spawn_mock_creator(true).await;
    let h = setup_temporal_with_creator(pool, Some(&mock_url)).await;
    *base_slot.lock().await = Some(h.base_url.clone());
    let (mut p1, mut p2, token, _pid1, _pid2) = pair_up(&h, 300, 301, "server_arena").await;

    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "accepts must land"
    );
    let _ = wait_for_event(
        "match-result p1",
        &mut p1,
        std::time::Instant::now() + Duration::from_secs(15),
        |ev| match ev {
            ServerEvent::MatchResult { .. } => Some(()),
            _ => None,
        },
    )
    .await;
    assert!(
        wait_for_status(&h.pool, &token, "Resolved").await,
        "happy path must resolve"
    );

    let client = reqwest::Client::new();

    // Wrong secret -> 401.
    let resp = client
        .post(format!(
            "{}/internal/game-result/{token}/wrong-secret",
            h.base_url
        ))
        .json(&serde_json::json!({ "winner": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 401);

    // Correct secret but already resolved -> 409.
    let secret: String =
        sqlx::query_scalar("SELECT result_secret FROM matches WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let resp = client
        .post(format!(
            "{}/internal/game-result/{token}/{secret}",
            h.base_url
        ))
        .json(&serde_json::json!({ "winner": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 409);

    // Unknown token -> 404.
    let resp = client
        .post(format!(
            "{}/internal/game-result/nonexistent-token/some-secret",
            h.base_url
        ))
        .json(&serde_json::json!({ "winner": null }))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 404);

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn pong_auto_resolves_on_three_points(pool: sqlx::PgPool) {
    let h = setup_temporal_pong(pool).await;
    let (mut p1, mut p2, token, pid1, _pid2) = pair_up(&h, 110, 210, "ranked_1v1").await;

    accept_and_connect(&h, &mut p1, &mut p2, &token).await;

    // The referee only advances a frame once BOTH players' inputs for it have
    // arrived, so each side must keep sending frame-stamped inputs for the
    // whole rally (~5s to 3 points at base speed). 110's paddle stays on the
    // serve line (0.5); 210's parks at the top (clamps to 0.08). The
    // deterministic serve goes toward the parked side at y = 0.5, so 110
    // scores every rally — regardless of which side the (racy) pairing
    // assigns them.
    let mut frame = 0u32;
    let deadline = std::time::Instant::now() + Duration::from_secs(12);
    while std::time::Instant::now() < deadline {
        p1.send_game_input(&token, frame, 0.5).await.unwrap();
        p2.send_game_input(&token, frame, 0.05).await.unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
        frame += 1;
    }
    // Step 11: the referee is playback-only — it exits at 3 points WITHOUT
    // resolving. The workflow resolves on the clients' who_won reports: 110
    // (serve line, scores every rally) claims the win and both report it.
    p1.submit_report(&token, Some(&pid1), Some("demo-a"))
        .await
        .unwrap();
    p2.submit_report(&token, Some(&pid1), Some("demo-b"))
        .await
        .unwrap();

    // The workflow's finish_match resolves after both reports (~5s of rally
    // plus the report round-trip); poll the terminal DB state for 30s.
    assert!(
        wait_for_status_for(&h.pool, &token, "Resolved", 30).await,
        "the workflow must resolve the match after both players report"
    );
    // `outcome` is player_a-perspective and pairing order is racy.
    let player_a: uuid::Uuid =
        sqlx::query_scalar("SELECT player_a FROM matches WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let expected = if player_a.to_string() == pid1 {
        "Win"
    } else {
        "Loss"
    };
    let outcome: String =
        sqlx::query_scalar("SELECT outcome FROM match_results WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        outcome, expected,
        "outcome must match player_a's perspective"
    );

    // Both players reset to the menus (resolve_agreed terminal reset).
    let s110: String = sqlx::query_scalar("SELECT state FROM player_state WHERE user_id = $1::uuid")
        .bind(&pid1)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    let s210: String = sqlx::query_scalar("SELECT state FROM player_state WHERE user_id = $1::uuid")
        .bind(&_pid2)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(s110, "InMenus", "winner must be free to queue again");
    assert_eq!(s210, "InMenus", "loser must be free to queue again");

    // p1's stream: at least one authoritative frame, then GameOver for 110.
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    let _ = wait_for_event("pong first-to-3", &mut p1, deadline, |ev| match ev {
        ServerEvent::GameState { .. } => Some(true),
        _ => None,
    })
    .await;
    let winner = wait_for_event("pong game_over", &mut p1, deadline, |ev| match ev {
        ServerEvent::GameOver { winner, .. } => Some(winner),
        _ => None,
    })
    .await;
    assert_eq!(winner, pid1, "110 must claim the victory");

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn queueing_survives_stale_sweep_after_reconnect(pool: sqlx::PgPool) {
    // A reconnect whose last heartbeat predates the 30s stale window must not
    // be evicted the moment they queue: queueing itself refreshes liveness,
    // so the entry gets the full grace period from the queue click.
    let h = setup_temporal(pool).await;

    let mut c1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = c1.authenticate_test_token(702, &h.base_url).await.unwrap();

    // Age the player's liveness past the sweep cutoff (like a player who
    // reconnects after their last session's heartbeats went stale).
    sqlx::query(
        "UPDATE player_state SET last_heartbeat = NOW() - INTERVAL '60 seconds' WHERE user_id = $1::uuid",
    )
    .bind(&auth.player_id)
    .execute(&h.pool)
    .await
    .unwrap();

    c1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // Wait past two ticker ticks (2s each) — the pre-fix behavior swept the
    // entry on the first tick after queueing.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let queued: Option<uuid::Uuid> =
        sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
            .bind(&auth.player_id)
            .fetch_optional(&h.pool)
            .await
            .unwrap();
    assert!(
        queued.is_some(),
        "a just-queued player must survive the stale sweep"
    );
    let state: String = sqlx::query_scalar("SELECT state FROM player_state WHERE user_id = $1::uuid")
        .bind(&auth.player_id)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(state, "Queueing", "queue state must be intact");

    drop(c1);
}

#[sqlx::test]
async fn start_timeout_forfeits_non_starter(pool: sqlx::PgPool) {
    // Both players accept; only p1 (110) clicks START. After the 2s window,
    // the server must forfeit p2 and award the match to p1.
    let h = setup_temporal_pong_start_timeout(pool, 2).await;
    let (mut p1, mut p2, token, pid1, _pid2) = pair_up(&h, 110, 210, "ranked_1v1").await;

    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "both accepts must transition the match to InProgress"
    );

    // Only the starter signals START; p2 never does.
    p1.start_match(&token).await.unwrap();

    assert!(
        wait_for_status_for(&h.pool, &token, "Resolved", 15).await,
        "the 2s START window must forfeit-resolve the match"
    );
    // outcome is player_a-perspective; pairing order is racy, so compute the
    // expected value from the starter's side.
    let player_a: uuid::Uuid =
        sqlx::query_scalar("SELECT player_a FROM matches WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    let expected = if player_a.to_string() == pid1 {
        "Win"
    } else {
        "Loss"
    };
    let outcome: String =
        sqlx::query_scalar("SELECT outcome FROM match_results WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        outcome, expected,
        "the starter must be recorded as the winner"
    );

    // The starter's rating rises; the non-starter's falls.
    let mu110: f64 = sqlx::query_scalar(
        "SELECT mu FROM ratings WHERE user_id = $1::uuid AND game_mode = 'ranked_1v1'",
    )
    .bind(&pid1)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    let mu210: f64 = sqlx::query_scalar(
        "SELECT mu FROM ratings WHERE user_id = $1::uuid AND game_mode = 'ranked_1v1'",
    )
    .bind(&_pid2)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert!(mu110 > 25.0, "starter's mu should increase, got {mu110}");
    assert!(
        mu210 < 25.0,
        "non-starter's mu should decrease, got {mu210}"
    );

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn start_timeout_forfeits_neither(pool: sqlx::PgPool) {
    // Both players accept but NEITHER clicks START → double loss (user
    // decision): outcome "Forfeit", both mu changes negative, both freed.
    let h = setup_temporal_pong_start_timeout(pool, 2).await;
    let (mut p1, mut p2, token, pid1, pid2) = pair_up(&h, 110, 210, "ranked_1v1").await;

    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "both accepts must transition the match to InProgress"
    );

    assert!(
        wait_for_status_for(&h.pool, &token, "Resolved", 15).await,
        "the 2s START window must resolve the match when nobody starts"
    );
    let outcome: String =
        sqlx::query_scalar("SELECT outcome FROM match_results WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        outcome, "Forfeit",
        "neither-started must record a double-loss Forfeit"
    );

    let (mu_change_a, mu_change_b): (f64, f64) =
        sqlx::query_as("SELECT mu_change_a, mu_change_b FROM match_results WHERE match_token = $1")
            .bind(&token)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert!(
        mu_change_a < 0.0,
        "player_a must lose rating, got {mu_change_a}"
    );
    assert!(
        mu_change_b < 0.0,
        "player_b must lose rating, got {mu_change_b}"
    );

    // Terminal: both players return to the menus.
    let s110: String = sqlx::query_scalar("SELECT state FROM player_state WHERE user_id = $1::uuid")
        .bind(&pid1)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    let s210: String = sqlx::query_scalar("SELECT state FROM player_state WHERE user_id = $1::uuid")
        .bind(&pid2)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(s110, "InMenus", "player 110 must be free to queue again");
    assert_eq!(s210, "InMenus", "player 210 must be free to queue again");

    // BOTH clients get a MatchResult whose outcome serializes with a Forfeit key.
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    let mut check = |ev: ServerEvent| match ev {
        ServerEvent::MatchResult { outcome, .. } => Some(outcome.get("Forfeit").is_some()),
        _ => None,
    };
    let r1 = wait_for_event("forfeit match_result p1", &mut p1, deadline, &mut check).await;
    let r2 = wait_for_event("forfeit match_result p2", &mut p2, deadline, &mut check).await;
    assert!(r1, "p1's match_result must carry a Forfeit outcome");
    assert!(r2, "p2's match_result must carry a Forfeit outcome");

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn pong_broadcasts_round_start_and_holds(pool: sqlx::PgPool) {
    // With the countdown enabled (90 ticks), the referee must broadcast a
    // RoundStart and then hold the sim frozen (constant checksum) for exactly
    // 90 frames before any checksum changes.
    let h = setup_temporal_pong_countdown(pool).await;
    let (mut p1, mut p2, token, _pid1, _pid2) = pair_up(&h, 110, 210, "ranked_1v1").await;

    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "InProgress").await,
        "both accepts must transition the match to InProgress"
    );
    p1.start_match(&token).await.unwrap();
    p2.start_match(&token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, &token, "Reporting").await,
        "both starts must transition the match to Reporting"
    );

    // p1 must receive RoundStart { frame: 0, round: 0, countdown_ticks: 90 }.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let rs = wait_for_event("round_start", &mut p1, deadline, |ev| match ev {
        ServerEvent::RoundStart {
            frame,
            round,
            countdown_ticks,
            ..
        } => Some((frame, round, countdown_ticks)),
        _ => None,
    })
    .await;
    assert_eq!(rs, (0, 0, 90), "round 0 must open with a 90-tick hold");

    // Feed inputs so the sim can advance past the hold; the hold itself is
    // broadcast regardless (frozen snapshot + constant checksum).
    let mut frame = 0u32;
    let feed_deadline = std::time::Instant::now() + Duration::from_secs(7);
    while std::time::Instant::now() < feed_deadline {
        p1.send_game_input(&token, frame, 0.5).await.unwrap();
        p2.send_game_input(&token, frame, 0.05).await.unwrap();
        tokio::time::sleep(Duration::from_millis(33)).await;
        frame += 1;
    }

    // Collect GameState checksums; the first ~90 frames must be IDENTICAL
    // (frozen ball), then the checksum changes once the ball launches.
    let mut checksums: Vec<(u32, String)> = Vec::new();
    let collect_deadline = std::time::Instant::now() + Duration::from_secs(10);
    while checksums.len() < 120 && std::time::Instant::now() < collect_deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(ServerEvent::GameState {
                frame, checksum, ..
            }))) => {
                if !checksums.iter().any(|(f, _)| *f == frame) {
                    checksums.push((frame, checksum));
                }
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    checksums.sort_by_key(|(f, _)| *f);
    assert!(
        checksums.len() >= 91,
        "need frames 0..90+, got {}",
        checksums.len()
    );
    let first = &checksums[0].1;
    for (f, c) in checksums.iter().take(90) {
        assert_eq!(c, first, "frame {f} must be frozen (constant checksum)");
    }
    assert_ne!(
        &checksums[90].1, first,
        "frame 90 must have a different checksum — the ball launches after the hold"
    );

    drop(p1);
    drop(p2);
}

// ── Schedule + queue-lifecycle regressions (matchmaker plan) ─────────────────

#[sqlx::test]
async fn pair_next_match_atomicity(pool: sqlx::PgPool) {
    let h = setup(pool).await;

    // Two queued players (Queueing state) for ranked_1v1, seeded directly.
    for sid in [1001u64, 1002u64] {
        let uid = uuid::Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, steam_id, display_name) VALUES ($1, $2, 'atomic-test')")
            .bind(uid)
            .bind(sid as i64)
            .execute(&h.pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO player_state (user_id, state, last_heartbeat) \
             VALUES ($1, 'Queueing', NOW())",
        )
        .bind(uid)
        .execute(&h.pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO matchmaking_queue (user_id, game_mode, match_difficulty, mu, queued_at) \
             VALUES ($1, 'ranked_1v1', 'normal', 25.0, NOW())",
        )
        .bind(uid)
        .execute(&h.pool)
        .await
        .unwrap();
    }

    // Two concurrent pairers: the FOR UPDATE transaction serializes them —
    // exactly one forms the match, the other sees an empty queue.
    let (ra, rb) = tokio::join!(
        h.state
            .store
            .pair_next_match("ranked_1v1", lobby_core::types::GameType::P2p, 300),
        h.state
            .store
            .pair_next_match("ranked_1v1", lobby_core::types::GameType::P2p, 300),
    );
    let formed = [ra, rb]
        .iter()
        .filter(|r| matches!(r, Ok(Some(_))))
        .count();
    assert_eq!(formed, 1, "exactly one pairer must form the match");

    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM matches WHERE game_mode = 'ranked_1v1'")
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(count, 1, "exactly one match row for the mode");
}

#[sqlx::test]
async fn requeue_after_unqueue_pairs(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = p1.authenticate_test_token(803, &h.base_url).await.unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // Wait for the entry, then unqueue.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never appeared for the queued player"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    p1.cancel_matchmaking().await.unwrap();

    // The leave_queue activity removes the row asynchronously.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never removed after unqueue"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Re-queue, then queue a second player: both must get the same match (the
    // re-queue failure regression — the second queue signal used to be
    // swallowed by the queue-child "already started" error).
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
    let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    p2.authenticate_test_token(804, &h.base_url).await.unwrap();
    p2.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    let m1 = timeout(Duration::from_secs(15), p1.wait_for_match())
        .await
        .expect("p1 match within 15s")
        .unwrap()
        .unwrap();
    let m2 = timeout(Duration::from_secs(15), p2.wait_for_match())
        .await
        .expect("p2 match within 15s")
        .unwrap()
        .unwrap();
    assert_eq!(
        m1.match_token, m2.match_token,
        "both players must get the same match"
    );

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn reconnect_then_unqueue_removes_entry(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = p1.authenticate_test_token(805, &h.base_url).await.unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never appeared for the queued player"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Brief drop (the entry survives the 30s stale window), then reconnect:
    // the fresh per-connection session must recover the queued state via
    // sync_session, so unqueue still acts on the DB row.
    drop(p1);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    p2.authenticate_test_token(805, &h.base_url).await.unwrap();
    p2.cancel_matchmaking().await.unwrap();

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_none() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "recovered session must be able to unqueue"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(p2);
}

#[sqlx::test]
async fn p2p_match_workflow_decline_completes(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;
    let (mut p1, mut p2, token, _pid1, pid2) = pair_up(&h, 801, 802, "ranked_1v1").await;

    // p1 accepts first — the pre-fix code recorded the ACCEPTOR (p1) as the
    // decliner when one player accepted before the other declined.
    p1.accept_match(&token).await.unwrap();
    p2.decline_match(&token).await.unwrap();

    // (a) The match workflow must COMPLETE — not linger in the accept wait.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        if h
            .workflow_status(&format!("match-{token}"))
            .await
            .as_deref()
            == Some("Completed")
        {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "match workflow never completed"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // (b) The declined event records the REAL decliner (p2), not the acceptor.
    let decliner: Option<String> = sqlx::query_scalar(
        "SELECT user_id::text FROM match_events WHERE match_token = $1 AND event_type = 'declined'",
    )
    .bind(&token)
    .fetch_optional(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        decliner.as_deref(),
        Some(pid2.as_str()),
        "declined event must record the real decliner"
    );

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn queue_expired_allows_requeue(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;

    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = p1.authenticate_test_token(806, &h.base_url).await.unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();

    // Wait for the entry, then age the heartbeat past the 30s stale window.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue row never appeared for the queued player"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    sqlx::query(
        "UPDATE player_state SET last_heartbeat = NOW() - INTERVAL '40 seconds' WHERE user_id = $1::uuid",
    )
    .bind(&auth.player_id)
    .execute(&h.pool)
    .await
    .unwrap();

    // The ticker evicts the entry and must tell the client QueueExpired.
    let deadline = std::time::Instant::now() + Duration::from_secs(6);
    let mut expired = false;
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(2), p1.next_event()).await {
            Ok(Some(Ok(lobby_client::ServerEvent::QueueExpired))) => {
                expired = true;
                break;
            }
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
        }
    }
    assert!(expired, "queued player must be told when its entry expires");

    // Re-queue: BEFORE the queue_expired signal, the session's `queued` copy
    // was never cleared, so this signal was swallowed and no row reappeared.
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let queued: Option<uuid::Uuid> =
            sqlx::query_scalar("SELECT user_id FROM matchmaking_queue WHERE user_id = $1::uuid")
                .bind(&auth.player_id)
                .fetch_optional(&h.pool)
                .await
                .unwrap();
        if queued.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "queue-expired player must be able to re-queue"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    drop(p1);
}
// ── OAuth2/OIDC provider tests (login-providers Step 9b) ──────────────────

/// Shared mutable state the mock OAuth routes read/write: tests swap
/// `userinfo` to simulate a changed group membership.
pub struct MockSlots {
    pub userinfo: tokio::sync::RwLock<serde_json::Value>,
}

/// Spawn a mock OAuth2/OIDC provider on an ephemeral port.
/// Routes: /authorize (307 → redirect_uri&code=mock-code-{provider}),
/// /token (→ {"access_token": "mock-token"}), /userinfo (→ slots.userinfo).
async fn spawn_mock_oauth(provider: &str) -> (String, std::sync::Arc<MockSlots>) {
    let base_userinfo = serde_json::json!({
        "sub": format!("mock-uid-{provider}"),
        "preferred_username": format!("Mock {provider}"),
        "id": format!("mock-uid-{provider}"),
        "global_name": format!("Mock {provider}"),
    });
    let mut userinfo = base_userinfo;
    if provider == "au2143" {
        userinfo["groups"] = serde_json::json!(["pvp_admin"]);
    }
    let slots = std::sync::Arc::new(MockSlots {
        userinfo: tokio::sync::RwLock::new(userinfo),
    });

    let provider_owned = provider.to_string();
    let slots_route = slots.clone();
    let app = Router::new()
        .route(
            "/authorize",
            axum::routing::get(move |axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>| {
                let provider = provider_owned.clone();
                async move {
                    let redirect_uri = params.get("redirect_uri").cloned().unwrap_or_default();
                    axum::response::Redirect::temporary(&format!(
                        "{redirect_uri}?code=mock-code-{provider}&state={}",
                        params.get("state").cloned().unwrap_or_default()
                    ))
                }
            }),
        )
        .route(
            "/token",
            axum::routing::post(|| async { Json(serde_json::json!({"access_token": "mock-token"})) }),
        )
        .route(
            "/userinfo",
            axum::routing::get({
                let slots = slots_route.clone();
                move || {
                    let slots = slots.clone();
                    async move { Json(slots.userinfo.read().await.clone()) }
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
        )
        .await
        .unwrap();
    });
    (format!("http://{addr}"), slots)
}

/// Build a ProviderConfig pointing at the mock's endpoints.
fn mock_provider_config(id: &str, base: &str) -> ProviderConfig {
    ProviderConfig {
        id: id.to_string(),
        kind: if id == "discord" {
            ProviderKind::OAuth2
        } else {
            ProviderKind::Oidc
        },
        client_id: format!("mock-client-{id}"),
        client_secret: format!("mock-secret-{id}"),
        authorization_endpoint: format!("{base}/authorize"),
        token_endpoint: format!("{base}/token"),
        userinfo_endpoint: format!("{base}/userinfo"),
        scopes: vec!["openid".into(), "groups".into()],
        id_field: if id == "discord" { "id".into() } else { "sub".into() },
        name_field: if id == "discord" { "global_name".into() } else { "preferred_username".into() },
        use_pkce: id != "discord",
    }
}

/// Walk the full redirect chain manually (reqwest with redirects disabled):
/// login → mock /authorize (307) → callback with the code.
async fn walk_provider_login(h: &common::TestHarness, provider: &str) -> String {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let login_url = format!("{}/auth/{provider}/login?return_to=/", h.base_url);
    let login_resp = client.get(&login_url).send().await.unwrap();
    assert_eq!(login_resp.status(), StatusCode::TEMPORARY_REDIRECT);
    let authorize_url = login_resp
        .headers()
        .get(axum::http::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    // The mock's /authorize 307 echoes the callback URL the server built from
    // PUBLIC_URL (https://lobby.example.com — not resolvable in tests). Parse
    // the code+state out of that location and re-target the callback at the
    // live test server's base_url.
    let auth_resp = client.get(&authorize_url).send().await.unwrap();
    assert_eq!(auth_resp.status(), StatusCode::TEMPORARY_REDIRECT);
    let mock_loc = auth_resp
        .headers()
        .get(axum::http::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    let parsed = url::Url::parse(&mock_loc).unwrap();
    let code = parsed
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default();
    let state = parsed
        .query_pairs()
        .find(|(k, _)| k == "state")
        .map(|(_, v)| v.into_owned())
        .unwrap_or_default();
    let callback_url = format!(
        "{}/auth/{provider}/callback?code={code}&state={state}&return_to=/",
        h.base_url
    );
    // Follow the callback to the final #token= redirect.
    let cb_resp = client.get(&callback_url).send().await.unwrap();
    assert_eq!(cb_resp.status(), StatusCode::TEMPORARY_REDIRECT);
    let final_url = cb_resp
        .headers()
        .get(axum::http::header::LOCATION)
        .unwrap()
        .to_str()
        .unwrap()
        .to_string();
    final_url
        .split("#token=")
        .nth(1)
        .expect("callback redirect must carry #token=")
        .to_string()
}

#[sqlx::test]
async fn oauth_login_creates_account_and_plays_match(pool: sqlx::PgPool) {
    let (mock_base, _slots) = spawn_mock_oauth("discord").await;
    let h = setup_temporal_with_auth(
        pool,
        vec![mock_provider_config("discord", &mock_base)],
    )
    .await;

    let token = walk_provider_login(&h, "discord").await;

    // The JWT validates and the account exists with steam_id NULL + identity.
    let ws_auth: lobby_client::AuthOk = {
        let mut c = lobby_client::LobbyClient::connect(&h.ws_url).await.unwrap();
        c.authenticate(&token).await.unwrap()
    };
    let user_id = ws_auth.player_id.clone();
    assert!(!user_id.is_empty(), "player_id must be a non-empty UUID string");
    let row = sqlx::query_as::<_, (Option<i64>, String)>(
        "SELECT steam_id, primary_provider FROM users WHERE id = $1::uuid",
    )
    .bind(&user_id)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(row.0, None, "discord user must have steam_id NULL");
    assert_eq!(row.1, "discord");
    let identity = sqlx::query_as::<_, (String,)>(
        "SELECT provider_uid FROM user_identities WHERE provider = 'discord' AND user_id = $1::uuid",
    )
    .bind(&user_id)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(identity.0, "mock-uid-discord");

    // The discord player queues against a dev player and plays a full match.
    let mut p1 = lobby_client::LobbyClient::connect(&h.ws_url).await.unwrap();
    let _ = p1.authenticate(&token).await.unwrap();
    let mut p2 = lobby_client::LobbyClient::connect(&h.ws_url).await.unwrap();
    let a2 = p2
        .authenticate_test_token(200, &h.base_url)
        .await
        .unwrap();
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
    p2.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
    let m1 = timeout(Duration::from_secs(15), p1.wait_for_match())
        .await
        .expect("p1 match")
        .unwrap()
        .unwrap();
    let m2 = timeout(Duration::from_secs(15), p2.wait_for_match())
        .await
        .expect("p2 match")
        .unwrap()
        .unwrap();
    assert_eq!(m1.match_token, m2.match_token);
    p1.accept_match(&m1.match_token).await.unwrap();
    p2.accept_match(&m1.match_token).await.unwrap();
    assert!(wait_for_status(&h.pool, &m1.match_token, "InProgress").await);
    p1.start_match(&m1.match_token).await.unwrap();
    p2.start_match(&m1.match_token).await.unwrap();
    assert!(wait_for_status(&h.pool, &m1.match_token, "Reporting").await);
    p1.submit_report(&m1.match_token, Some(&user_id), None)
        .await
        .unwrap();
    p2.submit_report(&m1.match_token, Some(&user_id), None)
        .await
        .unwrap();
    assert!(
        wait_for_row(
            &h.pool,
            &m1.match_token,
            "SELECT match_token, mu_change_a FROM match_results WHERE match_token = $1",
        )
        .await
        .is_some(),
        "match must resolve for a non-Steam player"
    );
    let _ = a2.player_id;
}

#[sqlx::test]
async fn auth_config_advertises_configured_providers(pool: sqlx::PgPool) {
    let (mock_base, _slots) = spawn_mock_oauth("discord").await;
    let h = setup_temporal_with_auth(
        pool,
        vec![mock_provider_config("discord", &mock_base)],
    )
    .await;
    let cfg: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/auth/config", h.base_url))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let providers = cfg["providers"].as_array().unwrap();
    assert!(
        providers.iter().any(|p| p.as_str() == Some("discord")),
        "providers must contain discord"
    );
    assert!(
        !providers.iter().any(|p| p.as_str() == Some("au2143")),
        "providers must NOT contain au2143 (not configured)"
    );
    assert!(
        providers.iter().any(|p| p.as_str() == Some("steam")),
        "public_url is set, so steam must be listed"
    );
}

#[sqlx::test]
async fn unknown_provider_404s(pool: sqlx::PgPool) {
    let h = setup_temporal(pool).await;
    let resp = reqwest::Client::new()
        .get(format!("{}/auth/nope/login", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn au2143_discovery_failure_disables_provider() {
    // Build a registry with au2143_client_id set + an unreachable issuer and
    // no overrides → the registry must contain NO au2143 provider.
    let http = reqwest::Client::new();
    let registry = lobby_server::auth_providers::build(
        None,
        None,
        Some("au2143-client".into()),
        Some("au2143-secret".into()),
        "http://127.0.0.1:1".into(), // nothing listens here — discovery fails fast
        None,
        &http,
    )
    .await;
    assert!(
        registry.get("au2143").is_none(),
        "discovery failure must disable the au2143 provider, never a boot error"
    );
    assert!(registry.providers.is_empty());
}

#[sqlx::test]
async fn au2143_group_controls_admin_flag(pool: sqlx::PgPool) {
    let (mock_base, slots) = spawn_mock_oauth("au2143").await;
    let h = setup_temporal_with_auth(
        pool,
        vec![mock_provider_config("au2143", &mock_base)],
    )
    .await;

    // First login: the mock userinfo carries groups: ["pvp_admin"].
    let token = walk_provider_login(&h, "au2143").await;
    let mut c = lobby_client::LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth = c.authenticate(&token).await.unwrap();
    let user_id = auth.player_id.clone();
    let is_admin: bool = sqlx::query_scalar("SELECT is_admin FROM users WHERE id = $1::uuid")
        .bind(&user_id)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert!(is_admin, "pvp_admin group must set is_admin = true");

    // Group removed → re-login clears the flag (self-healing revocation).
    {
        let mut ui = slots.userinfo.write().await;
        ui["groups"] = serde_json::json!(["pvp_regular"]);
    }
    let token2 = walk_provider_login(&h, "au2143").await;
    let mut c2 = lobby_client::LobbyClient::connect(&h.ws_url).await.unwrap();
    let auth2 = c2.authenticate(&token2).await.unwrap();
    assert_eq!(auth2.player_id, user_id, "same sub → same account");
    let is_admin2: bool = sqlx::query_scalar("SELECT is_admin FROM users WHERE id = $1::uuid")
        .bind(&user_id)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert!(!is_admin2, "group removal must clear the flag on the next login");

    // A discord login (no groups claim) stays false on its own row.
    let (mock2_base, _s2) = spawn_mock_oauth("discord").await;
    let h2 = setup_temporal_with_auth(
        h.pool.clone(),
        vec![
            mock_provider_config("au2143", &mock_base),
            mock_provider_config("discord", &mock2_base),
        ],
    )
    .await;
    let dtoken = walk_provider_login(&h2, "discord").await;
    let mut dc = lobby_client::LobbyClient::connect(&h2.ws_url).await.unwrap();
    let dauth = dc.authenticate(&dtoken).await.unwrap();
    let is_admin3: bool = sqlx::query_scalar("SELECT is_admin FROM users WHERE id = $1::uuid")
        .bind(&dauth.player_id)
        .fetch_one(&h2.pool)
        .await
        .unwrap();
    assert!(!is_admin3, "discord login (no groups claim) must leave is_admin false");
}
