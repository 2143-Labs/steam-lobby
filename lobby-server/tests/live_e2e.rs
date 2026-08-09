//! Live-server E2E smoke test (Temporal-driven lifecycle). Connects two
//! clients to a RUNNING lobby-server (AUTH_DEV_MODE=true) over WebSocket and
//! drives queue → match_found → accept → start → report through the workflows.
//! Not run by the suite (`#[ignore]`); used to verify the Step 9 wiring live.
use std::time::Duration;

use lobby_client::{LobbyClient, ServerEvent};

async fn auth_client(base: &str, steam_id: u64) -> LobbyClient {
    let token: serde_json::Value = reqwest::Client::new()
        .post(format!("{base}/auth/test-token"))
        .json(&serde_json::json!({ "steam_id": steam_id }))
        .send()
        .await
        .expect("test-token")
        .json()
        .await
        .expect("token json");
    let token = token["token"].as_str().expect("token").to_string();
    let ws_url = base.replace("http", "ws") + "/ws";
    let mut c = LobbyClient::connect(&ws_url).await.expect("connect");
    c.authenticate(&token).await.expect("auth");
    c
}

#[tokio::test]
#[ignore = "requires a running dev server with Temporal up"]
async fn live_temporal_full_lifecycle() {
    let base = std::env::var("LOBBY_BASE").unwrap_or_else(|_| "http://127.0.0.1:8080".into());
    // Fresh dev IDs per run: the 300s pair cooldown blocks re-pairing the same
    // two steam_ids right after a resolved match.
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        % 1_000_000;
    let p1_id = 900_000 + n;
    let p2_id = 901_000 + n;
    let mut p1 = auth_client(&base, p1_id).await;
    let mut p2 = auth_client(&base, p2_id).await;

    p1.begin_matchmaking("ranked_1v1", "normal")
        .await
        .expect("p1 queue");
    p2.begin_matchmaking("ranked_1v1", "normal")
        .await
        .expect("p2 queue");

    let m1 = tokio::time::timeout(Duration::from_secs(15), p1.wait_for_match())
        .await
        .expect("p1 match within 15s")
        .expect("p1 match")
        .expect("match info");
    let m2 = tokio::time::timeout(Duration::from_secs(15), p2.wait_for_match())
        .await
        .expect("p2 match within 15s")
        .expect("p2 match")
        .expect("match info");
    assert_eq!(m1.match_token, m2.match_token, "same match");
    let token = m1.match_token.clone();
    println!("matched: {token}");

    p1.accept_match(&token).await.expect("p1 accept");
    p2.accept_match(&token).await.expect("p2 accept");

    // Both players should receive match_started (the workflow's mark_accepts).
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match p1.next_event().await {
                Some(Ok(ServerEvent::MatchStarted { .. })) => break,
                Some(Ok(_)) => continue,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("p1 match_started");
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match p2.next_event().await {
                Some(Ok(ServerEvent::MatchStarted { .. })) => break,
                Some(Ok(_)) => continue,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("p2 match_started");
    println!("match_started: both players");

    p1.start_match(&token).await.expect("p1 start");
    p2.start_match(&token).await.expect("p2 start");

    // Wait for the first GameState frame (the referee spawned after both
    // started — the workflow's mark_connected flipped Reporting).
    let frame = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            match p1.next_event().await {
                Some(Ok(ServerEvent::GameState { frame, .. })) => break frame,
                Some(Ok(_)) => continue,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("game frame");
    println!("game running, first frame {frame}");

    // Both report the same winner (p1 won) → the workflow's finish_match resolves.
    p1.submit_report(&token, Some(p1_id), Some("demo-a"))
        .await
        .expect("p1 report");
    p2.submit_report(&token, Some(p1_id), Some("demo-b"))
        .await
        .expect("p2 report");

    let result = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            match p1.next_event().await {
                Some(Ok(ServerEvent::MatchResult { outcome, .. })) => break outcome,
                Some(Ok(_)) => continue,
                _ => tokio::time::sleep(Duration::from_millis(50)).await,
            }
        }
    })
    .await
    .expect("match result");
    println!("match result: {result}");
    assert!(
        result.to_string().contains("Win"),
        "expected a Win outcome, got {result}"
    );
    println!("LIVE E2E OK");
}
