// webrtc_signaling.rs — WebRTC signaling relay integration tests.
// Needs Postgres (`just db-up`).

mod common;

use std::time::Duration;

use lobby_client::LobbyClient;
use lobby_client::ServerEvent;
use tokio::time::timeout;

/// The TURN endpoint returns valid coturn REST credentials when configured,
/// and 503 when LOBBY_TURN_SECRET is unset.
#[tokio::test]
async fn turn_endpoint() {
    // With secret — 200 + valid payload.
    {
        let h = common::setup_with_turn(Some("test-turn-secret")).await;
        let resp = reqwest::get(format!("{}/internal/turn-credentials", h.base_url))
            .await
            .unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        let username = body["username"].as_str().unwrap();
        assert!(username.ends_with(":steam-lobby"), "username suffix, got {username}");
        assert!(!body["password"].as_str().unwrap().is_empty());
        assert_eq!(body["ttl"].as_u64().unwrap(), 3600);
        assert!(!body["uris"].as_array().unwrap().is_empty());
    }

    // Without secret — 503.
    {
        let h = common::setup_with_turn(None).await;
        let resp = reqwest::get(format!("{}/internal/turn-credentials", h.base_url))
            .await
            .unwrap();
        assert_eq!(resp.status(), 503);
    }
}

/// Block until `pred` matches an incoming event, or panic on timeout.
async fn expect<F>(client: &mut LobbyClient, secs: u64, pred: F)
where
    F: Fn(&ServerEvent) -> bool,
{
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let ev = tokio::select! {
            ev = client.next_event() => ev,
            _ = tokio::time::sleep_until(deadline) => panic!("timed out waiting for event"),
        };
        match ev {
            Some(Ok(e)) if pred(&e) => return,
            None => panic!("connection closed"),
            _ => continue,
        }
    }
}

/// Wait for a MatchFound event and return its token.
async fn wait_match(client: &mut LobbyClient) -> String {
    loop {
        let ev = timeout(Duration::from_secs(15), client.next_event())
            .await
            .unwrap();
        if let Some(Ok(ServerEvent::MatchFound { match_token, .. })) = ev {
            return match_token;
        }
    }
}

#[tokio::test]
async fn signaling_relay() {
    let h = common::setup_with_turn(Some("test-turn-secret")).await;

    // Fresh IDs — avoid the 300s cooldown window.
    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    p1.authenticate_test_token(930, &h.base_url).await.unwrap();
    p2.authenticate_test_token(931, &h.base_url).await.unwrap();

    // Queue both + wait for pairing.
    p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
    p2.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
    let t1 = wait_match(&mut p1).await;
    let t2 = wait_match(&mut p2).await;
    assert_eq!(t1, t2, "same match token");
    let token = t1;

    // Accept + p2p_connected (transitions match to Reporting, spawns pong game).
    p1.accept_match(&token).await.unwrap();
    p2.accept_match(&token).await.unwrap();
    p1.p2p_connected(&token).await.unwrap();
    p2.p2p_connected(&token).await.unwrap();

    // Offer from p1 → p2.
    p1.send_webrtc_offer(&token, "sdp-offer-a".into()).await.unwrap();
    expect(&mut p2, 5, |ev| {
        matches!(ev, ServerEvent::WebrtcOffer { sdp, .. } if sdp == "sdp-offer-a")
    }).await;

    // Answer from p2 → p1.
    p2.send_webrtc_answer(&token, "sdp-answer-b".into()).await.unwrap();
    expect(&mut p1, 5, |ev| {
        matches!(ev, ServerEvent::WebrtcAnswer { sdp, .. } if sdp == "sdp-answer-b")
    }).await;

    // ICE candidate both directions.
    p1.send_webrtc_ice(&token, "cand-a".into()).await.unwrap();
    expect(&mut p2, 5, |ev| {
        matches!(ev, ServerEvent::WebrtcIce { candidate, .. } if candidate == "cand-a")
    }).await;
    p2.send_webrtc_ice(&token, "cand-b".into()).await.unwrap();
    expect(&mut p1, 5, |ev| {
        matches!(ev, ServerEvent::WebrtcIce { candidate, .. } if candidate == "cand-b")
    }).await;

    // Negative: non-participant sends offer — relay must drop it.
    let mut p3 = LobbyClient::connect(&h.ws_url).await.unwrap();
    p3.authenticate_test_token(932, &h.base_url).await.unwrap();
    p3.send_webrtc_offer(&token, "spoofed-offer".into()).await.unwrap();

    // p3 must NOT receive any Webrtc* event within 500ms.
    let end = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut p3_ok = true;
    while tokio::time::Instant::now() < end {
        match timeout(Duration::from_millis(100), p3.next_event()).await {
            Ok(Some(Ok(ev))) if matches!(ev, ServerEvent::WebrtcOffer { .. }
                | ServerEvent::WebrtcAnswer { .. } | ServerEvent::WebrtcIce { .. }) =>
            {
                p3_ok = false;
            }
            _ => {}
        }
    }
    assert!(p3_ok, "non-participant must not receive WebRTC signaling");

    // p1 must NOT receive a spoofed offer from 932.
    let end = tokio::time::Instant::now() + Duration::from_millis(500);
    let mut p1_ok = true;
    while tokio::time::Instant::now() < end {
        match timeout(Duration::from_millis(100), p1.next_event()).await {
            Ok(Some(Ok(ServerEvent::WebrtcOffer { from, .. }))) if from == 932 => {
                p1_ok = false;
            }
            Ok(Some(Ok(_))) => {}
            _ => {}
        }
    }
    assert!(p1_ok, "participant must not receive spoofed offer from non-participant");
}
