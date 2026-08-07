// webrtc_signaling.rs — WebRTC signaling relay integration tests.
// Needs Postgres (`just db-up`).

mod common;

use std::time::Duration;

use lobby_client::LobbyClient;
use lobby_client::ServerEvent;
use tokio::time::timeout;

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

/// Single test: TURN endpoint (200/503) + signaling relay + negative test.
/// Combined because common.rs uses a tokio::sync::Mutex for DB serialisation;
/// multiple #[tokio::test] functions deadlock across separate runtimes. The
/// DB_LOCK guard is alive for the lifetime of each TestHarness — so scoping is
/// explicit.
#[tokio::test]
async fn webrtc_signaling() {
    // ── 200 + signaling relay (harness with turn secret) ──
    let token = {
        let h = common::setup_with_turn(Some("test-turn-secret")).await;

        // turn_endpoint: 200
        let resp = reqwest::get(format!("{}/internal/turn-credentials", h.base_url)).await.unwrap();
        assert_eq!(resp.status(), 200);
        let body: serde_json::Value = resp.json().await.unwrap();
        assert!(body["username"].as_str().unwrap().ends_with(":steam-lobby"));
        assert!(!body["password"].as_str().unwrap().is_empty());
        assert_eq!(body["ttl"].as_u64().unwrap(), 3600);
        assert!(!body["uris"].as_array().unwrap().is_empty());

        // signaling_relay
        let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
        let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
        p1.authenticate_test_token(9990, &h.base_url).await.unwrap();
        p2.authenticate_test_token(9991, &h.base_url).await.unwrap();

        p1.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
        p2.begin_matchmaking("ranked_1v1", "normal").await.unwrap();
        let m1 = timeout(Duration::from_secs(15), p1.wait_for_match()).await.unwrap().unwrap().unwrap();
        let m2 = timeout(Duration::from_secs(15), p2.wait_for_match()).await.unwrap().unwrap().unwrap();
        assert_eq!(m1.match_token, m2.match_token);
        let token = m1.match_token;

        p1.accept_match(&token).await.unwrap();
        p2.accept_match(&token).await.unwrap();
        p1.p2p_connected(&token).await.unwrap();
        p2.p2p_connected(&token).await.unwrap();

        // Offer/answer/ice round-trip
        p1.send_webrtc_offer(&token, "sdp-offer-a".into()).await.unwrap();
        expect(&mut p2, 5, |ev| matches!(ev, ServerEvent::WebrtcOffer { sdp, .. } if sdp == "sdp-offer-a")).await;
        p2.send_webrtc_answer(&token, "sdp-answer-b".into()).await.unwrap();
        expect(&mut p1, 5, |ev| matches!(ev, ServerEvent::WebrtcAnswer { sdp, .. } if sdp == "sdp-answer-b")).await;
        p1.send_webrtc_ice(&token, "cand-a".into()).await.unwrap();
        expect(&mut p2, 5, |ev| matches!(ev, ServerEvent::WebrtcIce { candidate, .. } if candidate == "cand-a")).await;
        p2.send_webrtc_ice(&token, "cand-b".into()).await.unwrap();
        expect(&mut p1, 5, |ev| matches!(ev, ServerEvent::WebrtcIce { candidate, .. } if candidate == "cand-b")).await;

        // Negative: non-participant
        let mut p3 = LobbyClient::connect(&h.ws_url).await.unwrap();
        p3.authenticate_test_token(9992, &h.base_url).await.unwrap();
        p3.send_webrtc_offer(&token, "spoofed-offer".into()).await.unwrap();

        let end = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut p3_ok = true;
        while tokio::time::Instant::now() < end {
            match timeout(Duration::from_millis(100), p3.next_event()).await {
                Ok(Some(Ok(ev))) if matches!(ev, ServerEvent::WebrtcOffer { .. } | ServerEvent::WebrtcAnswer { .. } | ServerEvent::WebrtcIce { .. }) => { p3_ok = false; }
                _ => {}
            }
        }
        assert!(p3_ok, "non-participant must not receive WebRTC signaling");

        let end = tokio::time::Instant::now() + Duration::from_millis(500);
        let mut p1_ok = true;
        while tokio::time::Instant::now() < end {
            match timeout(Duration::from_millis(100), p1.next_event()).await {
                Ok(Some(Ok(ServerEvent::WebrtcOffer { from, .. }))) if from == 9992 => { p1_ok = false; }
                Ok(Some(Ok(_))) => {}
                _ => {}
            }
        }
        assert!(p1_ok, "participant must not receive spoofed offer");

        token
    }; // harness dropped → DB_LOCK released

    // ── turn_endpoint: 503 (separate harness, no secret) ──
    {
        let h = common::setup_with_turn(None).await;
        let resp = reqwest::get(format!("{}/internal/turn-credentials", h.base_url)).await.unwrap();
        assert_eq!(resp.status(), 503);
    }
}
