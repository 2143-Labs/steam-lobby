use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use lobby_core::types::MatchDifficulty;
use lobby_server::commands::{self, CommandActor, RankedCommand, SessionKind};
use reqwest::StatusCode;
use serde_json::{Value, json};
use tokio::time::timeout;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, tungstenite::Message};
use uuid::Uuid;

mod common;
use common::{TestConfig, TestHarness, setup_with_config};

type TestSocket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

fn ranked_config() -> TestConfig {
    TestConfig {
        game_modes: vec![lobby_core::types::mode_spec("umvc3_1v1").unwrap()],
        ..TestConfig::default()
    }
}

async fn next_json(socket: &mut TestSocket) -> Value {
    loop {
        let message = timeout(Duration::from_secs(3), socket.next())
            .await
            .expect("websocket response within 3 seconds")
            .expect("websocket remains open")
            .expect("valid websocket frame");
        if let Message::Text(text) = message {
            return serde_json::from_str(&text).expect("websocket JSON response");
        }
    }
}

async fn connect_native(h: &TestHarness, token: &str) -> TestSocket {
    let (mut socket, response) = tokio_tungstenite::connect_async(&h.ws_url)
        .await
        .expect("connect ranked websocket");
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    socket
        .send(Message::Text(
            json!({"type":"auth","session_token":token})
                .to_string()
                .into(),
        ))
        .await
        .expect("send websocket auth");
    let auth = next_json(&mut socket).await;
    assert_eq!(auth["type"], "auth_ok");
    socket
}

async fn send_ws_command(socket: &mut TestSocket, command: Value) -> Value {
    socket
        .send(Message::Text(command.to_string().into()))
        .await
        .expect("send ranked websocket command");
    loop {
        let response = next_json(socket).await;
        if matches!(response["type"].as_str(), Some("command_receipt" | "error")) {
            return response;
        }
    }
}

async fn seed_match(
    h: &TestHarness,
    player_a: Uuid,
    player_b: Uuid,
    phase: &str,
    deadline: &str,
) -> (String, Uuid) {
    let token = Uuid::new_v4().to_string();
    let attempt_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO matches(match_token,player_a,player_a_difficulty,player_b,player_b_difficulty,game_mode,game_type,status) \
         VALUES($1,$2,'normal',$3,'hard','umvc3_1v1','p2p','PendingAccept')",
    )
    .bind(&token)
    .bind(player_a)
    .bind(player_b)
    .execute(&h.pool)
    .await
    .expect("seed ranked match");
    sqlx::query(
        "INSERT INTO umvc3_matches(match_token,phase,phase_deadline,attempt_id,original_queued_at_a,original_queued_at_b) \
         VALUES($1,$2,NOW()+$3::interval,$4,NOW()-INTERVAL '2 minutes',NOW()-INTERVAL '1 minute')",
    )
    .bind(&token)
    .bind(phase)
    .bind(deadline)
    .bind(attempt_id)
    .execute(&h.pool)
    .await
    .expect("seed UMVC3 lifecycle");
    sqlx::query(
        "UPDATE player_state SET state='MatchAccepted',active_match_token=$1 WHERE user_id=ANY($2)",
    )
    .bind(&token)
    .bind(&[player_a, player_b][..])
    .execute(&h.pool)
    .await
    .expect("seed active player states");
    (token, attempt_id)
}

#[sqlx::test]
async fn http_and_websocket_share_durable_dedupe_and_user_order(pool: sqlx::PgPool) {
    let mut config = ranked_config();
    config.ticker_enabled = false;
    let h = setup_with_config(pool, config).await;
    let (user_id, native_session, token) = h.native_principal(97101).await;
    let (same_user, _, second_token) = h.native_principal(97101).await;
    assert_eq!(same_user, user_id);
    let mut socket = connect_native(&h, &second_token).await;

    let queue_id = Uuid::new_v4();
    let queue = json!({
        "command_id": queue_id,
        "type": "queue",
        "mode": "umvc3_1v1",
        "difficulty": "hard"
    });
    let admitted = common::post_command(&h, &token, queue.clone()).await;
    assert_eq!(admitted.status(), StatusCode::ACCEPTED);
    let admitted_body: Value = admitted.json().await.unwrap();
    assert_eq!(admitted_body["status"], "pending");
    let receipt = admitted_body["receipt"].clone();

    let replay = common::post_command(&h, &token, queue).await;
    assert_eq!(replay.status(), StatusCode::OK);
    let replay_body: Value = replay.json().await.unwrap();
    assert_eq!(
        replay_body["receipt"], receipt,
        "identical pending replay must return its durable receipt"
    );
    assert_eq!(replay_body["status"], "pending");

    let conflict = common::post_command(
        &h,
        &token,
        json!({
            "command_id": queue_id,
            "type": "queue",
            "mode": "umvc3_1v1",
            "difficulty": "easy"
        }),
    )
    .await;
    assert_eq!(conflict.status(), StatusCode::CONFLICT);
    assert_eq!(
        conflict.json::<Value>().await.unwrap()["error"],
        "command_id_conflict"
    );

    let cancel_id = Uuid::new_v4();
    let ws_receipt = send_ws_command(
        &mut socket,
        json!({"type":"cancel_queue","command_id":cancel_id}),
    )
    .await;
    assert_eq!(ws_receipt["type"], "command_receipt");
    assert_eq!(ws_receipt["status"], "pending");

    let connection_id = {
        let connections = h.state.connections.lock().await;
        Uuid::parse_str(&connections.get(&user_id).expect("registered socket").session_id)
            .expect("websocket connection id is a UUID")
    };
    let streams: Vec<(Uuid, String, Uuid, i64)> = sqlx::query_as(
        "SELECT command_id,session_kind,session_id,user_sequence FROM command_inbox \
         WHERE user_id=$1 ORDER BY user_sequence",
    )
    .bind(user_id)
    .fetch_all(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        streams,
        vec![
            (queue_id, "native".into(), native_session, 1),
            (cancel_id, "websocket".into(), connection_id, 2),
        ],
        "one user sequence must order commands across durable transport sessions"
    );

    assert_eq!(commands::drain_pending(&h.state).await.unwrap(), 2);
    let results: Vec<(Uuid, String)> = sqlx::query_as(
        "SELECT command_id,status FROM command_inbox WHERE user_id=$1 ORDER BY user_sequence",
    )
    .bind(user_id)
    .fetch_all(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        results,
        vec![(queue_id, "applied".into()), (cancel_id, "applied".into())]
    );
    let queued: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM matchmaking_queue WHERE user_id=$1)",
    )
    .bind(user_id)
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert!(!queued, "ordered queue then cancel must leave the user out of queue");
}

#[sqlx::test]
async fn deadline_and_queue_flag_rejections_leave_non_queue_commands_usable(pool: sqlx::PgPool) {
    let mut config = ranked_config();
    config.ranked_queue_enabled = false;
    config.ticker_enabled = false;
    let h = setup_with_config(pool, config).await;
    let (a, _, token_a) = h.native_principal(97201).await;
    let (b, _, _) = h.native_principal(97202).await;
    let (match_token, _) = seed_match(&h, a, b, "AwaitingAccepted", "-1 second").await;

    let expired_id = Uuid::new_v4();
    let expired = common::post_command(
        &h,
        &token_a,
        json!({"command_id":expired_id,"type":"accept","match_token":match_token}),
    )
    .await;
    assert_eq!(expired.status(), StatusCode::CONFLICT);
    assert_eq!(
        expired.json::<Value>().await.unwrap()["error"],
        "deadline_elapsed"
    );

    let queue_id = Uuid::new_v4();
    let disabled = common::post_command(
        &h,
        &token_a,
        json!({"command_id":queue_id,"type":"queue","mode":"umvc3_1v1","difficulty":"normal"}),
    )
    .await;
    assert_eq!(disabled.status(), StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(
        disabled.json::<Value>().await.unwrap()["error"],
        "ranked_queue_disabled"
    );

    let heartbeat_id = Uuid::new_v4();
    let heartbeat = common::post_command(
        &h,
        &token_a,
        json!({"command_id":heartbeat_id,"type":"heartbeat"}),
    )
    .await;
    assert_eq!(heartbeat.status(), StatusCode::ACCEPTED);
    assert_eq!(commands::drain_pending(&h.state).await.unwrap(), 1);
    let status: String =
        sqlx::query_scalar("SELECT status FROM command_inbox WHERE command_id=$1")
            .bind(heartbeat_id)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(
        status, "applied",
        "queue gating must not disable non-queue ranked commands"
    );
    let rejected_rows: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM command_inbox WHERE command_id=ANY($1)",
    )
    .bind(&[expired_id, queue_id][..])
    .fetch_one(&h.pool)
    .await
    .unwrap();
    assert_eq!(
        rejected_rows, 0,
        "synchronous admission failures must not create receipts"
    );
}

#[sqlx::test]
async fn recipient_pages_and_snapshots_are_caller_local_and_cursor_exact(pool: sqlx::PgPool) {
    let mut config = ranked_config();
    config.ticker_enabled = false;
    let h = setup_with_config(pool, config).await;
    let (a, session_a, token_a) = h.native_principal(97301).await;
    let (b, _, token_b) = h.native_principal(97302).await;
    let (queued_user, queued_session, queued_token) = h.native_principal(97303).await;

    let heartbeat_id = Uuid::new_v4();
    commands::dispatch(
        &h.state,
        CommandActor {
            user_id: a,
            session_kind: SessionKind::Native,
            session_id: session_a,
        },
        heartbeat_id,
        RankedCommand::Heartbeat,
    )
    .await
    .unwrap();
    commands::drain_pending(&h.state).await.unwrap();
    let (match_token, attempt_id) =
        seed_match(&h, a, b, "AwaitingAccepted", "1 hour").await;

    let queue_id = Uuid::new_v4();
    commands::dispatch(
        &h.state,
        CommandActor {
            user_id: queued_user,
            session_kind: SessionKind::Native,
            session_id: queued_session,
        },
        queue_id,
        RankedCommand::Queue {
            mode: "umvc3_1v1".into(),
            difficulty: MatchDifficulty::Hard,
        },
    )
    .await
    .unwrap();
    commands::drain_pending(&h.state).await.unwrap();

    let mut tx = h.pool.begin().await.unwrap();
    for ordinal in 0..101 {
        commands::emit_recipient_events(
            &mut tx,
            "page-a",
            Some(a),
            vec![(a, "command_result", json!({"ordinal":ordinal}))],
        )
        .await
        .unwrap();
    }
    commands::emit_recipient_events(
        &mut tx,
        "only-b",
        Some(b),
        vec![(b, "command_result", json!({"private":"b"}))],
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let first = common::event_page(&h, &token_a, 0).await;
    let first_events = first["events"].as_array().unwrap();
    assert_eq!(first_events.len(), 100);
    assert_eq!(first_events.first().unwrap()["sequence_no"], 1);
    assert_eq!(first_events.last().unwrap()["sequence_no"], 100);
    assert_eq!(first["next_cursor"], 100);
    assert!(
        first_events
            .iter()
            .all(|event| event["match_token"] != "only-b")
    );

    let second = common::event_page(&h, &token_a, first["next_cursor"].as_i64().unwrap()).await;
    let second_events = second["events"].as_array().unwrap();
    assert_eq!(second_events.len(), 1);
    assert_eq!(second_events[0]["sequence_no"], 101);
    assert_eq!(second_events[0]["payload"]["ordinal"], 100);
    assert_eq!(second["next_cursor"], 101);

    let b_page = common::event_page(&h, &token_b, 0).await;
    let b_events = b_page["events"].as_array().unwrap();
    assert_eq!(b_events.len(), 1);
    assert_eq!(b_events[0]["match_token"], "only-b");
    assert_eq!(b_events[0]["sequence_no"], 1);

    let active = common::ranked_state(&h, &token_a).await;
    assert!(active["queue"].is_null());
    assert_eq!(active["active_match"]["match_token"], match_token);
    assert_eq!(active["active_match"]["opponent"], b.to_string());
    assert_eq!(active["active_match"]["attempt_id"], attempt_id.to_string());
    assert_eq!(active["active_match"]["role"], "create");
    assert_eq!(active["cursor"], 101);
    let active_receipts = active["receipts"].as_array().unwrap();
    assert_eq!(active_receipts.len(), 1);
    assert_eq!(active_receipts[0]["status"], "applied");
    assert_eq!(active_receipts[0]["session_kind"], "native");

    let queued = common::ranked_state(&h, &queued_token).await;
    assert_eq!(queued["queue"]["mode"], "umvc3_1v1");
    assert_eq!(queued["queue"]["difficulty"], "hard");
    assert!(queued["active_match"].is_null());
    assert_eq!(queued["cursor"], 0);
    let queued_receipts = queued["receipts"].as_array().unwrap();
    assert_eq!(queued_receipts.len(), 1);
    assert_eq!(queued_receipts[0]["status"], "applied");
    assert_ne!(
        queued_receipts[0]["receipt"], active_receipts[0]["receipt"],
        "a snapshot must never leak another user's receipt"
    );
}

#[sqlx::test]
async fn ticker_applies_without_temporal_and_socket_close_preserves_match(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, ranked_config()).await;
    let (a, _, token_a) = h.native_principal(97401).await;
    let (b, _, _) = h.native_principal(97402).await;

    let queue_id = Uuid::new_v4();
    let admitted = common::post_command(
        &h,
        &token_a,
        json!({"command_id":queue_id,"type":"queue","mode":"umvc3_1v1","difficulty":"easy"}),
    )
    .await;
    assert_eq!(admitted.status(), StatusCode::ACCEPTED);
    let queue_receipt = admitted.json::<Value>().await.unwrap()["receipt"].clone();

    let applied = timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = common::ranked_state(&h, &token_a).await;
            let receipt_applied = snapshot["receipts"].as_array().is_some_and(|receipts| {
                receipts.iter().any(|receipt| {
                    receipt["receipt"] == queue_receipt && receipt["status"] == "applied"
                })
            });
            if receipt_applied && snapshot["queue"]["difficulty"] == "easy" {
                break snapshot;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ticker applies an admitted command without a Temporal signal");
    assert_eq!(applied["queue"]["mode"], "umvc3_1v1");

    let cancel = common::post_command(
        &h,
        &token_a,
        json!({"command_id":Uuid::new_v4(),"type":"cancel_queue"}),
    )
    .await;
    assert_eq!(cancel.status(), StatusCode::ACCEPTED);
    timeout(Duration::from_secs(5), async {
        loop {
            if common::ranked_state(&h, &token_a).await["queue"].is_null() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("ticker applies queue cancellation");

    let (match_token, _) = seed_match(&h, a, b, "AwaitingAccepted", "1 hour").await;
    let mut socket = connect_native(&h, &token_a).await;
    let ws_command = Uuid::new_v4();
    let receipt = send_ws_command(
        &mut socket,
        json!({"type":"ranked_heartbeat","command_id":ws_command}),
    )
    .await;
    assert_eq!(receipt["type"], "command_receipt");
    socket.close(None).await.expect("close ranked websocket");

    timeout(Duration::from_secs(3), async {
        loop {
            if !h.state.connections.lock().await.contains_key(&a) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("server observes websocket close");

    let snapshot = common::ranked_state(&h, &token_a).await;
    assert_eq!(snapshot["active_match"]["match_token"], match_token);
    assert_eq!(snapshot["active_match"]["phase"], "AwaitingAccepted");
    let player: (String, Option<String>) =
        sqlx::query_as("SELECT state,active_match_token FROM player_state WHERE user_id=$1")
            .bind(a)
            .fetch_one(&h.pool)
            .await
            .unwrap();
    assert_eq!(player, ("MatchAccepted".into(), Some(match_token)));
    let ws_session: (String, Uuid) = sqlx::query_as(
        "SELECT session_kind,session_id FROM command_inbox WHERE command_id=$1",
    )
    .bind(ws_command)
    .fetch_one(&h.pool)
    .await
    .expect("websocket command persisted under its connection UUID");
    assert_eq!(ws_session.0, "websocket");
    assert_ne!(ws_session.1, Uuid::nil());
}
