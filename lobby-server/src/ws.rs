use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use lobby_core::traits::{MatchStore, PlayerStore, QueueStore};
use lobby_core::types::{MatchDifficulty, MatchReport, SteamId};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

use crate::state::{AppState, ConnectionEntry};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Auth {
        session_token: String,
    },
    AuthTicket {
        ticket: String,
    },
    BeginMatchmaking {
        mode: String,
        difficulty: String,
    },
    CancelMatchmaking,
    AcceptMatch {
        match_token: String,
    },
    DeclineMatch {
        match_token: String,
    },
    P2pConnected {
        match_token: String,
    },
    MatchReport {
        match_token: String,
        #[serde(default, deserialize_with = "lobby_core::types::deserialize_optional_steam_id")]
        winner: Option<u64>,
        demo_hash: Option<String>,
    },
    Heartbeat,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Outbound messages sent over WebSocket connections.
#[allow(dead_code)] // QueueStatus, MatchAccepted, MatchStarted reserved for future use
pub enum ServerMessage {
    AuthOk {
        #[serde(serialize_with = "lobby_core::types::serialize_steam_id")]
        steam_id: u64,
        display_name: String,
        state: lobby_core::types::PlayerState,
    },
    QueueStatus {
        elapsed_ms: u64,
        band_lo: f64,
        band_hi: f64,
        candidates: u32, // other queued players whose mu falls in [band_lo, band_hi]
        queue_size: u32, // total queued in the mode
        my_mu: f64,
        my_sigma: f64,
        my_rating: f64, // mu - 3*sigma
        leaderboard: Vec<lobby_core::types::LeaderboardEntry>,
    },
    OpponentConnected {
        match_token: String,
    },
    ReportReceived {
        match_token: String,
        #[serde(serialize_with = "lobby_core::types::serialize_steam_id")]
        reporting_player: u64,
        #[serde(serialize_with = "lobby_core::types::serialize_optional_steam_id")]
        winner: Option<u64>,
        demo_hash: Option<String>,
    },
    MatchFound {
        match_token: String,
        opponent: OpponentInfo,
        timeout_ms: u64,
    },
    MatchAccepted {
        match_token: String,
        opponent_steam_id: u64,
    },
    MatchStarted {
        match_token: String,
    },
    MatchResult {
        match_token: String,
        outcome: serde_json::Value, // MatchOutcome serialized
    },
    MatchDeclined {
        match_token: String,
    },
    MatchExpired {
        match_token: String,
    },
    QueueExpired,
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Serialize, Clone)]
pub struct OpponentInfo {
    #[serde(serialize_with = "lobby_core::types::serialize_steam_id")]
    pub steam_id: u64,
    pub display_name: String,
}

pub async fn handle_ws(ws: WebSocket, state: Arc<AppState>, peer_ip: std::net::SocketAddr) {
    let (mut sender, mut receiver) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();

    // Auth phase
    let steam_id = match authenticate(&mut receiver, &mut sender, &state, peer_ip).await {
        Ok(id) => id,
        Err(_) => return,
    };

    // Send auth ok — include the player's persisted state so a reconnecting
    // client knows it was in the queue, mid-match, etc.
    let display_name = state
        .steam_auth
        .get_player_summary(steam_id)
        .await
        .unwrap_or_else(|_| "Unknown".into());
    let player_state = state
        .store
        .get_player_state(steam_id)
        .await
        .ok()
        .flatten()
        .map(|p| p.state)
        .unwrap_or(lobby_core::types::PlayerState::InMenus);
    let _ = sender
        .send(Message::Text(
            serde_json::to_string(&ServerMessage::AuthOk {
                steam_id,
                display_name: display_name.clone(),
                state: player_state,
            })
            .unwrap()
            .into(),
        ))
        .await;

    tracing::info!("player {steam_id} connected from {peer_ip} ({display_name})");

    // Enter menus
    let _ = state
        .player_manager
        .enter_menus(steam_id, &state.store)
        .await;

    // Spawn the outbound message forwarder FIRST so the map entry can hold an
    // abort handle and kill a ghosted connection.
    let mut outbound_task = tokio::spawn(async move {
        while let Some(server_msg) = rx.recv().await {
            let json = serde_json::to_string(&server_msg).unwrap();
            if sender.send(Message::Text(json.into())).await.is_err() {
                break; // Socket closed
            }
        }
    });
    let outbound_abort = outbound_task.abort_handle();

    // Register connection — replace any ghost of this player.
    let my_gen = state
        .next_generation
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    {
        let mut connections = state.connections.lock().await;
        if let Some(old) = connections.insert(
            steam_id,
            ConnectionEntry {
                tx: tx.clone(),
                generation: my_gen,
                abort: outbound_abort,
            },
        ) {
            let _ = old.tx.send(ServerMessage::Error {
                code: "replaced".into(),
                message: "Newer connection established".into(),
            });
            old.abort.abort(); // kills the old socket; its cleanup sees a stale generation
        }
    }

    // Message loop — select between client messages and outbound task
    // Set on every break path below; overridden to "replaced…" for ghosts.
    let mut disconnect_reason;
    loop {
        tokio::select! {
            msg = timeout(Duration::from_secs(60), receiver.next()) => {
                match msg {
                    Ok(Some(Ok(Message::Text(text)))) => {
                        let cm: ClientMessage = match serde_json::from_str(&text) {
                            Ok(m) => m,
                            Err(_) => {
                                let _ = tx.send(ServerMessage::Error {
                                    code: "invalid_message".into(),
                                    message: "Could not parse message".into(),
                                });
                                continue;
                            }
                        };
                        handle_client_message(
                            cm,
                            steam_id,
                            &state,
                            &tx,
                        )
                        .await;
                    }
                    Ok(Some(Ok(Message::Close(_)))) => {
                        disconnect_reason = "close frame";
                        break;
                    }
                    Ok(Some(Err(e))) => {
                        tracing::debug!("ws read error from {steam_id}: {e}");
                        disconnect_reason = "socket error";
                        break;
                    }
                    Err(_) => {
                        // No frames at all for 60s (an idle client) — drop it.
                        disconnect_reason = "idle timeout (60s)";
                        break;
                    }
                    _ => {
                        // Ping/pong or other — ignore
                    }
                }
            }
            _ = &mut outbound_task => {
                // Outbound task died (socket closed, send-side error)
                disconnect_reason = "peer disconnected (send side)";
                break;
            }
        }
    }
    let is_current = {
        let connections = state.connections.lock().await;
        connections
            .get(&steam_id)
            .map(|e| e.generation == my_gen)
            .unwrap_or(false)
    };
    if is_current {
        let _ = state
            .player_manager
            .handle_disconnect(steam_id, &state.store)
            .await;
        state.connections.lock().await.remove(&steam_id);
    } else {
        disconnect_reason = "replaced by newer connection";
    }
    tracing::info!("player {steam_id} disconnected ({disconnect_reason})");
    outbound_task.abort();
}

async fn authenticate(
    receiver: &mut (impl StreamExt<Item = Result<Message, axum::Error>> + Unpin),
    sender: &mut (impl SinkExt<Message, Error = axum::Error> + Unpin),
    state: &Arc<AppState>,
    peer_ip: std::net::SocketAddr,
) -> Result<SteamId, ()> {
    let first_msg = timeout(Duration::from_secs(10), receiver.next()).await;
    let text = match first_msg {
        Ok(Some(Ok(Message::Text(t)))) => t.to_string(),
        _ => {
            tracing::warn!("auth failed (no auth message within 10s) from {peer_ip}");
            let _ = sender
                .send(Message::Text(
                    serde_json::to_string(&ServerMessage::Error {
                        code: "auth_required".into(),
                        message: "First message must be auth".into(),
                    })
                    .unwrap()
                    .into(),
                ))
                .await;
            return Err(());
        }
    };

    let cm: ClientMessage = match serde_json::from_str(&text) {
        Ok(m) => m,
        Err(_) => {
            tracing::warn!("auth failed (unparseable first message) from {peer_ip}");
            let _ = sender
                .send(Message::Text(
                    serde_json::to_string(&ServerMessage::Error {
                        code: "invalid_message".into(),
                        message: "Could not parse message".into(),
                    })
                    .unwrap()
                    .into(),
                ))
                .await;
            return Err(());
        }
    };

    match cm {
        ClientMessage::Auth { session_token } => {
            let (id, ver) = match state.steam_auth.validate_session_token(&session_token) {
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!("auth failed (invalid session token) from {peer_ip}");
                    return Err(());
                }
            };
            // A token minted before a logout (or a DB error) is rejected.
            let db_ver = match state.store.get_token_version(id).await {
                Ok(v) => v,
                Err(_) => {
                    tracing::warn!("auth failed (token version lookup error) from {peer_ip}");
                    return Err(());
                }
            };
            if db_ver != ver {
                tracing::warn!("auth failed (revoked or outdated token) from {peer_ip}");
                return Err(());
            }
            Ok(id)
        }
        ClientMessage::AuthTicket { ticket } => {
            if !state.ticket_limiter.check(peer_ip.ip()) {
                tracing::warn!("auth failed (ticket rate-limited) from {peer_ip}");
                let _ = sender
                    .send(Message::Text(
                        serde_json::to_string(&ServerMessage::Error {
                            code: "rate_limited".into(),
                            message: "Too many auth attempts".into(),
                        })
                        .unwrap()
                        .into(),
                    ))
                    .await;
                return Err(());
            }
            match state.steam_auth.verify_ticket(&ticket).await {
                Ok(id) => Ok(id),
                Err(_) => {
                    tracing::warn!("auth failed (invalid ticket) from {peer_ip}");
                    Err(())
                }
            }
        }
        _ => {
            tracing::warn!("auth failed (first message was not auth) from {peer_ip}");
            let _ = sender
                .send(Message::Text(
                    serde_json::to_string(&ServerMessage::Error {
                        code: "auth_required".into(),
                        message: "First message must be auth".into(),
                    })
                    .unwrap()
                    .into(),
                ))
                .await;
            Err(())
        }
    }
}


async fn handle_client_message(
    cm: ClientMessage,
    steam_id: SteamId,
    state: &Arc<AppState>,
    tx: &mpsc::UnboundedSender<ServerMessage>,
) {
    match cm {
        ClientMessage::BeginMatchmaking { mode, difficulty } => {
            let diff = match difficulty.as_str() {
                "easy" => MatchDifficulty::Easy,
                "hard" => MatchDifficulty::Hard,
                _ => MatchDifficulty::Normal,
            };
            let _ = state
                .player_manager
                .begin_matchmaking(steam_id, diff, &state.store)
                .await;

            // Create queue entry
            let rating = <dyn lobby_core::traits::RatingStore>::get_rating(&state.store, steam_id, &mode)
                .await
                .unwrap_or(
                    lobby_core::types::OpenSkillRating {
                        mu: 25.0,
                        sigma: 25.0 / 3.0,
                        last_updated: chrono::Utc::now(),
                    },
                );

            tracing::info!("player {steam_id} entered queue ({mode}, {difficulty})");
            let entry = lobby_core::types::QueueEntry {
                steam_id,
                game_mode: mode,
                difficulty: diff,
                mu: rating.mu,
                queued_at: chrono::Utc::now(),
            };
            let _ = state.store.enqueue(&entry).await;
        }
        ClientMessage::CancelMatchmaking => {
            let _ = state
                .player_manager
                .cancel_matchmaking(steam_id, &state.store)
                .await;
            let _ = state.store.dequeue(steam_id, "ranked_1v1").await;
            tracing::info!("player {steam_id} left queue (cancelled)");
        }
        ClientMessage::AcceptMatch { match_token } => {
            match state
                .match_manager
                .accept_match(&match_token, steam_id, &state.store)
                .await
            {
                Ok(()) => tracing::info!("player {steam_id} accepted match {match_token}"),
                Err(e) => {
                    tracing::warn!("player {steam_id} accept rejected for match {match_token}: {e}")
                }
            }
        }
        ClientMessage::DeclineMatch { match_token } => {
            tracing::info!("player {steam_id} declined match {match_token}");
            let _ = tx.send(ServerMessage::MatchDeclined {
                match_token: match_token.clone(),
            });
            if let Ok(Some(m)) = state.store.get_match(&match_token).await {
                let other = if steam_id == m.player_a { m.player_b } else { m.player_a };
                let connections = state.connections.lock().await;
                if let Some(e) = connections.get(&other) {
                    let _ = e.tx.send(ServerMessage::MatchDeclined { match_token });
                }
            }
        }
        ClientMessage::P2pConnected { match_token } => {
            match state
                .match_manager
                .mark_connected(&match_token, steam_id, &state.store)
                .await
            {
                Ok(()) => {
                    if let Ok(Some(m)) = state.store.get_match(&match_token).await {
                        let other = if steam_id == m.player_a { m.player_b } else { m.player_a };
                        let connections = state.connections.lock().await;
                        if let Some(e) = connections.get(&other) {
                            let _ = e.tx.send(ServerMessage::OpponentConnected { match_token });
                        }
                    }
                }
                Err(e) => {
                    tracing::warn!("player {steam_id} p2p signal rejected for match {match_token}: {e}")
                }
            }
        }
        ClientMessage::MatchReport {
            match_token,
            winner,
            demo_hash,
        } => {
            let report = MatchReport {
                match_token: match_token.clone(),
                reporting_player: steam_id,
                winner,
                demo_hash,
            };
            let outcome = state
                .match_manager
                .submit_report(report.clone(), &state.store, &state.store)
                .await;

            match &outcome {
                Ok(_) => tracing::info!(
                    "report from {steam_id} for match {match_token}: winner {:?}",
                    report.winner
                ),
                Err(e) => tracing::warn!("report from {steam_id} for match {match_token} rejected: {e}"),
            }
            // Broadcast the received report to BOTH players so each sees what the
            // other selected before resolution. Gated on the report actually being
            // stored (Ok) — a rejected report (wrong state/participant) is not
            // presented as a fact.
            if outcome.is_ok() {
                if let Ok(Some(m)) = state.store.get_match(&match_token).await {
                    let connections = state.connections.lock().await;
                    for pid in [m.player_a, m.player_b] {
                        if let Some(e) = connections.get(&pid) {
                            let _ = e.tx.send(ServerMessage::ReportReceived {
                                match_token: match_token.clone(),
                                reporting_player: steam_id,
                                winner: report.winner,
                                demo_hash: report.demo_hash.clone(),
                            });
                        }
                    }
                }
            }
            // On a REAL resolution (both reports processed — match terminal),
            // tell BOTH players. The first report alone returns Ok(Disputed)
            // while awaiting the opponent's, so gate on match status, not on Ok.
            let terminal = match state.store.get_match(&match_token).await {
                Ok(Some(m)) => {
                    matches!(m.status, lobby_core::types::MatchStatus::Resolved | lobby_core::types::MatchStatus::Disputed)
                }
                _ => false,
            };
            if terminal {
                if let Ok(outcome) = outcome {
                    tracing::info!("match {match_token} resolved: {outcome:?}");
                    if let Ok(Some(m)) = state.store.get_match(&match_token).await {
                        let connections = state.connections.lock().await;
                        for pid in [m.player_a, m.player_b] {
                            if let Some(e) = connections.get(&pid) {
                                let _ = e.tx.send(ServerMessage::MatchResult {
                                    match_token: match_token.clone(),
                                    outcome: serde_json::to_value(&outcome).unwrap(),
                                });
                            }
                        }
                    }
                }
            }
        }
        ClientMessage::Heartbeat => {
            let _ = state.player_manager.heartbeat(steam_id, &state.store).await;
        }
        _ => {}
    }
}
