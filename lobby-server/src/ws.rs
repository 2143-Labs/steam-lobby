use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures_util::{SinkExt, StreamExt};
use lobby_core::traits::{QueueStore, RatingStore};
use lobby_core::types::{MatchDifficulty, MatchReport, SteamId};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

use crate::state::AppState;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ClientMessage {
    Auth { session_token: String },
    AuthTicket { ticket: String },
    BeginMatchmaking { mode: String, difficulty: String },
    CancelMatchmaking,
    AcceptMatch { match_token: String },
    DeclineMatch { match_token: String },
    P2pConnected { match_token: String },
    MatchReport {
        match_token: String,
        winner: Option<u64>,
        demo_hash: Option<String>,
    },
    Heartbeat,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerMessage {
    AuthOk {
        steam_id: u64,
        display_name: String,
    },
    QueueStatus {
        state: String,
        elapsed_ms: u64,
        mmr_band: f64,
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
    Error {
        code: String,
        message: String,
    },
}

#[derive(Debug, Serialize, Clone)]
pub struct OpponentInfo {
    pub steam_id: u64,
    pub display_name: String,
}

pub async fn handle_ws(ws: WebSocket, state: Arc<AppState>) {
    let (mut sender, mut receiver) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();

    // Auth phase
    let steam_id = match authenticate(&mut receiver, &mut sender, &state).await {
        Ok(id) => id,
        Err(_) => return,
    };

    // Register connection
    {
        let mut connections = state.connections.lock().await;
        if let Some(old_tx) = connections.insert(steam_id, tx.clone()) {
            let _ = old_tx.send(ServerMessage::Error {
                code: "replaced".into(),
                message: "Newer connection established".into(),
            });
        }
    }

    // Send auth ok
    let display_name = state
        .steam_auth
        .get_player_summary(steam_id)
        .await
        .unwrap_or_else(|_| "Unknown".into());
    let _ = sender
        .send(Message::Text(
            serde_json::to_string(&ServerMessage::AuthOk {
                steam_id,
                display_name: display_name.clone(),
            })
            .unwrap()
            .into(),
        ))
        .await;

    // Enter menus
    let _ = state
        .player_manager
        .enter_menus(steam_id, &state.store)
        .await;

    // Spawn outbound message forwarder — takes ownership of sender
    let mut outbound_task = tokio::spawn(async move {
        while let Some(server_msg) = rx.recv().await {
            let json = serde_json::to_string(&server_msg).unwrap();
            if sender.send(Message::Text(json.into())).await.is_err() {
                break; // Socket closed
            }
        }
    });

    // Message loop — select between client messages and outbound task
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
                    Ok(Some(Ok(Message::Close(_)))) | Err(_) => {
                        // Disconnect or timeout
                        break;
                    }
                    _ => {
                        // Ping/pong or other — ignore
                    }
                }
            }
            _ = &mut outbound_task => {
                // Outbound task died (socket closed, send-side error)
                break;
            }
        }
    }

    // Cleanup
    let _ = state.player_manager.handle_disconnect(steam_id, &state.store).await;
    let mut connections = state.connections.lock().await;
    connections.remove(&steam_id);
    outbound_task.abort();
}

async fn authenticate(
    receiver: &mut (impl StreamExt<Item = Result<Message, axum::Error>> + Unpin),
    sender: &mut (impl SinkExt<Message, Error = axum::Error> + Unpin),
    state: &Arc<AppState>,
) -> Result<SteamId, ()> {
    let first_msg = timeout(Duration::from_secs(10), receiver.next()).await;
    let text = match first_msg {
        Ok(Some(Ok(Message::Text(t)))) => t.to_string(),
        _ => {
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
            state.steam_auth.validate_session_token(&session_token).map_err(|_| ())
        }
        ClientMessage::AuthTicket { ticket } => {
            state.steam_auth.verify_ticket(&ticket).await.map_err(|_| ())
        }
        _ => {
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
            let rating = state
                .store
                .get_rating(steam_id, &mode)
                .await
                .unwrap_or(lobby_core::types::OpenSkillRating {
                    mu: 25.0,
                    sigma: 25.0 / 3.0,
                    last_updated: chrono::Utc::now(),
                });

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
            let _ = state.player_manager.cancel_matchmaking(steam_id, &state.store).await;
            let _ = state.store.dequeue(steam_id, "ranked_1v1").await;
        }
        ClientMessage::AcceptMatch { match_token } => {
            let _ = state
                .match_manager
                .accept_match(&match_token, steam_id, &state.store)
                .await;
        }
        ClientMessage::DeclineMatch { match_token } => {
            let _ = tx.send(ServerMessage::MatchDeclined { match_token });
        }
        ClientMessage::P2pConnected { match_token } => {
            let _ = state
                .match_manager
                .mark_connected(&match_token, steam_id, &state.store)
                .await;
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
                .submit_report(report, &state.store, &state.store)
                .await;
            if let Ok(outcome) = outcome {
                let _ = tx.send(ServerMessage::MatchResult {
                    match_token,
                    outcome: serde_json::to_value(&outcome).unwrap(),
                });
            }
        }
        ClientMessage::Heartbeat => {
            let _ = state.player_manager.heartbeat(steam_id, &state.store).await;
        }
        _ => {}
    }
}
