//! Type-safe WebSocket client for the Steam Lobby matchmaking service.
//!
//! ```no_run
//! use lobby_client::LobbyClient;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut client = LobbyClient::connect("ws://localhost:8080/ws").await?;
//!
//! // Authenticate
//! let auth = client.authenticate("your-jwt-token").await?;
//! println!("Logged in as {} ({})", auth.display_name, auth.steam_id);
//!
//! // Enter queue
//! client.begin_matchmaking("ranked_1v1", "normal").await?;
//!
//! // Wait for a match
//! if let Some(m) = client.wait_for_match().await? {
//!     println!("Match found: {}", m.match_token);
//!     client.accept_match(&m.match_token).await?;
//!     client.p2p_connected(&m.match_token).await?;
//!     // ... play the game ...
//!     client.submit_report(&m.match_token, Some(auth.steam_id), None).await?;
//! }
//! # Ok(())
//! # }
//! ```

use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::Message;

// ── Wire types (mirror server's ClientMessage / ServerMessage) ──

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMsg {
    Auth { session_token: String },
    BeginMatchmaking { mode: String, difficulty: String },
    CancelMatchmaking,
    AcceptMatch { match_token: String },
    DeclineMatch { match_token: String },
    P2pConnected { match_token: String },
    MatchReport { match_token: String, winner: Option<u64>, demo_hash: Option<String> },
}

// Hand-written so session tokens never appear in logs: the JWT is a
// credential; match_token/winner/demo_hash are not.
impl std::fmt::Debug for ClientMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientMsg::Auth { .. } => {
                f.debug_struct("Auth").field("session_token", &"<redacted>").finish()
            }
            ClientMsg::BeginMatchmaking { mode, difficulty } => f
                .debug_struct("BeginMatchmaking")
                .field("mode", mode)
                .field("difficulty", difficulty)
                .finish(),
            ClientMsg::CancelMatchmaking => f.write_str("CancelMatchmaking"),
            ClientMsg::AcceptMatch { match_token } => f
                .debug_struct("AcceptMatch")
                .field("match_token", match_token)
                .finish(),
            ClientMsg::DeclineMatch { match_token } => f
                .debug_struct("DeclineMatch")
                .field("match_token", match_token)
                .finish(),
            ClientMsg::P2pConnected { match_token } => f
                .debug_struct("P2pConnected")
                .field("match_token", match_token)
                .finish(),
            ClientMsg::MatchReport { match_token, winner, demo_hash } => f
                .debug_struct("MatchReport")
                .field("match_token", match_token)
                .field("winner", winner)
                .field("demo_hash", demo_hash)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    #[serde(rename = "auth_ok")]
    AuthOk { steam_id: u64, display_name: String },
    #[serde(rename = "match_found")]
    MatchFound { match_token: String, opponent: OpponentInfo, timeout_ms: u64 },
    Error { message: String },
}

#[derive(Debug, Clone, Deserialize)]
pub struct OpponentInfo {
    pub steam_id: u64,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct AuthOk {
    pub steam_id: u64,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct MatchFound {
    pub match_token: String,
    pub opponent: OpponentInfo,
    pub timeout_ms: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("connection error: {0}")]
    Connection(String),
    #[error("server error: {0}")]
    Server(String),
    #[error("serialization error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("no response received")]
    NoResponse,
    #[error("unexpected message type: expected auth_ok, got {0:?}")]
    UnexpectedResponse(ServerEvent),
    #[error("channel closed")]
    ChannelClosed,
}

// ── Client ──

pub struct LobbyClient {
    tx: mpsc::UnboundedSender<String>,
    rx: mpsc::UnboundedReceiver<Result<ServerEvent, ClientError>>,
}

impl LobbyClient {
    /// Connect to a Steam Lobby server. Returns immediately — I/O runs in a background task.
    pub async fn connect(url: &str) -> Result<Self, ClientError> {
        // wss:// is required for non-loopback servers — the JWT is sent in the
        // first WS frame and must not travel cleartext.
        if let Ok(parsed) = url::Url::parse(url) {
            match parsed.scheme() {
                "wss" => {}
                "ws" => {
                    let loopback = parsed.host_str().is_some_and(|h| {
                        h == "localhost" || h == "127.0.0.1" || h == "::1" || h == "[::1]"
                    });
                    if !loopback {
                        return Err(ClientError::Connection(
                            "wss:// is required for non-loopback servers".into(),
                        ));
                    }
                }
                other => {
                    return Err(ClientError::Connection(format!(
                        "unsupported scheme: {other}"
                    )));
                }
            }
        }
        let (ws, _) = connect_async(url)
            .await
            .map_err(|e| ClientError::Connection(e.to_string()))?;

        let (mut ws_tx, mut ws_rx) = ws.split();

        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<String>();
        let (incoming_tx, incoming_rx) = mpsc::unbounded_channel::<Result<ServerEvent, ClientError>>();

        // Outbound task: send JSON frames from the channel to the socket
        tokio::spawn(async move {
            while let Some(msg) = outgoing_rx.recv().await {
                if ws_tx.send(Message::Text(msg.into())).await.is_err() {
                    break;
                }
            }
        });

        // Inbound task: parse incoming frames and push to channel
        tokio::spawn(async move {
            while let Some(msg) = ws_rx.next().await {
                match msg {
                    Ok(Message::Text(text)) => {
                        match serde_json::from_str::<ServerEvent>(&text) {
                            Ok(event) => {
                                if incoming_tx.send(Ok(event)).is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                let _ = incoming_tx.send(Err(ClientError::Json(e)));
                            }
                        }
                    }
                    Ok(Message::Close(_)) => break,
                    Err(e) => {
                        let _ = incoming_tx.send(Err(ClientError::Connection(e.to_string())));
                        break;
                    }
                    _ => {} // ignore ping/pong/binary
                }
            }
        });

        Ok(Self {
            tx: outgoing_tx,
            rx: incoming_rx,
        })
    }

    fn send(&self, msg: ClientMsg) -> Result<(), ClientError> {
        let text = serde_json::to_string(&msg)?;
        self.tx.send(text).map_err(|_| ClientError::ChannelClosed)
    }

    /// Authenticate with a JWT session token. Returns player info on success.
    pub async fn authenticate(&mut self, token: &str) -> Result<AuthOk, ClientError> {
        self.send(ClientMsg::Auth { session_token: token.to_string() })?;
        match self.rx.recv().await.ok_or(ClientError::NoResponse)? {
            Ok(ServerEvent::AuthOk { steam_id, display_name }) => {
                Ok(AuthOk { steam_id, display_name })
            }
            Ok(ServerEvent::Error { message }) => Err(ClientError::Server(message)),
            Ok(other) => Err(ClientError::UnexpectedResponse(other)),
            Err(e) => Err(e),
        }
    }

    /// Dev-only: POST /auth/test-token to get a JWT, then authenticate over WebSocket.
    /// Only works when the server runs with `AUTH_DEV_MODE=true`.
    pub async fn authenticate_test_token(
        &mut self,
        steam_id: u64,
        base_url: &str,
    ) -> Result<AuthOk, ClientError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|e| ClientError::Connection(e.to_string()))?;
        let resp = client
            .post(format!("{base_url}/auth/test-token"))
            .json(&serde_json::json!({"steam_id": steam_id}))
            .send()
            .await
            .map_err(|e| ClientError::Connection(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(ClientError::Server(format!("HTTP {}", resp.status())));
        }
        let body: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ClientError::Connection(e.to_string()))?;
        let token = body["token"]
            .as_str()
            .ok_or_else(|| ClientError::Server("no token in response".into()))?;
        self.authenticate(token).await
    }

    /// Enter the matchmaking queue.
    pub async fn begin_matchmaking(&mut self, mode: &str, difficulty: &str) -> Result<(), ClientError> {
        self.send(ClientMsg::BeginMatchmaking {
            mode: mode.to_string(),
            difficulty: difficulty.to_string(),
        })
    }

    /// Leave the queue (no server response expected).
    pub async fn cancel_matchmaking(&mut self) -> Result<(), ClientError> {
        self.send(ClientMsg::CancelMatchmaking)
    }

    /// Accept a found match.
    pub async fn accept_match(&mut self, match_token: &str) -> Result<(), ClientError> {
        self.send(ClientMsg::AcceptMatch { match_token: match_token.to_string() })
    }

    /// Decline a found match.
    pub async fn decline_match(&mut self, match_token: &str) -> Result<(), ClientError> {
        self.send(ClientMsg::DeclineMatch { match_token: match_token.to_string() })
    }

    /// Notify the server that the P2P connection to the opponent is established.
    pub async fn p2p_connected(&mut self, match_token: &str) -> Result<(), ClientError> {
        self.send(ClientMsg::P2pConnected { match_token: match_token.to_string() })
    }

    /// Submit a match result. `winner` is the victor's steam_id; `None` for a draw.
    pub async fn submit_report(
        &mut self,
        match_token: &str,
        winner: Option<u64>,
        demo_hash: Option<&str>,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::MatchReport {
            match_token: match_token.to_string(),
            winner,
            demo_hash: demo_hash.map(|s| s.to_string()),
        })
    }

    /// Wait for the next server event. Returns `None` if the connection is closed.
    pub async fn next_event(&mut self) -> Option<Result<ServerEvent, ClientError>> {
        self.rx.recv().await
    }

    /// Block until a MatchFound event is received (skipping non-match events).
    pub async fn wait_for_match(&mut self) -> Result<Option<MatchFound>, ClientError> {
        loop {
            match self.rx.recv().await {
                Some(Ok(ServerEvent::MatchFound { match_token, opponent, timeout_ms })) => {
                    return Ok(Some(MatchFound { match_token, opponent, timeout_ms }));
                }
                Some(Ok(ServerEvent::Error { message })) => {
                    return Err(ClientError::Server(message));
                }
                Some(Err(e)) => return Err(e),
                Some(_) => continue, // skip other events while waiting
                None => return Ok(None),
            }
        }
    }
}
