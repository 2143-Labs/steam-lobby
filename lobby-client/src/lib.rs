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
//! println!("Logged in as {} ({})", auth.display_name, auth.player_id);
//!
//! // Enter queue
//! client.begin_matchmaking("ranked_1v1", "normal").await?;
//!
//! // Wait for a match
//! if let Some(m) = client.wait_for_match().await? {
//!     println!("Match found: {}", m.match_token);
//!     client.accept_match(&m.match_token).await?;
//!     client.start_match(&m.match_token).await?;
//!     // ... play the game ...
//!     client.submit_report(&m.match_token, Some(&auth.player_id), None).await?;
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
    Auth {
        session_token: String,
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
    StartMatch {
        match_token: String,
    },
    GameInput {
        match_token: String,
        frame: u32,
        target: String,
    },
    RollbackHealth {
        match_token: String,
        frame: u32,
        checksum: String,
    },
    MatchReport {
        match_token: String,
        winner: Option<String>,
        demo_hash: Option<String>,
    },
    Heartbeat,
    WebrtcOffer {
        match_token: String,
        sdp: String,
    },
    WebrtcAnswer {
        match_token: String,
        sdp: String,
    },
    WebrtcIce {
        match_token: String,
        candidate: String,
    },
}

// Hand-written so session tokens never appear in logs: the JWT is a
// credential; match_token/winner/demo_hash are not.
impl std::fmt::Debug for ClientMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientMsg::Auth { .. } => f
                .debug_struct("Auth")
                .field("session_token", &"<redacted>")
                .finish(),
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
            ClientMsg::StartMatch { match_token } => f
                .debug_struct("StartMatch")
                .field("match_token", match_token)
                .finish(),
            ClientMsg::GameInput {
                match_token,
                frame,
                target,
            } => f
                .debug_struct("GameInput")
                .field("match_token", match_token)
                .field("frame", frame)
                .field("target", target)
                .finish(),
            ClientMsg::RollbackHealth {
                match_token,
                frame,
                checksum,
            } => f
                .debug_struct("RollbackHealth")
                .field("match_token", match_token)
                .field("frame", frame)
                .field("checksum", checksum)
                .finish(),
            ClientMsg::MatchReport {
                match_token,
                winner,
                demo_hash,
            } => f
                .debug_struct("MatchReport")
                .field("match_token", match_token)
                .field("winner", winner)
                .field("demo_hash", demo_hash)
                .finish(),
            ClientMsg::Heartbeat => f.write_str("Heartbeat"),
            ClientMsg::WebrtcOffer {
                match_token,
                sdp: _,
            } => f
                .debug_struct("WebrtcOffer")
                .field("match_token", match_token)
                .field("sdp", &"<sdp>")
                .finish(),
            ClientMsg::WebrtcAnswer {
                match_token,
                sdp: _,
            } => f
                .debug_struct("WebrtcAnswer")
                .field("match_token", match_token)
                .field("sdp", &"<sdp>")
                .finish(),
            ClientMsg::WebrtcIce {
                match_token,
                candidate: _,
            } => f
                .debug_struct("WebrtcIce")
                .field("match_token", match_token)
                .field("candidate", &"<candidate>")
                .finish(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ServerEvent {
    #[serde(rename = "auth_ok")]
    AuthOk {
        player_id: String,
        display_name: String,
        state: lobby_core::types::PlayerState,
    },
    #[serde(rename = "match_found")]
    MatchFound {
        match_token: String,
        opponent: OpponentInfo,
        timeout_ms: u64,
        game_type: lobby_core::types::GameType,
    },
    #[serde(rename = "game_server_ready")]
    GameServerReady {
        match_token: String,
        address: String,
        join_token: Option<String>,
    },
    #[serde(rename = "game_server_error")]
    GameServerError {
        match_token: String,
        message: String,
    },
    GameState {
        match_token: String,
        frame: u32,
        player_a: String,
        player_b: String,
        left_y: f64,
        right_y: f64,
        ball_x: f64,
        ball_y: f64,
        left_score: u8,
        right_score: u8,
        speed: f64,
        checksum: String,
    },
    #[serde(rename = "input_ack")]
    InputAck {
        match_token: String,
        frame: u32,
    },
    #[serde(rename = "match_started")]
    MatchStarted {
        match_token: String,
        start_timeout_secs: u64,
    },
    #[serde(rename = "round_start")]
    RoundStart {
        match_token: String,
        frame: u32,
        round: u32,
        countdown_ticks: u32,
    },
    #[serde(rename = "peer_input")]
    PeerInput {
        match_token: String,
        from: String,
        frame: u32,
        target: String,
    },
    #[serde(rename = "rollback_resync")]
    RollbackResync {
        match_token: String,
        frame: u32,
        state: String,
    },
    GameOver {
        match_token: String,
        winner: String,
    },
    QueueStatus {
        elapsed_ms: u64,
        band_lo: f64,
        band_hi: f64,
        candidates: u32,
        queue_size: u32,
        my_mu: f64,
        my_sigma: f64,
        my_rating: f64,
        leaderboard: Vec<lobby_core::types::LeaderboardEntry>,
    },
    #[serde(rename = "opponent_connected")]
    OpponentConnected {
        match_token: String,
    },
    ReportReceived {
        match_token: String,
        reporting_player: String,
        #[serde(default)]
        winner: Option<String>,
        demo_hash: Option<String>,
    },
    #[serde(rename = "match_declined")]
    MatchDeclined {
        match_token: String,
    },
    #[serde(rename = "match_expired")]
    MatchExpired {
        match_token: String,
    },
    #[serde(rename = "match_result")]
    MatchResult {
        match_token: String,
        outcome: serde_json::Value,
    },
    #[serde(rename = "queue_expired")]
    QueueExpired,
    Error {
        message: String,
    },
    #[serde(rename = "webrtc_offer")]
    WebrtcOffer {
        match_token: String,
        from: String,
        sdp: String,
    },
    #[serde(rename = "webrtc_answer")]
    WebrtcAnswer {
        match_token: String,
        from: String,
        sdp: String,
    },
    #[serde(rename = "webrtc_ice")]
    WebrtcIce {
        match_token: String,
        from: String,
        candidate: String,
    },
}
#[derive(Debug, Clone, Deserialize)]
pub struct OpponentInfo {
    pub player_id: String,
    pub display_name: String,
}

#[derive(Debug, Clone)]
pub struct AuthOk {
    pub player_id: String,
    pub display_name: String,
    pub state: lobby_core::types::PlayerState,
}

#[derive(Debug, Clone)]
pub struct MatchFound {
    pub match_token: String,
    pub opponent: OpponentInfo,
    pub timeout_ms: u64,
    pub game_type: lobby_core::types::GameType,
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
    tx: mpsc::UnboundedSender<Message>,
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

        // The outbound channel carries `Message` (not just JSON text) so
        // `close()` can send a real Close frame — dropping the channels alone
        // never closes a tungstenite socket whose stream half is still parked
        // on `next()`, and the server would not see the disconnect.
        let (outgoing_tx, mut outgoing_rx) = mpsc::unbounded_channel::<Message>();
        let (incoming_tx, incoming_rx) =
            mpsc::unbounded_channel::<Result<ServerEvent, ClientError>>();

        // Outbound task: send frames from the channel to the socket
        tokio::spawn(async move {
            while let Some(msg) = outgoing_rx.recv().await {
                if ws_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // Inbound task: parse incoming frames and push to channel
        tokio::spawn(async move {
            while let Some(msg) = ws_rx.next().await {
                match msg {
                    Ok(Message::Text(text)) => match serde_json::from_str::<ServerEvent>(&text) {
                        Ok(event) => {
                            if incoming_tx.send(Ok(event)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = incoming_tx.send(Err(ClientError::Json(e)));
                        }
                    },
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
        self.tx
            .send(Message::Text(text.into()))
            .map_err(|_| ClientError::ChannelClosed)
    }

    /// Close the WebSocket cleanly (sends a Close frame, like the browser
    /// demo's `ws.close()`). Without this, a dropped `LobbyClient` leaves the
    /// socket half-open — its background reader task never wakes to notice the
    /// channel closures, so the server does not see the disconnect.
    pub fn close(&mut self) -> Result<(), ClientError> {
        self.tx
            .send(Message::Close(None))
            .map_err(|_| ClientError::ChannelClosed)
    }

    /// Authenticate with a JWT session token. Returns player info on success.
    pub async fn authenticate(&mut self, token: &str) -> Result<AuthOk, ClientError> {
        self.send(ClientMsg::Auth {
            session_token: token.to_string(),
        })?;
        match self.rx.recv().await.ok_or(ClientError::NoResponse)? {
            Ok(ServerEvent::AuthOk {
                player_id,
                display_name,
                state,
            }) => Ok(AuthOk {
                player_id,
                display_name,
                state,
            }),
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
        let client = reqwest::Client::new();
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

    /// POST /auth/guest to mint a fresh ephemeral "No account" session JWT,
    /// then authenticate over WebSocket. Always available (not dev-gated);
    /// rate-limited to 20/min per IP server-side.
    pub async fn authenticate_guest(&mut self, base_url: &str) -> Result<AuthOk, ClientError> {
        let client = reqwest::Client::new();
        let resp = client
            .post(format!("{base_url}/auth/guest"))
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
    pub async fn begin_matchmaking(
        &mut self,
        mode: &str,
        difficulty: &str,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::BeginMatchmaking {
            mode: mode.to_string(),
            difficulty: difficulty.to_string(),
        })
    }

    /// Leave the queue (no server response expected).
    pub async fn cancel_matchmaking(&mut self) -> Result<(), ClientError> {
        self.send(ClientMsg::CancelMatchmaking)
    }

    /// Send a WebRTC offer SDP to the opponent via the signaling relay.
    pub async fn send_webrtc_offer(
        &mut self,
        match_token: &str,
        sdp: String,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::WebrtcOffer {
            match_token: match_token.to_string(),
            sdp,
        })
    }

    /// Send a WebRTC answer SDP to the opponent via the signaling relay.
    pub async fn send_webrtc_answer(
        &mut self,
        match_token: &str,
        sdp: String,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::WebrtcAnswer {
            match_token: match_token.to_string(),
            sdp,
        })
    }

    /// Send a WebRTC ICE candidate to the opponent via the signaling relay.
    pub async fn send_webrtc_ice(
        &mut self,
        match_token: &str,
        candidate: String,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::WebrtcIce {
            match_token: match_token.to_string(),
            candidate,
        })
    }

    /// Tell the server the client is still alive. While queueing, refresh
    /// regularly (e.g. every 5s) to keep the queue entry from going stale.
    pub async fn heartbeat(&mut self) -> Result<(), ClientError> {
        self.send(ClientMsg::Heartbeat)
    }

    /// Accept a found match.
    pub async fn accept_match(&mut self, match_token: &str) -> Result<(), ClientError> {
        self.send(ClientMsg::AcceptMatch {
            match_token: match_token.to_string(),
        })
    }

    /// Decline a found match.
    pub async fn decline_match(&mut self, match_token: &str) -> Result<(), ClientError> {
        self.send(ClientMsg::DeclineMatch {
            match_token: match_token.to_string(),
        })
    }

    /// Notify the server that the P2P connection is established; begin the match.
    pub async fn start_match(&mut self, match_token: &str) -> Result<(), ClientError> {
        self.send(ClientMsg::StartMatch {
            match_token: match_token.to_string(),
        })
    }

    /// Send a frame-stamped paddle target for the rollback protocol.
    /// `frame` is the sim frame the input applies to (the client's
    /// `session.frame + 1`). The target travels as its shortest round-trip
    /// decimal string — serde_json's f64 parser is off by 1 ULP for some
    /// values, which would silently desync the sims.
    pub async fn send_game_input(
        &mut self,
        match_token: &str,
        frame: u32,
        target: f64,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::GameInput {
            match_token: match_token.to_string(),
            frame,
            target: target.to_string(),
        })
    }

    /// Report the local checksum for a confirmed frame (referee health check).
    /// `checksum` is serialized as a decimal string (u64 exceeds JS precision).
    pub async fn send_rollback_health(
        &mut self,
        match_token: &str,
        frame: u32,
        checksum: u64,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::RollbackHealth {
            match_token: match_token.to_string(),
            frame,
            checksum: checksum.to_string(),
        })
    }

    /// Submit a match result. `winner` is the victor's player_id (UUID
    /// string); `None` for a draw.
    pub async fn submit_report(
        &mut self,
        match_token: &str,
        winner: Option<&str>,
        demo_hash: Option<&str>,
    ) -> Result<(), ClientError> {
        self.send(ClientMsg::MatchReport {
            match_token: match_token.to_string(),
            winner: winner.map(|s| s.to_string()),
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
                Some(Ok(ServerEvent::MatchFound {
                    match_token,
                    opponent,
                    timeout_ms,
                    game_type,
                })) => {
                    return Ok(Some(MatchFound {
                        match_token,
                        opponent,
                        timeout_ms,
                        game_type,
                    }));
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
