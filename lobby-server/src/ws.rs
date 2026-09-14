//! WebSocket protocol: the `ClientMessage`/`ServerMessage` enums (tag=type,
//! snake_case) and the auth-then-command flow from upgrade to disconnect.
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use axum::extract::{ConnectInfo, State};
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use futures_util::{SinkExt, StreamExt};
use lobby_core::pong::PongSide;
use lobby_core::traits::{MatchStore, PlayerStore, QueueStore};
use lobby_core::types::MatchDifficulty;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::time::{Duration, timeout};

use crate::pong::{PongInput, RollbackHealth};
use crate::state::{AppState, ConnectionEntry};
use crate::steam_auth::ValidatedSession;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Inbound commands from the client, tag=type, snake_case. The first message
/// must be `Auth` or `AuthTicket`; anything else ends the session.
pub enum ClientMessage {
    /// Authenticate with a JWT session token (from OpenID login).
    Auth { session_token: String },
    /// Authenticate with a raw Steam ticket.
    AuthTicket { ticket: String },
    /// Enter the matchmaking queue for a mode.
    BeginMatchmaking { mode: String, difficulty: String },
    /// Leave the queue.
    CancelMatchmaking,
    /// Accept a found match.
    AcceptMatch { match_token: String },
    /// Decline a found match; the match becomes `Disputed` if still pending.
    DeclineMatch { match_token: String },
    /// Click START = my P2P connection is established; begin the match.
    StartMatch { match_token: String },
    /// Durable ranked command variants use a client-generated command ID.
    Queue {
        command_id: uuid::Uuid,
        mode: String,
        difficulty: MatchDifficulty,
    },
    CancelQueue { command_id: uuid::Uuid },
    Accept { command_id: uuid::Uuid, match_token: String },
    Decline { command_id: uuid::Uuid, match_token: String },
    Trying {
        command_id: uuid::Uuid,
        match_token: String,
        attempt_id: uuid::Uuid,
        role: crate::commands::LobbyRole,
        lobby_id: Option<String>,
    },
    Connect {
        command_id: uuid::Uuid,
        match_token: String,
        attempt_id: uuid::Uuid,
        local_reached: bool,
        peer_reached: bool,
    },
    Ready {
        command_id: uuid::Uuid,
        match_token: String,
        attempt_id: uuid::Uuid,
    },
    Report {
        command_id: uuid::Uuid,
        match_token: String,
        outcome: crate::commands::RelativeOutcome,
        score: Option<String>,
        checksum: Option<String>,
        end_frame: Option<u64>,
    },
    Abandon {
        command_id: uuid::Uuid,
        match_token: String,
        target: crate::commands::AbandonTarget,
    },
    RankedHeartbeat { command_id: uuid::Uuid },
    /// Pong paddle target (normalized paddle-center Y, 0..1), frame-stamped.
    /// `frame` is the sim frame this input applies to (the client's
    /// `session.frame + 1`). No `#[serde(default)]` — clean cutover, the demo
    /// is the only client; a stale tab fails to parse and gets the standard
    /// invalid-message error.
    GameInput {
        match_token: String,
        frame: u32,
        /// Paddle target as its shortest round-trip decimal STRING: serde_json's
        /// f64 parser is off by 1 ULP for some values, which would silently
        /// diverge the referee's sim from the clients' (any desync is
        /// unacceptable). Parsed with `str::parse::<f64>()` (correctly rounded).
        target: String,
    },
    /// RPS round choice (0 = rock, 1 = paper, 2 = scissors).
    RpsChoice {
        match_token: String,
        choice: u8,
    },
    /// WebRTC signaling: offer SDP from the offerer (player_a).
    WebrtcOffer { match_token: String, sdp: String },
    /// WebRTC signaling: answer SDP from the answerer (player_b).
    WebrtcAnswer { match_token: String, sdp: String },
    /// WebRTC signaling: ICE candidate.
    WebrtcIce {
        match_token: String,
        candidate: String,
    },
    /// Client's per-frame checksum report (referee health check): the FNV-1a 64
    /// checksum of the authoritative state it computed for `frame`, as a decimal
    /// string (JS cannot hold u64 exactly). Parse failure → message ignored.
    RollbackHealth {
        match_token: String,
        frame: u32,
        checksum: String,
    },
    /// Submit a match report (`winner` None = draw).
    MatchReport {
        match_token: String,
        winner: Option<String>,
        demo_hash: Option<String>,
    },
    /// Keepalive; refreshes queue liveness.
    Heartbeat,
}

#[derive(Debug, Serialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
/// Outbound messages sent over WebSocket connections.
#[allow(dead_code)] // reserved variants: none sent yet
pub enum ServerMessage {
    /// Authentication succeeded; carries the abstract account id (users.id,
    /// what the UI shows as "player id") plus the display name and state.
    AuthOk {
        player_id: String,
        display_name: String,
        state: lobby_core::types::PlayerState,
    },
    /// Live queue stats + leaderboard for a queued player's mode.
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
    /// The opponent reported their peer-to-peer connection.
    OpponentConnected { match_token: String },
    /// A match report was stored (broadcast to both players).
    ReportReceived {
        match_token: String,
        reporting_player: String,
        winner: Option<String>,
        demo_hash: Option<String>,
    },
    /// A match was found; the player must accept within `timeout_ms`.
    MatchFound {
        match_token: String,
        opponent: OpponentInfo,
        timeout_ms: u64,
        game_type: lobby_core::types::ConnectionStrategy,
        game_mode: String,
    },
    /// Both players accepted — the START window of `start_timeout_secs` is open.
    MatchStarted {
        match_token: String,
        start_timeout_secs: u64,
    },
    /// A gameserver was provisioned for a server-authoritative match.
    GameServerReady {
        match_token: String,
        address: String,
        join_token: Option<String>,
    },
    /// Gameserver provisioning failed or timed out.
    GameServerError {
        match_token: String,
        message: String,
    },
    /// Authoritative pong frame, broadcast ~30x/sec to both players.
    /// `player_a` is Left, `player_b` is Right (so each client renders itself).
    /// `frame` is the sim frame this state is AFTER (inputs ≤ frame applied);
    /// `checksum` is the FNV-1a 64 of the authoritative state as a decimal
    /// string, for the clients' local desync check.
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
    /// The referee has advanced to `frame` — both players' inputs for it are
    /// known and applied. Clients advance their confirmed frame on this.
    InputAck { match_token: String, frame: u32 },
    /// A round has begun: the referee holds the sim frozen for `countdown_ticks`
    /// 33ms ticks (3-2-1). `frame` is the first frame of the hold (the ball
    /// launches at `frame + countdown_ticks`); `round` is the 0-based round
    /// number (0 at game start, then `left_score + right_score`).
    RoundStart {
        match_token: String,
        frame: u32,
        round: u32,
        countdown_ticks: u32,
    },
    /// An RPS round is open: both players must send `RpsChoice` within
    /// `timeout_ms`. `round` is 0-based (0 at match start); `player_a` /
    /// `player_b` are the participants, so each client learns its own side
    /// (RPS has no `GameState` frames to carry it).
    RpsBegin {
        match_token: String,
        round: u32,
        timeout_ms: u64,
        player_a: String,
        player_b: String,
    },
    /// verdict. `winner` is the round winner's player id (None = draw);
    /// scores are cumulative.
    RpsRound {
        match_token: String,
        round: u32,
        a_choice: u8,
        b_choice: u8,
        winner: Option<String>,
        a_score: u8,
        b_score: u8,
    },
    /// One player's `GameInput`, relayed to the opponent so each client can
    /// run the peer's inputs through its local rollback engine. The sender
    /// never receives its own `PeerInput`. `target` is a decimal STRING for
    /// the same bit-exactness reason as `GameInput` (serde_json f64 parsing
    /// is not correctly rounded).
    PeerInput {
        match_token: String,
        from: String,
        frame: u32,
        target: String,
    },
    /// The client's reported checksum diverged from the referee's: here is the
    /// authoritative 74-byte state at `frame`, hex-encoded. The client must
    /// `restore` from it and replay its buffered inputs.
    RollbackResync {
        match_token: String,
        frame: u32,
        state: String,
    },
    /// WebRTC signaling: offer SDP relayed from the offerer to the answerer.
    WebrtcOffer {
        match_token: String,
        from: String,
        sdp: String,
    },
    /// WebRTC signaling: answer SDP relayed back to the offerer.
    WebrtcAnswer {
        match_token: String,
        from: String,
        sdp: String,
    },
    /// WebRTC signaling: ICE candidate relayed to the peer.
    WebrtcIce {
        match_token: String,
        from: String,
        candidate: String,
    },
    /// The pong match ended (first to 3, or forfeit on disconnect).
    GameOver {
        match_token: String,
        winner: String,
    },
    /// Final outcome of a match.
    MatchResult {
        match_token: String,
        outcome: serde_json::Value, // MatchOutcome serialized
    },
    /// The opponent declined the match.
    MatchDeclined { match_token: String },
    /// The match expired (accept timeout).
    MatchExpired { match_token: String },
    /// The player's queue entry was dropped (stale heartbeat).
    QueueExpired,
    /// Admission receipt for a durable ranked command.
    CommandReceipt {
        receipt: uuid::Uuid,
        status: crate::commands::CommandStatus,
        error_code: Option<String>,
    },
    /// Protocol-level error.
    Error { code: String, message: String },
}

#[derive(Debug, Serialize, Clone)]
pub struct OpponentInfo {
    pub player_id: String,
    pub display_name: String,
}

/// WebSocket upgrade handler: origin-checked (CORS allowlist, dev file://
/// null origin, or same-origin as the page that served /), 64 KiB frame cap,
/// then on_upgrade into `handle_ws`.
pub async fn ws_route(
    ws: axum::extract::WebSocketUpgrade,
    State(app_state): axum::extract::State<Arc<AppState>>,
    ConnectInfo(peer): ConnectInfo<std::net::SocketAddr>,
    headers: HeaderMap,
) -> axum::response::Response {
    let cookie_session = crate::routes::authenticate_cookie_ws(&app_state, &headers).await;
    let origin = headers.get(axum::http::header::ORIGIN).and_then(|v|v.to_str().ok());
    // Token/bearer handshakes keep the historical same-host allowance (the page
    // served by this very host), plus the configured CORS allowlist and the
    // dev-only null origin. Cookie credentials never take this path.
    let same_host_origin = origin.is_some_and(|value| {
        let Ok(parsed) = url::Url::parse(value) else {
            return false;
        };
        headers
            .get(axum::http::header::HOST)
            .and_then(|v|v.to_str().ok())
            .is_some_and(|host| {
                parsed.host_str().is_some_and(|h| {
                    let port = parsed.port().or_else(|| parsed.port_or_known_default());
                    host == h || port.is_some_and(|p| host == format!("{h}:{p}"))
                })
            })
    });
    let bearer_origin_allowed = origin.is_none_or(|s| {
        same_host_origin
            || app_state.config.cors_origins.iter().any(|a|a==s)
            || (app_state.config.auth_dev_mode && s=="null")
    });
    // A cookie credential is ambient, so it may only ever upgrade from the
    // exact PUBLIC_URL origin (enforced in `authenticate_cookie_ws`). The CORS
    // allowlist authorizes bearer/token handshakes only, never a cookie one: a
    // cookie sent from a merely allowlisted origin must not authenticate.
    let presents_cookie = headers
        .get(axum::http::header::COOKIE)
        .and_then(|v|v.to_str().ok())
        .is_some_and(|raw| raw.split(';').any(|part| {
            part.trim()
                .split_once('=')
                .is_some_and(|(key,_)|key=="lobby_session")
        }));
    let authorized = if presents_cookie {
        cookie_session.is_some()
    } else {
        bearer_origin_allowed
    };
    if !authorized {
        return axum::http::StatusCode::FORBIDDEN.into_response();
    }
    ws.max_message_size(64 * 1024)
        .max_frame_size(64 * 1024)
        .on_upgrade(move |socket| handle_ws(socket, app_state, peer, cookie_session))
}
/// Run a WebSocket session: authenticate, then pump client commands and
/// server broadcasts until either side closes.
pub async fn handle_ws(
    ws: WebSocket,
    state: Arc<AppState>,
    peer_ip: std::net::SocketAddr,
    cookie_session: Option<ValidatedSession>,
) {
    let (mut sender, mut receiver) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();

    let (user_id, auth_session) = if let Some(session)=cookie_session {
        let Some(user_id)=crate::routes::revalidate_session(&state,&session).await else{return};
        (user_id,Some(session))
    } else {
        match authenticate(&mut receiver, &mut sender, &state, peer_ip).await {
            Ok(result) => result,
            Err(_) => return,
        }
    };

    // Send auth ok — include the player's persisted state so a reconnecting
    // client knows it was in the queue, mid-match, etc.
    let display_name = state
        .store
        .get_display_name(user_id)
        .await
        .unwrap_or_else(|_| None)
        .unwrap_or_else(|| "Unknown".into());
    let player_state = state
        .store
        .get_player_state(user_id)
        .await
        .ok()
        .flatten()
        .map(|p| p.state)
        .unwrap_or(lobby_core::types::PlayerState::InMenus);
    let _ = sender
        .send(Message::Text(
            serde_json::to_string(&ServerMessage::AuthOk {
                player_id: user_id.to_string(),
                display_name: display_name.clone(),
                state: player_state,
            })
            .unwrap()
            .into(),
        ))
        .await;

    tracing::info!("player {user_id} connected from {peer_ip} ({display_name})");

    // Enter menus
    let _ = state
        .player_manager
        .enter_menus(user_id, &state.store)
        .await;

    // Temporal path: start THIS connection's UserSessionWorkflow — a fresh
    // session per connection (session UUID), so a crash-then-reconnect or a
    // replaced connection starts a new workflow instead of colliding with the
    // old one. Best-effort; None while Temporal is down.
    let session_id = uuid::Uuid::new_v4().to_string();
    crate::temporal::signals::start_user_session(&state, user_id, &session_id).await;

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
            user_id,
            ConnectionEntry {
                tx: tx.clone(),
                generation: my_gen,
                abort: outbound_abort,
                session_id: session_id.clone(),
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
            msg = timeout(Duration::from_secs(30), receiver.next()) => {
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
                        if let Some(session)=&auth_session
                            && crate::routes::revalidate_session(&state,session).await.is_none()
                        {
                            disconnect_reason = "session revoked";
                            break;
                        }
                        handle_client_message(
                            cm,
                            user_id,
                            &session_id,
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
                        tracing::debug!("ws read error from {user_id}: {e}");
                        disconnect_reason = "socket error";
                        break;
                    }
                    Err(_) => {
                        // No frames for 30s — a client that stopped heartbeating.
                        // Clients send a passive heartbeat every ~10s while
                        // connected; 30s of silence means the connection is dead
                        // or the client is gone, so drop it.
                        disconnect_reason = "no heartbeat for 30s";
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
            .get(&user_id)
            .map(|e| e.generation == my_gen)
            .unwrap_or(false)
    };
    // Ending a socket only ends its connection workflow. Ranked queue leases
    // and canonical match state are driven by durable commands and deadlines;
    // transport loss is never evidence of decline, abandonment, or a result.
    crate::temporal::signals::signal_disconnect(&state, user_id, &session_id).await;
    if is_current {
        let still_queued = state.store.is_queued(user_id).await.unwrap_or(false);
        let has_live_match = state
            .store
            .get_player_state(user_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|player| {
                matches!(
                    player.state,
                    lobby_core::types::PlayerState::MatchAccepted
                        | lobby_core::types::PlayerState::InMatch
                        | lobby_core::types::PlayerState::Reporting
                )
            });
        if !still_queued && !has_live_match {
            let _ = state
                .player_manager
                .handle_disconnect(user_id, &state.store)
                .await;
        }
        state.connections.lock().await.remove(&user_id);
    } else {
        disconnect_reason = "replaced by newer connection";
    }
    tracing::info!("player {user_id} disconnected ({disconnect_reason})");
    outbound_task.abort();
}

async fn authenticate(
    receiver: &mut (impl StreamExt<Item = Result<Message, axum::Error>> + Unpin),
    sender: &mut (impl SinkExt<Message, Error = axum::Error> + Unpin),
    state: &Arc<AppState>,
    peer_ip: std::net::SocketAddr,
) -> Result<(uuid::Uuid,Option<ValidatedSession>), ()> {
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

    // ── Auth ──
    match cm {
        ClientMessage::Auth { session_token } => {
            let Some(session)=crate::routes::authenticate_token(state,&session_token).await else{
                tracing::warn!("auth failed (invalid or revoked session) from {peer_ip}");return Err(())
            };
            let user_id=crate::routes::revalidate_session(state,&session).await.ok_or(())?;
            Ok((user_id,Some(session)))
        }
        ClientMessage::AuthTicket { ticket } => {
            if !state.ticket_limiter.check(peer_ip.ip()) { return Err(()) }
            match state.steam_auth.verify_ticket(&ticket,"matchmaking").await {
                Ok(steam_id) => match state.store.find_or_create_user("steam",&steam_id.to_string(),"",true).await {
                    Ok(user_id) => {
                        let sid=state.store.create_native_session(user_id).await.map_err(|_|())?;
                        let version=state.store.get_token_version(user_id).await.map_err(|_|())?;
                        let token=state.steam_auth.generate_native_token(user_id,sid,version).map_err(|_|())?;
                        let session=state.steam_auth.validate_native_token(&token).map_err(|_|())?;
                        Ok((user_id,Some(ValidatedSession::Native(session))))
                    }
                    Err(_) => Err(())
                },
                Err(_) => Err(()),
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
    user_id: uuid::Uuid,
    session_id: &str,
    state: &Arc<AppState>,
    tx: &mpsc::UnboundedSender<ServerMessage>,
) {
    match cm {
        ClientMessage::Queue { command_id, mode, difficulty } => {
            dispatch_ranked(
                state,
                user_id,
                session_id,
                command_id,
                crate::commands::RankedCommand::Queue { mode, difficulty },
                tx,
            )
            .await;
        }
        ClientMessage::CancelQueue { command_id } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::CancelQueue, tx).await;
        }
        ClientMessage::Accept { command_id, match_token } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Accept { match_token }, tx).await;
        }
        ClientMessage::Decline { command_id, match_token } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Decline { match_token }, tx).await;
        }
        ClientMessage::Trying { command_id, match_token, attempt_id, role, lobby_id } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Trying { match_token, attempt_id, role, lobby_id }, tx).await;
        }
        ClientMessage::Connect { command_id, match_token, attempt_id, local_reached, peer_reached } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Connect { match_token, attempt_id, local_reached, peer_reached }, tx).await;
        }
        ClientMessage::Ready { command_id, match_token, attempt_id } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Ready { match_token, attempt_id }, tx).await;
        }
        ClientMessage::Report { command_id, match_token, outcome, score, checksum, end_frame } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Report { match_token, outcome, score, checksum, end_frame }, tx).await;
        }
        ClientMessage::Abandon { command_id, match_token, target } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Abandon { match_token, target }, tx).await;
        }
        ClientMessage::RankedHeartbeat { command_id } => {
            dispatch_ranked(state, user_id, session_id, command_id, crate::commands::RankedCommand::Heartbeat, tx).await;
        }
        // ── Matchmaking ──
        ClientMessage::BeginMatchmaking { mode, difficulty } => {
            let diff = match difficulty.as_str() {
                "easy" => MatchDifficulty::Easy,
                "hard" => MatchDifficulty::Hard,
                _ => MatchDifficulty::Normal,
            };
            tracing::info!("player {user_id} entered queue ({mode}, {difficulty})");
            // The UserSessionWorkflow's queue signal runs the enter_queue
            // activity (player state + queue entry); the pairing Schedule
            // pairs from there. Cutover: no in-process fallback — if Temporal
            // is down, the signal helper no-ops and the client is told nothing
            // (the server is considered unavailable for matchmaking).
            crate::temporal::signals::signal_queue(state, user_id, session_id, mode, diff).await;
        }
        ClientMessage::CancelMatchmaking => {
            crate::temporal::signals::signal_unqueue(state, user_id, session_id).await;
            tracing::info!("player {user_id} left queue (cancelled)");
        }
        // ── Match lifecycle ──
        ClientMessage::AcceptMatch { match_token } => {
            tracing::info!("player {user_id} accepted match {match_token}");
            // P2p matches are owned by the P2PMatchWorkflow (match_choice
            // signal). Server matches stay in-process — the Temporal migration
            // is p2p-only; they have no START phase and resolve via the
            // gameserver webhook (out of scope).
            let is_server = match state.store.get_match(&match_token).await {
                Ok(Some(m)) => m.connection == lobby_core::types::ConnectionStrategy::Server,
                _ => false,
            };
            if is_server {
                match state
                    .match_manager
                    .accept_match(&match_token, user_id, &state.store)
                    .await
                {
                    Ok(()) => {}
                    Err(e) => tracing::warn!(
                        "player {user_id} accept rejected for match {match_token}: {e}"
                    ),
                }
            } else {
                crate::temporal::signals::signal_match_choice(state, &match_token, user_id, true)
                    .await;
            }
        }
        ClientMessage::DeclineMatch { match_token } => {
            tracing::info!("player {user_id} declined match {match_token}");
            // The workflow's handle_decline activity notifies both players
            // and flips Disputed (the DeclineMatch handler body, moved).
            crate::temporal::signals::signal_match_choice(state, &match_token, user_id, false)
                .await;
        }
        ClientMessage::StartMatch { match_token } => {
            tracing::info!("player {user_id} started match {match_token}");
            // The P2PMatchWorkflow's start signal runs mark_connected (DB
            // Reporting + opponent_connected + spawn_game for playback) and
            // its start-window timer owns the forfeit. Cutover: no in-process
            // fallback — the workflow is the sole lifecycle writer.
            crate::temporal::signals::signal_start(state, &match_token, user_id).await;
        }
        ClientMessage::WebrtcOffer { match_token, sdp } => {
            let other = match &state.store.get_match(&match_token).await {
                Ok(Some(m)) if m.player_a == user_id => Some(m.player_b),
                Ok(Some(m)) if m.player_b == user_id => Some(m.player_a),
                _ => None,
            };
            if let Some(other) = other {
                let connections = state.connections.lock().await;
                if let Some(e) = connections.get(&other) {
                    let _ = e.tx.send(ServerMessage::WebrtcOffer {
                        match_token,
                        from: user_id.to_string(),
                        sdp,
                    });
                }
            }
        }
        ClientMessage::WebrtcAnswer { match_token, sdp } => {
            let other = match &state.store.get_match(&match_token).await {
                Ok(Some(m)) if m.player_a == user_id => Some(m.player_b),
                Ok(Some(m)) if m.player_b == user_id => Some(m.player_a),
                _ => None,
            };
            if let Some(other) = other {
                let connections = state.connections.lock().await;
                if let Some(e) = connections.get(&other) {
                    let _ = e.tx.send(ServerMessage::WebrtcAnswer {
                        match_token,
                        from: user_id.to_string(),
                        sdp,
                    });
                }
            }
        }
        ClientMessage::WebrtcIce {
            match_token,
            candidate,
        } => {
            let other = match &state.store.get_match(&match_token).await {
                Ok(Some(m)) if m.player_a == user_id => Some(m.player_b),
                Ok(Some(m)) if m.player_b == user_id => Some(m.player_a),
                _ => None,
            };
            if let Some(other) = other {
                let connections = state.connections.lock().await;
                if let Some(e) = connections.get(&other) {
                    let _ = e.tx.send(ServerMessage::WebrtcIce {
                        match_token,
                        from: user_id.to_string(),
                        candidate,
                    });
                }
            }
        }
        ClientMessage::GameInput {
            match_token,
            target,
            frame,
        } => {
            // `str::parse` is correctly rounded (serde_json's f64 parser is
            // not — up to 1 ULP off, which would desync the sims).
            let Ok(target) = target.parse::<f64>() else {
                return;
            };
            let target = target.clamp(0.0, 1.0);
            // Resolve the side BEFORE taking the parking_lot Mutex (the guard
            // is !Send and must not be held across an await).
            let side_and_other = match state.store.get_match(&match_token).await {
                Ok(Some(m)) if m.player_a == user_id => Some((PongSide::Left, m.player_b)),
                Ok(Some(m)) if m.player_b == user_id => Some((PongSide::Right, m.player_a)),
                _ => None,
            };
            if let Some((side, other)) = side_and_other {
                // Relay to the opponent: each client runs the peer's real
                // inputs through its local rollback engine. The sender never
                // receives its own PeerInput.
                {
                    let connections = state.connections.lock().await;
                    if let Some(e) = connections.get(&other) {
                        let _ = e.tx.send(ServerMessage::PeerInput {
                            match_token: match_token.clone(),
                            from: user_id.to_string(),
                            frame,
                            target: target.to_string(), // shortest round-trip decimal
                        });
                    }
                }
                let games = state.pong_games.lock();
                if let Some(g) = games.get(&match_token) {
                    let _ = g.input_tx.send(PongInput {
                        side,
                        target,
                        frame,
                    });
                }
            }
        }
        ClientMessage::RollbackHealth {
            match_token,
            frame,
            checksum,
        } => {
            // Checksums travel as decimal strings (JS cannot hold u64 exactly);
            // a malformed report is ignored.
            if let Ok(checksum) = checksum.parse::<u64>() {
                let games = state.pong_games.lock();
                if let Some(g) = games.get(&match_token) {
                    let _ = g.health_tx.send(RollbackHealth {
                        from: user_id,
                        frame,
                        checksum,
                    });
                }
            }
        }
        ClientMessage::RpsChoice {
            match_token,
            choice,
        } => {
            if choice > 2 {
                return;
            }
            let games = state.rps_games.lock();
            if let Some(g) = games.get(&match_token) {
                let _ = g.choice_tx.send(crate::rps::RpsChoice {
                    from: user_id,
                    choice,
                });
            }
        }
        // ── Reporting ──
        ClientMessage::MatchReport {
            match_token,
            winner,
            demo_hash,
        } => {
            tracing::info!(
                "report from {user_id} for match {match_token}: winner {:?}",
                winner
            );
            // The P2PMatchWorkflow's who_won + submit_demo signals drive
            // finish_match / resolve_dispute — the workflow is the sole
            // lifecycle writer. Cutover: no in-process submit_report.
            if let Some(w) = winner {
                if let Ok(w) = uuid::Uuid::parse_str(&w) {
                    crate::temporal::signals::signal_who_won(state, &match_token, user_id, w)
                        .await;
                }
            }
            if let Some(h) = demo_hash {
                crate::temporal::signals::signal_submit_demo(state, &match_token, user_id, h)
                    .await;
            }
        }
        // ── Liveness ──
        ClientMessage::Heartbeat => {
            tracing::trace!("heartbeat from {user_id}");
            let _ = state.player_manager.heartbeat(user_id, &state.store).await;
        }
        _ => {}
    }
}

async fn dispatch_ranked(
    state: &Arc<AppState>,
    user_id: uuid::Uuid,
    connection_id: &str,
    command_id: uuid::Uuid,
    command: crate::commands::RankedCommand,
    tx: &mpsc::UnboundedSender<ServerMessage>,
) {
    let Ok(session_id) = uuid::Uuid::parse_str(connection_id) else {
        let _ = tx.send(ServerMessage::Error {
            code: "invalid_session".into(),
            message: "Invalid WebSocket session".into(),
        });
        return;
    };
    let actor = crate::commands::CommandActor {
        user_id,
        session_kind: crate::commands::SessionKind::Websocket,
        session_id,
    };
    match crate::commands::dispatch(state, actor, command_id, command).await {
        Ok(receipt) => {
            let _ = tx.send(ServerMessage::CommandReceipt {
                receipt: receipt.receipt,
                status: receipt.status,
                error_code: receipt.error_code,
            });
        }
        Err(error) => {
            let code = crate::commands::error_code(&error).to_owned();
            let _ = tx.send(ServerMessage::Error {
                code,
                message: error.to_string(),
            });
        }
    }
}
/// Send `msg` to both players of a match (best-effort; skips disconnected).
pub async fn notify_match_players(state: &Arc<AppState>, token: &str, msg: ServerMessage) {
    if let Ok(Some(m)) = state.store.get_match(token).await {
        let connections = state.connections.lock().await;
        for pid in [m.player_a, m.player_b] {
            if let Some(e) = connections.get(&pid) {
                let _ = e.tx.send(msg.clone());
            }
        }
    }
}
