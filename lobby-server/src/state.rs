//! Shared `AppState`: the composition root handed to every axum handler and
//! the ticker. Config values handlers need are grouped in `RuntimeConfig`
//! (secrets stay in `SteamAuthService`); the rest are runtime-only structures.
use parking_lot::Mutex as ParkMutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64;

use lobby_core::traits::GameCallbacks;
use tokio::sync::{Mutex as TokioMutex, mpsc};

use crate::db::PostgresStore;
use crate::rate_limit::RateLimiter;
use crate::steam_auth::SteamAuthService;
use crate::ws::ServerMessage;

/// Handler-visible runtime configuration, copied once from AppConfig at
/// startup. Keeps the JWT/Steam secrets (which live only in SteamAuthService)
/// out of the shared state.
pub struct RuntimeConfig {
    /// Absolute public base URL (None = relative return_to only).
    pub public_url: Option<String>,
    /// Test auth mode: true = /auth/test-token enabled.
    pub auth_dev_mode: bool,
    /// Session JWT lifetime in seconds.
    pub jwt_ttl_secs: u64,
    /// CORS allowlist origins (may be empty; same-origin always allowed).
    pub cors_origins: Vec<String>,
    /// Pong: run p2p matches as server-authoritative pong games (LOBBY_PONG).
    pub pong_enabled: bool,
    /// LOBBY_START_TIMEOUT_SECS; START window after both accept (forfeit).
    pub start_timeout_secs: u64,
    /// LOBBY_PONG_COUNTDOWN_TICKS; 3-2-1 hold in 33ms ticks; 0 = disabled.
    pub pong_countdown_ticks: u32,
    /// LOBBY_TURN_SECRET; None => /internal/turn-credentials returns 503.
    pub turn_secret: Option<String>,
    /// LOBBY_TURN_URIS; TURN server URIs returned to clients.
    pub turn_uris: Vec<String>,
    /// TEMPORAL_ADDRESS; Temporal gRPC frontend (plaintext URI).
    pub temporal_address: String,
    /// TEMPORAL_NAMESPACE; Temporal namespace the worker + client bind to.
    pub temporal_namespace: String,
    /// TEMPORAL_TASK_QUEUE; the in-process worker's task queue.
    pub temporal_task_queue: String,
    /// MATCH_ACCEPT_TIMEOUT_S; how long a PendingAccept match waits.
    pub match_accept_timeout_secs: u64,
    /// REPORT_TIMEOUT_S; how long a Reporting match waits for reports.
    pub report_timeout_secs: u64,
    /// LOBBY_PAIR_COOLDOWN_S; anti re-pair window after a resolved match.
    pub pair_cooldown_secs: u64,
}

pub struct AppState {
    pub player_manager: lobby_core::player::PlayerManager<DefaultCallbacks>,
    /// Match lifecycle actions: accept, connect, report, resolve.
    pub match_manager: lobby_core::match_lifecycle::MatchManager<DefaultCallbacks>,
    /// Live WebSocket senders by steam_id.
    pub connections: TokioMutex<HashMap<u64, ConnectionEntry>>,
    /// Steam ticket/OpenID auth + session JWTs.
    pub steam_auth: SteamAuthService,
    /// Postgres-backed storage for all core traits.
    pub store: PostgresStore,
    /// The modes this server actually runs.
    pub game_modes: Vec<(String, lobby_core::types::GameType)>,
    /// Client for the external gameserver creator.
    pub gameserver: crate::gameserver::GameserverClient,
    /// Shared HTTP client — one connection pool for outbound calls.
    pub http: reqwest::Client,
    /// Max seconds an accepted Server match may wait for allocation.
    pub gameserver_alloc_timeout_secs: u64,
    /// Max seconds a Playing match may run before the server is expected to report.
    pub gameserver_result_timeout_secs: u64,
    /// Config handlers need, copied once at startup (see `RuntimeConfig`).
    pub config: RuntimeConfig,
    /// OpenID login states awaiting their callback (600s TTL, 4096 cap).
    pub openid_states: StdMutex<HashMap<String, OpenIdState>>,
    /// Rate limiter for /auth/ticket.
    pub ticket_limiter: RateLimiter,
    /// Rate limiter for /auth/test-token.
    pub test_token_limiter: RateLimiter,
    /// Bumped per connection so a reconnecting client supersedes a stale one.
    pub next_generation: AtomicU64,
    /// Active pong matches: match_token -> input channel + task handle.
    pub pong_games: ParkMutex<std::collections::HashMap<String, crate::pong::ActivePong>>,
    /// Temporal client slot, set by the worker once it connects. `None` while
    /// Temporal is down — handlers fall back to the in-process path (Step 9
    /// transition window; deleted at cutover).
    pub temporal: std::sync::RwLock<Option<Arc<temporalio_client::Client>>>,
    /// Worker shutdown handle, set once the in-process Temporal worker boots
    /// (the test harness fires it at teardown so the worker stops polling
    /// before its test DB drops; production never touches it).
    pub temporal_shutdown: std::sync::RwLock<Option<Box<dyn Fn() + Send + Sync>>>,
}

pub struct OpenIdState {
    pub return_to: String,
    pub created_at: std::time::Instant,
}

pub struct ConnectionEntry {
    pub tx: mpsc::UnboundedSender<ServerMessage>,
    pub generation: u64,
    pub abort: tokio::task::AbortHandle,
    /// The per-connection session UUID (workflow ID suffix) this connection owns.
    pub session_id: String,
}

#[derive(Clone, Default)]
pub struct DefaultCallbacks;

impl GameCallbacks for DefaultCallbacks {}
