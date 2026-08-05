//! Shared `AppState`: the composition root handed to every axum handler and
//! the ticker. Config values handlers need are grouped in `RuntimeConfig`
//! (secrets stay in `SteamAuthService`); the rest are runtime-only structures.
use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex as StdMutex;

use lobby_core::traits::GameCallbacks;
use tokio::sync::{mpsc, Mutex as TokioMutex};

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
}

pub struct AppState {
    /// Per-player state machine (`PlayerState` transitions).
    pub player_manager: lobby_core::player::PlayerManager<DefaultCallbacks>,
    /// Matchmaking: pairing, re-pair cooldown, stale cleanup.
    pub matchmaking_queue: lobby_core::queue::MatchmakingQueue<DefaultCallbacks>,
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
}

pub struct OpenIdState {
    pub return_to: String,
    pub created_at: std::time::Instant,
}

pub struct ConnectionEntry {
    pub tx: mpsc::UnboundedSender<ServerMessage>,
    pub generation: u64,
    pub abort: tokio::task::AbortHandle,
}

#[derive(Clone, Default)]
pub struct DefaultCallbacks;

impl GameCallbacks for DefaultCallbacks {}
