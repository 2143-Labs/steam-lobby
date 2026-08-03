use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::Mutex as StdMutex;

use lobby_core::traits::GameCallbacks;
use tokio::sync::{mpsc, Mutex as TokioMutex};

use crate::db::PostgresStore;
use crate::rate_limit::RateLimiter;
use crate::steam_auth::SteamAuthService;
use crate::ws::ServerMessage;

pub struct AppState {
    pub player_manager: lobby_core::player::PlayerManager<DefaultCallbacks>,
    pub matchmaking_queue: lobby_core::queue::MatchmakingQueue<DefaultCallbacks>,
    pub match_manager: lobby_core::match_lifecycle::MatchManager<DefaultCallbacks>,
    pub connections: TokioMutex<HashMap<u64, ConnectionEntry>>,
    pub steam_auth: SteamAuthService,
    pub store: PostgresStore,
    pub public_url: Option<String>,
    pub auth_dev_mode: bool,
    pub jwt_ttl_secs: u64,
    pub allowed_origins: Vec<String>,
    pub openid_states: StdMutex<HashMap<String, OpenIdState>>,
    pub ticket_limiter: RateLimiter,
    pub test_token_limiter: RateLimiter,
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
