use std::collections::HashMap;

use lobby_core::traits::GameCallbacks;
use tokio::sync::{mpsc, Mutex};

use crate::db::PostgresStore;
use crate::steam_auth::SteamAuthService;
use crate::ws::ServerMessage;

pub struct AppState {
    pub player_manager: lobby_core::player::PlayerManager<DefaultCallbacks>,
    pub matchmaking_queue: lobby_core::queue::MatchmakingQueue<DefaultCallbacks>,
    pub match_manager: lobby_core::match_lifecycle::MatchManager<DefaultCallbacks>,
    pub steam_auth: SteamAuthService,
    pub store: PostgresStore,
    pub connections: Mutex<HashMap<u64, mpsc::UnboundedSender<ServerMessage>>>,
}

#[derive(Clone, Default)]
pub struct DefaultCallbacks;

impl GameCallbacks for DefaultCallbacks {}
