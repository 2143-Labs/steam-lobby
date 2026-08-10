//! `PostgresStore`: the production impl of the core storage traits. Shared
//! row types and parse helpers used by the per-trait impl files (`players`,
//! `matches`, `queue`, `ratings`).
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use lobby_core::error::{LobbyError, Result};
use lobby_core::traits::{MatchStore, PlayerStore, QueueStore, RatingStore};
use lobby_core::types::{
    GameType, MatchDifficulty, MatchEvent, MatchInfo, MatchReport, MatchStatus, OpenSkillRating,
    PlayerInfo, PlayerState, QueueEntry, SteamId,
};
use sqlx::PgPool;

pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

fn map_db_error(e: sqlx::Error) -> LobbyError {
    LobbyError::Database(e.to_string())
}

fn parse_difficulty(s: &str) -> MatchDifficulty {
    match s {
        "easy" => MatchDifficulty::Easy,
        "hard" => MatchDifficulty::Hard,
        _ => MatchDifficulty::Normal,
    }
}

fn parse_game_type(s: &str) -> GameType {
    match s {
        "server" => GameType::Server,
        _ => GameType::P2p,
    }
}

fn parse_match_status(s: &str) -> MatchStatus {
    match s {
        "PendingAccept" => MatchStatus::PendingAccept,
        "InProgress" => MatchStatus::InProgress,
        "Playing" => MatchStatus::Playing,
        "Reporting" => MatchStatus::Reporting,
        "Disputed" => MatchStatus::Disputed,
        "Resolved" => MatchStatus::Resolved,
        _ => MatchStatus::PendingAccept,
    }
}

fn parse_player_state(s: &str) -> PlayerState {
    match s {
        "InMenus" => PlayerState::InMenus,
        "Queueing" => PlayerState::Queueing,
        "MatchAccepted" => PlayerState::MatchAccepted,
        "InMatch" => PlayerState::InMatch,
        "Reporting" => PlayerState::Reporting,
        _ => PlayerState::InMenus,
    }
}

/// Row shape for the 19-column `matches` SELECTs. A struct (not a tuple) because
/// sqlx `FromRow` is only implemented for tuples up to 16 elements.
#[derive(sqlx::FromRow)]
struct MatchRow {
    match_token: String,
    player_a: uuid::Uuid,
    player_a_difficulty: String,
    player_b: uuid::Uuid,
    player_b_difficulty: String,
    game_mode: String,
    game_type: String,
    status: String,
    created_at: DateTime<Utc>,
    accepted_at: Option<DateTime<Utc>>,
    started_at: Option<DateTime<Utc>>,
    ended_at: Option<DateTime<Utc>>,
    server_address: Option<String>,
    join_token: Option<String>,
    result_secret: Option<String>,
    accepted_a: bool,
    accepted_b: bool,
    connected_a: bool,
    connected_b: bool,
}

impl From<MatchRow> for MatchInfo {
    fn from(r: MatchRow) -> Self {
        MatchInfo {
            match_token: r.match_token,
            player_a: r.player_a,
            player_a_difficulty: parse_difficulty(&r.player_a_difficulty),
            player_b: r.player_b,
            player_b_difficulty: parse_difficulty(&r.player_b_difficulty),
            game_mode: r.game_mode,
            game_type: parse_game_type(&r.game_type),
            status: parse_match_status(&r.status),
            created_at: r.created_at,
            accepted_at: r.accepted_at,
            started_at: r.started_at,
            ended_at: r.ended_at,
            server_address: r.server_address,
            join_token: r.join_token,
            result_secret: r.result_secret,
            accepted_a: r.accepted_a,
            accepted_b: r.accepted_b,
            connected_a: r.connected_a,
            connected_b: r.connected_b,
        }
    }
}

mod matches;
mod players;
mod queue;
mod ratings;
