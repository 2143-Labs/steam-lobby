use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type SteamId = u64;

// 17-digit Steam IDs exceed JavaScript's exact integer range (2^53 ≈ 9e15), so
// browser clients send them as decimal strings; Rust clients send numbers.
// These helpers accept both on the way in and emit strings on the way out.

/// Deserialize a Steam ID from either a JSON number or a decimal string.
pub fn deserialize_steam_id<'de, D>(de: D) -> Result<SteamId, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Num(u64),
        Str(String),
    }
    match Deserialize::deserialize(de)? {
        Repr::Num(n) => Ok(n),
        Repr::Str(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

/// Like [`deserialize_steam_id`] but for `Option<SteamId>` (JSON `null` → `None`).
pub fn deserialize_optional_steam_id<'de, D>(de: D) -> Result<Option<SteamId>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Repr {
        Null,
        Num(u64),
        Str(String),
    }
    Ok(match Deserialize::deserialize(de)? {
        Repr::Null => None,
        Repr::Num(n) => Some(n),
        Repr::Str(s) => Some(s.parse().map_err(serde::de::Error::custom)?),
    })
}

/// Serialize a Steam ID as a decimal string (safe for JS clients to round-trip).
pub fn serialize_steam_id<S>(id: &SteamId, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&id.to_string())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum PlayerState {
    InMenus,
    Queueing,
    MatchAccepted,
    InMatch,
    Reporting,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchStatus {
    PendingAccept,
    InProgress,
    Reporting,
    Disputed,
    Resolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchDifficulty {
    Easy,
    Normal,
    Hard,
}

impl MatchDifficulty {
    /// MMR offset applied to the search band during matchmaking.
    /// Easy shifts band down (target weaker opponents),
    /// Hard shifts band up (target stronger opponents).
    pub fn mmr_offset(&self) -> f64 {
        match self {
            MatchDifficulty::Easy => -150.0,
            MatchDifficulty::Normal => 0.0,
            MatchDifficulty::Hard => 150.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlayerInfo {
    pub steam_id: SteamId,
    pub display_name: String,
    pub state: PlayerState,
    pub last_heartbeat: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenSkillRating {
    pub mu: f64,    // skill estimate, default 25.0
    pub sigma: f64, // uncertainty, default 25.0/3.0 ≈ 8.333
    pub last_updated: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchInfo {
    pub match_token: String, // UUIDv4
    pub player_a: SteamId,
    pub player_a_difficulty: MatchDifficulty,
    pub player_b: SteamId,
    pub player_b_difficulty: MatchDifficulty,
    pub game_mode: String,
    pub status: MatchStatus,
    pub created_at: DateTime<Utc>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub accepted_a: bool,
    pub accepted_b: bool,
    pub connected_a: bool,
    pub connected_b: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatchReport {
    pub match_token: String,
    pub reporting_player: SteamId,
    pub winner: Option<SteamId>,   // None = draw
    pub demo_hash: Option<String>, // SHA-256 hex
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum MatchOutcome {
    Win { mu_change: f64 },
    Loss { mu_change: f64 },
    Draw { mu_change: f64 },
    Disputed,
    UnreviewableDispute,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub steam_id: SteamId,
    pub game_mode: String,
    pub difficulty: MatchDifficulty,
    pub mu: f64,
    pub queued_at: DateTime<Utc>,
}
