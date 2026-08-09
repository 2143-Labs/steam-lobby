//! Wire types shared by all crates, plus Steam-ID serde helpers that keep
//! 17-digit IDs exact for JS clients (serialized as decimal strings). Most
//! enums are `snake_case`; `MatchStatus` is deliberately PascalCase because
//! it is the DB column contract.
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
/// Serialize an optional Steam ID as a JSON string (or `null`).
pub fn serialize_optional_steam_id<S>(
    id: &Option<SteamId>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match id {
        Some(n) => serializer.serialize_str(&n.to_string()),
        None => serializer.serialize_none(),
    }
}

/// One row of the MMR leaderboard pushed to queueing players.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaderboardEntry {
    /// The abstract account id (users.id) — what the UI shows as "player id".
    pub player_id: String,
    #[serde(
        serialize_with = "serialize_steam_id",
        deserialize_with = "deserialize_steam_id"
    )]
    pub steam_id: SteamId,
    pub mu: f64,
    pub sigma: f64,
    pub rating: f64, // mu - 3*sigma, the display/matchmaking value
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    Playing,
    Reporting,
    Disputed,
    Resolved,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GameType {
    P2p,
    Server,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MatchEvent {
    Paired,
    Accepted,
    Declined,
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
    pub game_type: GameType,
    pub status: MatchStatus,
    pub created_at: DateTime<Utc>,
    pub accepted_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub ended_at: Option<DateTime<Utc>>,
    pub server_address: Option<String>, // set by mark_server_ready
    pub join_token: Option<String>,     // set by mark_server_ready, relayed in game_server_ready
    #[serde(skip_serializing)] // never leaves the server (URL secret)
    pub result_secret: Option<String>, // generated at match creation for Server games
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
    Win {
        mu_change: f64,
    },
    Loss {
        mu_change: f64,
    },
    Draw {
        mu_change: f64,
    },
    Disputed,
    UnreviewableDispute,
    /// Neither player started within the START window — double loss.
    Forfeit {
        mu_change: f64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueEntry {
    pub steam_id: SteamId,
    pub game_mode: String,
    pub difficulty: MatchDifficulty,
    pub mu: f64,
    pub queued_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;

    /// The Steam ID helpers are the JS-precision boundary: browser clients
    /// cannot hold 17-digit IDs exactly, so serialization must emit strings
    /// and deserialization must accept both numbers and strings.
    #[derive(Serialize, Deserialize)]
    struct SteamIdProbe {
        #[serde(
            serialize_with = "serialize_steam_id",
            deserialize_with = "deserialize_steam_id"
        )]
        id: SteamId,
    }

    #[derive(Deserialize)]
    struct OptionalSteamIdProbe {
        #[serde(default, deserialize_with = "deserialize_optional_steam_id")]
        winner: Option<SteamId>,
    }

    #[test]
    fn steam_id_serializes_as_string() {
        let v = serde_json::to_value(SteamIdProbe {
            id: 76_561_198_000_000_000,
        })
        .unwrap();
        assert_eq!(v, json!({ "id": "76561198000000000" }));
    }

    #[test]
    fn steam_id_deserializes_from_number_or_string() {
        let from_num: SteamIdProbe =
            serde_json::from_value(json!({ "id": 76561198000000000_i64 })).unwrap();
        let from_str: SteamIdProbe =
            serde_json::from_value(json!({ "id": "76561198000000000" })).unwrap();
        assert_eq!(from_num.id, from_str.id);
        assert_eq!(from_num.id, 76_561_198_000_000_000);
    }

    #[test]
    fn steam_id_round_trips_u64_max() {
        // Guards against a future naive u64->f64 conversion losing precision.
        let original = SteamIdProbe { id: u64::MAX };
        let v = serde_json::to_value(&original).unwrap();
        let back: SteamIdProbe = serde_json::from_value(v).unwrap();
        assert_eq!(original.id, back.id);
    }

    #[test]
    fn optional_steam_id_accepts_null_and_string() {
        let none: OptionalSteamIdProbe = serde_json::from_value(json!({ "winner": null })).unwrap();
        let some: OptionalSteamIdProbe =
            serde_json::from_value(json!({ "winner": "402" })).unwrap();
        assert_eq!(none.winner, None);
        assert_eq!(some.winner, Some(402));
    }

    #[test]
    fn snake_case_enums_serialize_lowercase() {
        assert_eq!(
            serde_json::to_string(&PlayerState::InMenus).unwrap(),
            "\"in_menus\""
        );
        assert_eq!(
            serde_json::to_string(&PlayerState::MatchAccepted).unwrap(),
            "\"match_accepted\""
        );
        assert_eq!(serde_json::to_string(&GameType::P2p).unwrap(), "\"p2p\"");
        assert_eq!(
            serde_json::to_string(&GameType::Server).unwrap(),
            "\"server\""
        );
        assert_eq!(
            serde_json::to_string(&MatchEvent::Paired).unwrap(),
            "\"paired\""
        );
        assert_eq!(
            serde_json::to_string(&MatchEvent::Declined).unwrap(),
            "\"declined\""
        );
        assert_eq!(
            serde_json::to_string(&MatchDifficulty::Hard).unwrap(),
            "\"hard\""
        );
    }

    #[test]
    fn match_status_uses_exact_variant_names() {
        // No rename_all on MatchStatus: the strings are the DB contract.
        assert_eq!(
            serde_json::to_string(&MatchStatus::PendingAccept).unwrap(),
            "\"PendingAccept\""
        );
        assert_eq!(
            serde_json::to_string(&MatchStatus::InProgress).unwrap(),
            "\"InProgress\""
        );
        assert_eq!(
            serde_json::to_string(&MatchStatus::Disputed).unwrap(),
            "\"Disputed\""
        );
        assert_eq!(
            serde_json::from_str::<MatchStatus>("\"PendingAccept\"").unwrap(),
            MatchStatus::PendingAccept
        );
    }

    #[test]
    fn leaderboard_entry_round_trips() {
        let entry = LeaderboardEntry {
            player_id: "a1b2c3d4-e5f6-4a7b-8c9d-0e1f2a3b4c5d".into(),
            steam_id: 76_561_198_000_000_000,
            mu: 27.0,
            sigma: 8.3,
            rating: 2.1,
        };
        let v = serde_json::to_value(&entry).unwrap();
        assert_eq!(
            v["steam_id"],
            json!("76561198000000000"),
            "leaderboard IDs are strings"
        );
        let back: LeaderboardEntry = serde_json::from_value(v).unwrap();
        assert_eq!(back.steam_id, entry.steam_id);
        assert_eq!(back.player_id, entry.player_id);
    }

    #[test]
    fn match_info_round_trips_and_hides_result_secret() {
        let m = MatchInfo {
            match_token: "token-1".to_string(),
            player_a: 400,
            player_a_difficulty: MatchDifficulty::Normal,
            player_b: 401,
            player_b_difficulty: MatchDifficulty::Easy,
            game_mode: "ranked_1v1".to_string(),
            game_type: GameType::Server,
            status: MatchStatus::PendingAccept,
            created_at: Utc::now(),
            accepted_at: None,
            started_at: None,
            ended_at: None,
            server_address: None,
            join_token: Some("join-abc".to_string()),
            result_secret: Some("secret-xyz".to_string()),
            accepted_a: true,
            accepted_b: false,
            connected_a: false,
            connected_b: false,
        };
        let v = serde_json::to_value(&m).unwrap();
        assert!(
            v.get("result_secret").is_none(),
            "result_secret must never leave the server"
        );
        let back: MatchInfo = serde_json::from_value(v).unwrap();
        // MatchInfo intentionally lacks PartialEq; compare the fields that the
        // round-trip must preserve exactly.
        assert_eq!(back.match_token, m.match_token);
        assert_eq!(back.player_a, m.player_a);
        assert_eq!(back.player_b, m.player_b);
        assert_eq!(back.game_type, m.game_type);
        assert_eq!(back.status, m.status);
        assert_eq!(back.join_token, m.join_token);
        assert_eq!(back.accepted_a, m.accepted_a);
        assert_eq!(back.connected_b, m.connected_b);
    }
}
