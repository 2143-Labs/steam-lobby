//! Storage-agnostic matchmaking core shared by the lobby server and tests.
//!
//! - `error` — `LobbyError` variants + `Result` alias.
//! - `match_lifecycle` — `MatchManager`'s player-facing actions; expiry lives in `match_expiry`.
//! - `mmr` — Weng-Lin rating updates.
//! - `player` — `PlayerManager`'s `PlayerState` machine.
//! - `queue` — matchmaking + expanding `search_band`.
//! - `traits` — the storage/callback traits the core depends on.
//! - `types` — wire types + Steam-ID serde helpers.
pub mod error;
pub mod match_lifecycle;
mod match_expiry;
pub mod mmr;
pub mod player;
pub mod queue;
pub mod traits;
pub mod types;
