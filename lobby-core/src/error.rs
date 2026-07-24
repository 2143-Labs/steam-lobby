use crate::types::{PlayerState, SteamId};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LobbyError {
    #[error("player not found: {0}")]
    PlayerNotFound(SteamId),
    #[error("invalid state transition: from {from:?} to {to:?}")]
    InvalidStateTransition { from: PlayerState, to: PlayerState },
    #[error("match not found: {0}")]
    MatchNotFound(String),
    #[error("match is not in the expected state: {0}")]
    MatchStateMismatch(String),
    #[error("not a participant of match {0}")]
    NotParticipant(String),
    #[error("steam auth failed: {0}")]
    SteamAuthFailed(String),
    #[error("database error: {0}")]
    Database(String),
}

pub type Result<T> = std::result::Result<T, LobbyError>;
