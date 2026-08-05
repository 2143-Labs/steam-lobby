//! `PlayerManager` drives the per-player `PlayerState` state machine: every
//! transition validates the prior state and rejects illegal ones with
//! `LobbyError::InvalidStateTransition`.
use crate::error::{LobbyError, Result};
use crate::traits::{GameCallbacks, PlayerStore};
use crate::types::{MatchDifficulty, PlayerState, SteamId};

/// Drives a single player's `PlayerState` machine against a `PlayerStore`.
/// Every transition validates the current state first and rejects illegal
/// ones with `LobbyError::InvalidStateTransition`. The happy path is
/// `InMenus → Queueing → MatchAccepted → InMatch → Reporting → InMenus`;
/// `handle_disconnect` returns to `InMenus` from anywhere.
pub struct PlayerManager<CB: GameCallbacks> {
    callbacks: CB,
}

impl<CB: GameCallbacks> PlayerManager<CB> {
    /// Create a manager wired to the given game callbacks.
    pub fn new(callbacks: CB) -> Self {
        Self { callbacks }
    }

    /// Move a player into the menus (first login only): creates the player
    /// row at `InMenus` when none exists, otherwise a no-op. Idempotent.
    pub async fn enter_menus(
        &self,
        steam_id: SteamId,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        let current = player_store.get_player_state(steam_id).await?;
        if current.is_none() {
            // First login — create with InMenus.
            player_store.upsert_player(steam_id, "").await?;
            player_store
                .set_player_state(steam_id, PlayerState::InMenus)
                .await?;
        }
        self.callbacks.on_player_in_menu(steam_id).await?;
        Ok(())
    }

    /// `InMenus → Queueing`. A missing player row defaults to `InMenus`, so a
    /// player who skipped `enter_menus` can still queue; fires the
    /// `on_player_queueing` callback, then sets the state.
    pub async fn begin_matchmaking(
        &self,
        steam_id: SteamId,
        difficulty: MatchDifficulty,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        let current = player_store.get_player_state(steam_id).await?;
        let current_state = current
            .as_ref()
            .map(|p| p.state)
            .unwrap_or(PlayerState::InMenus);
        if current_state != PlayerState::InMenus {
            return Err(LobbyError::InvalidStateTransition {
                from: current_state,
                to: PlayerState::Queueing,
            });
        }
        self.callbacks
            .on_player_queueing(steam_id, "ranked_1v1", difficulty)
            .await?;
        player_store
            .set_player_state(steam_id, PlayerState::Queueing)
            .await?;
        Ok(())
    }

    /// `Queueing → InMenus`. Errors `PlayerNotFound` when no row exists.
    pub async fn cancel_matchmaking(
        &self,
        steam_id: SteamId,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        let current = player_store.get_player_state(steam_id).await?;
        let current_state = current
            .as_ref()
            .map(|p| p.state)
            .ok_or(LobbyError::PlayerNotFound(steam_id))?;
        if current_state != PlayerState::Queueing {
            return Err(LobbyError::InvalidStateTransition {
                from: current_state,
                to: PlayerState::InMenus,
            });
        }
        self.callbacks.on_player_cancel_queue(steam_id).await?;
        player_store
            .set_player_state(steam_id, PlayerState::InMenus)
            .await?;
        Ok(())
    }

    /// `Queueing → MatchAccepted` once the player accepts a found match.
    pub async fn match_accepted(
        &self,
        steam_id: SteamId,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        let current = player_store.get_player_state(steam_id).await?;
        let current_state = current
            .as_ref()
            .map(|p| p.state)
            .ok_or(LobbyError::PlayerNotFound(steam_id))?;
        if current_state != PlayerState::Queueing {
            return Err(LobbyError::InvalidStateTransition {
                from: current_state,
                to: PlayerState::MatchAccepted,
            });
        }
        player_store
            .set_player_state(steam_id, PlayerState::MatchAccepted)
            .await?;
        Ok(())
    }

    /// `MatchAccepted → InMatch` once both peers have connected.
    pub async fn p2p_connected(
        &self,
        steam_id: SteamId,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        let current = player_store.get_player_state(steam_id).await?;
        let current_state = current
            .as_ref()
            .map(|p| p.state)
            .ok_or(LobbyError::PlayerNotFound(steam_id))?;
        if current_state != PlayerState::MatchAccepted {
            return Err(LobbyError::InvalidStateTransition {
                from: current_state,
                to: PlayerState::InMatch,
            });
        }
        player_store
            .set_player_state(steam_id, PlayerState::InMatch)
            .await?;
        Ok(())
    }

    /// `InMatch → Reporting` when the player submits their match report.
    pub async fn begin_reporting(
        &self,
        steam_id: SteamId,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        let current = player_store.get_player_state(steam_id).await?;
        let current_state = current
            .as_ref()
            .map(|p| p.state)
            .ok_or(LobbyError::PlayerNotFound(steam_id))?;
        if current_state != PlayerState::InMatch {
            return Err(LobbyError::InvalidStateTransition {
                from: current_state,
                to: PlayerState::Reporting,
            });
        }
        player_store
            .set_player_state(steam_id, PlayerState::Reporting)
            .await?;
        Ok(())
    }

    /// `Reporting → InMenus`; the player is free to queue again.
    pub async fn reporting_complete(
        &self,
        steam_id: SteamId,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        let current = player_store.get_player_state(steam_id).await?;
        let current_state = current
            .as_ref()
            .map(|p| p.state)
            .ok_or(LobbyError::PlayerNotFound(steam_id))?;
        if current_state != PlayerState::Reporting {
            return Err(LobbyError::InvalidStateTransition {
                from: current_state,
                to: PlayerState::InMenus,
            });
        }
        player_store
            .set_player_state(steam_id, PlayerState::InMenus)
            .await?;
        Ok(())
    }

    /// No state transition — refreshes `last_heartbeat` (queue liveness).
    pub async fn heartbeat(&self, steam_id: SteamId, player_store: &dyn PlayerStore) -> Result<()> {
        self.callbacks.on_heartbeat(steam_id).await?;
        player_store.update_heartbeat(steam_id).await?;
        Ok(())
    }

    /// Any state → `InMenus`; called when the client disconnects.
    pub async fn handle_disconnect(
        &self,
        steam_id: SteamId,
        player_store: &dyn PlayerStore,
    ) -> Result<()> {
        self.callbacks.on_player_disconnected(steam_id).await?;
        player_store
            .set_player_state(steam_id, PlayerState::InMenus)
            .await?;
        Ok(())
    }
}
