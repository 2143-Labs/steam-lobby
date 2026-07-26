use crate::error::{LobbyError, Result};
use crate::traits::{GameCallbacks, PlayerStore};
use crate::types::{MatchDifficulty, PlayerState, SteamId};

pub struct PlayerManager<CB: GameCallbacks> {
    callbacks: CB,
}

impl<CB: GameCallbacks> PlayerManager<CB> {
    pub fn new(callbacks: CB) -> Self {
        Self { callbacks }
    }

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

    pub async fn heartbeat(&self, steam_id: SteamId, player_store: &dyn PlayerStore) -> Result<()> {
        self.callbacks.on_heartbeat(steam_id).await?;
        player_store.update_heartbeat(steam_id).await?;
        Ok(())
    }

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
