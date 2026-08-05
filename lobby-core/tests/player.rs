mod common;

use common::{MockStore, TestCallbacks};
use lobby_core::error::LobbyError;
use lobby_core::player::PlayerManager;
use lobby_core::traits::PlayerStore;
use lobby_core::types::{MatchDifficulty, PlayerState};

fn mgr() -> PlayerManager<TestCallbacks> {
    PlayerManager::new(TestCallbacks)
}

async fn state(store: &MockStore, id: u64) -> PlayerState {
    store.get_player_state(id).await.unwrap().unwrap().state
}

#[tokio::test]
async fn first_login_creates_player_in_menus() {
    let store = MockStore::new();
    mgr().enter_menus(101, &store).await.unwrap();
    assert_eq!(state(&store, 101).await, PlayerState::InMenus);
}

#[tokio::test]
async fn enter_menus_is_idempotent_for_existing_players() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(102, &store).await.unwrap();
    // A second login must not clobber state (only first login creates).
    m.enter_menus(102, &store).await.unwrap();
    m.begin_matchmaking(102, MatchDifficulty::Normal, &store).await.unwrap();
    m.enter_menus(102, &store).await.unwrap();
    assert_eq!(state(&store, 102).await, PlayerState::Queueing);
}

#[tokio::test]
async fn full_state_machine_walk() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(103, &store).await.unwrap();
    m.begin_matchmaking(103, MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    assert_eq!(state(&store, 103).await, PlayerState::Queueing);
    m.match_accepted(103, &store).await.unwrap();
    assert_eq!(state(&store, 103).await, PlayerState::MatchAccepted);
    m.p2p_connected(103, &store).await.unwrap();
    assert_eq!(state(&store, 103).await, PlayerState::InMatch);
    m.begin_reporting(103, &store).await.unwrap();
    assert_eq!(state(&store, 103).await, PlayerState::Reporting);
    m.reporting_complete(103, &store).await.unwrap();
    assert_eq!(state(&store, 103).await, PlayerState::InMenus);
}

#[tokio::test]
async fn cancel_matchmaking_returns_to_menus() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(104, &store).await.unwrap();
    m.begin_matchmaking(104, MatchDifficulty::Easy, &store)
        .await
        .unwrap();
    m.cancel_matchmaking(104, &store).await.unwrap();
    assert_eq!(state(&store, 104).await, PlayerState::InMenus);
}

#[tokio::test]
async fn transitions_require_the_expected_prior_state() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(105, &store).await.unwrap();

    // InMenus -> MatchAccepted is invalid (must queue first).
    let err = m.match_accepted(105, &store).await.unwrap_err();
    assert!(matches!(
        err,
        LobbyError::InvalidStateTransition {
            from: PlayerState::InMenus,
            to: PlayerState::MatchAccepted
        }
    ));

    // Queueing -> Queueing is invalid (double queue).
    m.begin_matchmaking(105, MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    let err = m.begin_matchmaking(105, MatchDifficulty::Normal, &store).await.unwrap_err();
    assert!(matches!(
        err,
        LobbyError::InvalidStateTransition {
            from: PlayerState::Queueing,
            to: PlayerState::Queueing
        }
    ));

    // MatchAccepted -> InMenus via cancel is invalid.
    m.match_accepted(105, &store).await.unwrap();
    let err = m.cancel_matchmaking(105, &store).await.unwrap_err();
    assert!(matches!(
        err,
        LobbyError::InvalidStateTransition {
            from: PlayerState::MatchAccepted,
            to: PlayerState::InMenus
        }
    ));

    // Reporting must be reached from InMatch only.
    let err = m.begin_reporting(105, &store).await.unwrap_err();
    assert!(matches!(
        err,
        LobbyError::InvalidStateTransition {
            from: PlayerState::MatchAccepted,
            to: PlayerState::Reporting
        }
    ));
}

#[tokio::test]
async fn unknown_player_operations_error() {
    let store = MockStore::new();
    let m = mgr();

    let err = m.cancel_matchmaking(106, &store).await.unwrap_err();
    assert!(matches!(err, LobbyError::PlayerNotFound(106)));
    let err = m.match_accepted(106, &store).await.unwrap_err();
    assert!(matches!(err, LobbyError::PlayerNotFound(106)));
}

#[tokio::test]
async fn heartbeat_refreshes_liveness() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(107, &store).await.unwrap();
    let before = store.get_player_state(107).await.unwrap().unwrap().last_heartbeat;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    m.heartbeat(107, &store).await.unwrap();
    let after = store.get_player_state(107).await.unwrap().unwrap().last_heartbeat;
    assert!(after > before, "heartbeat must refresh last_heartbeat");
}

#[tokio::test]
async fn disconnect_resets_mid_match_player_to_menus() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(108, &store).await.unwrap();
    m.begin_matchmaking(108, MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    m.match_accepted(108, &store).await.unwrap();
    m.handle_disconnect(108, &store).await.unwrap();
    assert_eq!(state(&store, 108).await, PlayerState::InMenus);
}
