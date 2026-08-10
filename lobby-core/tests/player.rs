mod common;

use common::{pid, MockStore, TestCallbacks};
use lobby_core::error::LobbyError;
use lobby_core::player::PlayerManager;
use lobby_core::traits::PlayerStore;
use lobby_core::types::{MatchDifficulty, PlayerState};

fn mgr() -> PlayerManager<TestCallbacks> {
    PlayerManager::new(TestCallbacks)
}

async fn state(store: &MockStore, id: lobby_core::types::PlayerId) -> PlayerState {
    store.get_player_state(id).await.unwrap().unwrap().state
}

#[tokio::test]
async fn first_login_creates_player_in_menus() {
    let store = MockStore::new();
    mgr().enter_menus(pid(101), &store).await.unwrap();
    assert_eq!(state(&store, pid(101)).await, PlayerState::InMenus);
}

#[tokio::test]
async fn enter_menus_is_idempotent_for_existing_players() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(pid(102), &store).await.unwrap();
    // A second login must not clobber state (only first login creates).
    m.enter_menus(pid(102), &store).await.unwrap();
    m.begin_matchmaking(pid(102), MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    m.enter_menus(pid(102), &store).await.unwrap();
    assert_eq!(state(&store, pid(102)).await, PlayerState::Queueing);
}

#[tokio::test]
async fn full_state_machine_walk() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(pid(103), &store).await.unwrap();
    m.begin_matchmaking(pid(103), MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    assert_eq!(state(&store, pid(103)).await, PlayerState::Queueing);
    m.match_accepted(pid(103), &store).await.unwrap();
    assert_eq!(state(&store, pid(103)).await, PlayerState::MatchAccepted);
    m.p2p_connected(pid(103), &store).await.unwrap();
    assert_eq!(state(&store, pid(103)).await, PlayerState::InMatch);
    m.begin_reporting(pid(103), &store).await.unwrap();
    assert_eq!(state(&store, pid(103)).await, PlayerState::Reporting);
    m.reporting_complete(pid(103), &store).await.unwrap();
    assert_eq!(state(&store, pid(103)).await, PlayerState::InMenus);
}

#[tokio::test]
async fn cancel_matchmaking_returns_to_menus() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(pid(104), &store).await.unwrap();
    m.begin_matchmaking(pid(104), MatchDifficulty::Easy, &store)
        .await
        .unwrap();
    m.cancel_matchmaking(pid(104), &store).await.unwrap();
    assert_eq!(state(&store, pid(104)).await, PlayerState::InMenus);
}

#[tokio::test]
async fn transitions_require_the_expected_prior_state() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(pid(105), &store).await.unwrap();

    // InMenus -> MatchAccepted is invalid (must queue first).
    let err = m.match_accepted(pid(105), &store).await.unwrap_err();
    assert!(matches!(
        err,
        LobbyError::InvalidStateTransition {
            from: PlayerState::InMenus,
            to: PlayerState::MatchAccepted
        }
    ));

    // Queueing -> Queueing is invalid (double queue).
    m.begin_matchmaking(pid(105), MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    let err = m
        .begin_matchmaking(pid(105), MatchDifficulty::Normal, &store)
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        LobbyError::InvalidStateTransition {
            from: PlayerState::Queueing,
            to: PlayerState::Queueing
        }
    ));

    // MatchAccepted -> InMenus via cancel is invalid.
    m.match_accepted(pid(105), &store).await.unwrap();
    let err = m.cancel_matchmaking(pid(105), &store).await.unwrap_err();
    assert!(matches!(
        err,
        LobbyError::InvalidStateTransition {
            from: PlayerState::MatchAccepted,
            to: PlayerState::InMenus
        }
    ));

    // Reporting must be reached from InMatch only.
    let err = m.begin_reporting(pid(105), &store).await.unwrap_err();
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

    let err = m.cancel_matchmaking(pid(106), &store).await.unwrap_err();
    let not_found = pid(106);
    assert!(matches!(err, LobbyError::PlayerNotFound(id) if id == not_found));
    let err = m.match_accepted(pid(106), &store).await.unwrap_err();
    let not_found = pid(106);
    assert!(matches!(err, LobbyError::PlayerNotFound(id) if id == not_found));
}

#[tokio::test]
async fn heartbeat_refreshes_liveness() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(pid(107), &store).await.unwrap();
    let before = store
        .get_player_state(pid(107))
        .await
        .unwrap()
        .unwrap()
        .last_heartbeat;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    m.heartbeat(pid(107), &store).await.unwrap();
    let after = store
        .get_player_state(pid(107))
        .await
        .unwrap()
        .unwrap()
        .last_heartbeat;
    assert!(after > before, "heartbeat must refresh last_heartbeat");
}

#[tokio::test]
async fn queueing_refreshes_liveness() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(pid(109), &store).await.unwrap();
    let before = store
        .get_player_state(pid(109))
        .await
        .unwrap()
        .unwrap()
        .last_heartbeat;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    m.begin_matchmaking(pid(109), MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    let after = store
        .get_player_state(pid(109))
        .await
        .unwrap()
        .unwrap()
        .last_heartbeat;
    assert!(
        after > before,
        "queueing must refresh last_heartbeat so the stale sweep cannot evict a just-queued entry"
    );
}

#[tokio::test]
async fn disconnect_resets_mid_match_player_to_menus() {
    let store = MockStore::new();
    let m = mgr();
    m.enter_menus(pid(108), &store).await.unwrap();
    m.begin_matchmaking(pid(108), MatchDifficulty::Normal, &store)
        .await
        .unwrap();
    m.match_accepted(pid(108), &store).await.unwrap();
    m.handle_disconnect(pid(108), &store).await.unwrap();
    assert_eq!(state(&store, pid(108)).await, PlayerState::InMenus);
}
