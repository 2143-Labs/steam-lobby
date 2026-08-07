//! Server-authoritative pong: one tokio task per active p2p match ticks the
//! shared `lobby_core::pong` logic and broadcasts frames to both players.
use std::sync::Arc;

use lobby_core::pong::{PongGame, PongSide, DT_SECS, TICK_MS};

use lobby_core::traits::MatchStore;

use crate::state::AppState;

/// A single paddle-target update from a player.
pub struct PongInput {
    pub side: PongSide,
    pub target: f64,
}

/// Registry entry for a running pong match.
pub struct ActivePong {
    /// Participants, for disconnect-forfeit lookup.
    pub player_a: u64,
    pub player_b: u64,
    pub input_tx: tokio::sync::mpsc::UnboundedSender<PongInput>,
    pub abort: tokio::task::AbortHandle,
}

/// Spawn the game task for a p2p match that just reached Reporting.
/// Idempotent: no-op if a game already exists for this token.
pub fn spawn_game(state: &Arc<AppState>, m: &lobby_core::types::MatchInfo) {
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel::<PongInput>();
    let task_state = state.clone();
    let token = m.match_token.clone();
    let task_token = token.clone();
    let players = [m.player_a, m.player_b];
    let handle = tokio::spawn(async move {
        let mut game = PongGame::new();
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(TICK_MS));
        loop {
            interval.tick().await;
            // 1. stop if the match left Reporting (report/expiry raced us)
            if let Ok(Some(match_info)) = task_state.store.get_match(&task_token).await {
                if match_info.status != lobby_core::types::MatchStatus::Reporting {
                    break;
                }
            }
            // 2. drain inputs
            while let Ok(i) = input_rx.try_recv() {
                game.set_target(i.side, i.target);
            }
            // 3. step physics
            game.step(DT_SECS);
            // 4. broadcast the authoritative frame to both players' connections
            let snap = game.snapshot();
            {
                let connections = task_state.connections.lock().await;
                for pid in players {
                    if let Some(e) = connections.get(&pid) {
                        let _ = e.tx.send(crate::ws::ServerMessage::GameState {
                            match_token: task_token.clone(),
                            player_a: players[0],
                            player_b: players[1],
                            left_y: snap.left_y,
                            right_y: snap.right_y,
                            ball_x: snap.ball_x,
                            ball_y: snap.ball_y,
                            left_score: snap.left_score,
                            right_score: snap.right_score,
                            speed: snap.speed,
                        });
                    }
                }
            }
            // 5. first to 3 -> declare winner, auto-resolve, notify, exit
            if let Some(winner_side) = game.winner() {
                let winner = match winner_side {
                    PongSide::Left => players[0],
                    PongSide::Right => players[1],
                };
                let outcome = task_state
                    .match_manager
                    .resolve_pong(
                        &task_token,
                        winner,
                        &task_state.store,
                        &task_state.store,
                        &task_state.store,
                    )
                    .await;
                tracing::info!("match {task_token} pong ended, winner {winner}: {outcome:?}");
                // GameOver to both (best-effort), then match_result like the report path does
                let connections = task_state.connections.lock().await;
                for pid in players {
                    if let Some(e) = connections.get(&pid) {
                        let _ = e.tx.send(crate::ws::ServerMessage::GameOver {
                            match_token: task_token.clone(),
                            winner,
                        });
                        if let Ok(o) = &outcome {
                            let _ = e.tx.send(crate::ws::ServerMessage::MatchResult {
                                match_token: task_token.clone(),
                                outcome: serde_json::to_value(o).unwrap(),
                            });
                        }
                    }
                }
                break;
            }
        }
        // always remove the registry entry on exit
        task_state.pong_games.lock().unwrap().remove(&task_token);
    });
    // Register the game; if the two p2p_connected messages raced, the first
    // spawn wins and this one is aborted.
    let mut games = state.pong_games.lock().unwrap();
    if games.contains_key(&token) {
        handle.abort();
        return;
    }
    games.insert(
        token.clone(),
        ActivePong {
            player_a: players[0],
            player_b: players[1],
            input_tx,
            abort: handle.abort_handle(),
        },
    );
}
