//! Server-authoritative ROCK PAPER SCISSORS as a simple REFEREE: one tokio
//! task per active p2p match broadcasts the round prompt (`RpsBegin`), waits
//! for both players' `RpsChoice` (10s window, relayed from the ws layer), then
//! resolves the round server-side and broadcasts `RpsRound` with the verdict
//! + score. First to 3 wins; a player who skips a round loses it (the other
//! scores). The referee is the authority on the winner, so at match end it
//! drives the Temporal workflow's who_won path directly (both players report
//! the same winner), mirroring pong's resolution flow otherwise.
use std::sync::Arc;

use lobby_core::traits::MatchStore;

use crate::state::AppState;

/// A player's round choice, relayed from the ws layer.
pub struct RpsChoice {
    pub from: uuid::Uuid,
    /// 0 = rock, 1 = paper, 2 = scissors. Anything else is rejected upstream.
    pub choice: u8,
}

/// Registry entry for a running RPS match.
pub struct ActiveRps {
    pub player_a: uuid::Uuid,
    pub player_b: uuid::Uuid,
    pub choice_tx: tokio::sync::mpsc::UnboundedSender<RpsChoice>,
    pub abort: tokio::task::AbortHandle,
}

/// Choices a client may send.
pub const ROCK: u8 = 0;
pub const PAPER: u8 = 1;
pub const SCISSORS: u8 = 2;
/// Sentinel broadcast for a player who did not choose before the window.
pub const NO_CHOICE: u8 = 255;

/// Round window per player, ms (broadcast in `RpsBegin` so the client can
/// render its own countdown).
const ROUND_TIMEOUT_MS: u64 = 10_000;
/// How long a resolved round's verdict stays on screen before the next
/// `RpsBegin` (ms).
const ROUND_PAUSE_MS: u64 = 2_000;
/// Referee tick.
const TICK_MS: u64 = 500;
/// First to this many round wins takes the match.
const FIRST_TO: u8 = 3;
/// Hard cap on rounds; at the cap the leader wins, a tie resolves nowhere
/// (the workflow's report timer disputes it).
const MAX_ROUNDS: u32 = 15;

/// Pure round resolution: `Some(0)` = a wins, `Some(1)` = b wins, `None` = draw.
/// rock beats scissors, scissors beats paper, paper beats rock.
fn resolve_round(a: u8, b: u8) -> Option<u8> {
    if a >= 3 || b >= 3 {
        return None; // NO_CHOICE (255) or any garbage: never a win
    }
    if a == b {
        return None;
    }
    Some(if b == (a + 1) % 3 { 1 } else { 0 })
}

/// Spawn the game task for a p2p match that just reached Reporting.
/// Idempotent: no-op if a game already exists for this token.
pub fn spawn_game(state: &Arc<AppState>, m: &lobby_core::types::MatchInfo) {
    let (choice_tx, mut choice_rx) = tokio::sync::mpsc::unbounded_channel::<RpsChoice>();
    let task_state = state.clone();
    let token = m.match_token.clone();
    let task_token = token.clone();
    let players = [m.player_a, m.player_b];
    let handle = tokio::spawn(async move {
        let mut round: u32 = 0;
        let mut a_choice: Option<u8> = None;
        let mut b_choice: Option<u8> = None;
        let mut a_score: u8 = 0;
        let mut b_score: u8 = 0;
        // Deadline for the current round's choices; None while between rounds.
        let mut round_deadline: Option<std::time::Instant> = None;
        // When the round verdict should give way to the next `RpsBegin`.
        let mut next_begin_at: Option<std::time::Instant> = None;
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(TICK_MS));
        let mut done = false;
        while !done {
            interval.tick().await;
            // Stop if the match left Reporting (report/expiry/dispute raced us):
            // the workflow is the lifecycle writer once Temporal is up.
            if let Ok(Some(match_info)) = task_state.store.get_match(&task_token).await
                && match_info.status != lobby_core::types::MatchStatus::Reporting
            {
                break;
            }
            // Drain choices; ignore anything for a player already locked in.
            while let Ok(c) = choice_rx.try_recv() {
                if c.choice > 2 {
                    continue;
                }
                if c.from == players[0] {
                    if a_choice.is_none() {
                        a_choice = Some(c.choice);
                    }
                } else if c.from == players[1] && b_choice.is_none() {
                    b_choice = Some(c.choice);
                }
            }
            let now = std::time::Instant::now();
            // Round 0 opens immediately on spawn (both players connected).
            if round == 0 && round_deadline.is_none() && next_begin_at.is_none() {
                broadcast(
                    &task_state,
                    &players,
                    &task_token,
                    crate::ws::ServerMessage::RpsBegin {
                        match_token: task_token.clone(),
                        round,
                        timeout_ms: ROUND_TIMEOUT_MS,
                        player_a: players[0].to_string(),
                        player_b: players[1].to_string(),
                    },
                )
                .await;
                round_deadline = Some(now + std::time::Duration::from_millis(ROUND_TIMEOUT_MS));
                continue;
            }
            // A resolved round's verdict has been on screen long enough:
            // open the next round.
            if let Some(t) = next_begin_at
                && now >= t
            {
                round += 1;
                a_choice = None;
                b_choice = None;
                next_begin_at = None;
                broadcast(
                    &task_state,
                    &players,
                    &task_token,
                    crate::ws::ServerMessage::RpsBegin {
                        match_token: task_token.clone(),
                        round,
                        timeout_ms: ROUND_TIMEOUT_MS,
                        player_a: players[0].to_string(),
                        player_b: players[1].to_string(),
                    },
                )
                .await;
                round_deadline = Some(now + std::time::Duration::from_millis(ROUND_TIMEOUT_MS));
                continue;
            }
            // Resolve the round once both chose or the window expires.
            let Some(deadline) = round_deadline else {
                continue;
            };
            if (a_choice.is_some() && b_choice.is_some()) || now >= deadline {
                let a = a_choice.unwrap_or(NO_CHOICE);
                let b = b_choice.unwrap_or(NO_CHOICE);
                // A missing choice loses the round to the player who chose.
                let winner = if a == NO_CHOICE && b == NO_CHOICE {
                    None
                } else if a == NO_CHOICE {
                    Some(1u8)
                } else if b == NO_CHOICE {
                    Some(0u8)
                } else {
                    resolve_round(a, b)
                };
                if winner == Some(0) {
                    a_score += 1;
                } else if winner == Some(1) {
                    b_score += 1;
                }
                let winner_id = winner.map(|w| players[w as usize].to_string());
                broadcast(
                    &task_state,
                    &players,
                    &task_token,
                    crate::ws::ServerMessage::RpsRound {
                        match_token: task_token.clone(),
                        round,
                        a_choice: a,
                        b_choice: b,
                        winner: winner_id.clone(),
                        a_score,
                        b_score,
                    },
                )
                .await;
                round_deadline = None;
                // Match over?
                if a_score >= FIRST_TO || b_score >= FIRST_TO {
                    let winner_id = winner_id.unwrap_or_else(|| {
                        if a_score >= FIRST_TO {
                            players[0].to_string()
                        } else {
                            players[1].to_string()
                        }
                    });
                    finish(&task_state, &players, &task_token, winner_id).await;
                    done = true;
                    break;
                }
                // Hard cap: leader wins; tie resolves nowhere (workflow disputes).
                if round + 1 >= MAX_ROUNDS {
                    if a_score != b_score {
                        let leader = if a_score > b_score {
                            players[0]
                        } else {
                            players[1]
                        };
                        finish(&task_state, &players, &task_token, leader.to_string()).await;
                    }
                    done = true;
                    break;
                }
                next_begin_at =
                    Some(now + std::time::Duration::from_millis(ROUND_PAUSE_MS));
            }
        }
        // always remove the registry entry on exit
        task_state.rps_games.lock().remove(&task_token);
    });
    // Register the game; if the two start_match messages raced, the first
    // spawn wins and this one is aborted.
    let mut games = state.rps_games.lock();
    if games.contains_key(&token) {
        handle.abort();
        return;
    }
    games.insert(
        token.clone(),
        ActiveRps {
            player_a: players[0],
            player_b: players[1],
            choice_tx,
            abort: handle.abort_handle(),
        },
    );
}

/// Broadcast a message to both players (best-effort; skips disconnected).
async fn broadcast(
    state: &Arc<AppState>,
    players: &[uuid::Uuid; 2],
    token: &str,
    msg: crate::ws::ServerMessage,
) {
    let connections = state.connections.lock().await;
    for pid in players {
        if let Some(e) = connections.get(pid) {
            let _ = e.tx.send(msg.clone());
        }
    }
}

/// End the match: broadcast `GameOver`; then, with Temporal up, the referee
/// (the sole authority on the RPS winner) signals who_won for BOTH players so
/// the workflow's agree-path resolves cleanly. Without Temporal, resolve
/// in-process like pong's fallback.
async fn finish(
    state: &Arc<AppState>,
    players: &[uuid::Uuid; 2],
    token: &str,
    winner: String,
) {
    broadcast(
        state,
        players,
        token,
        crate::ws::ServerMessage::GameOver {
            match_token: token.to_string(),
            winner: winner.clone(),
        },
    )
    .await;
    let temporal_up = state.temporal.read().ok().is_some_and(|g| g.is_some());
    if temporal_up {
        tracing::info!("match {token} rps ended (winner {winner}) — signaling workflow");
        for pid in players {
            if let Ok(w) = uuid::Uuid::parse_str(&winner) {
                crate::temporal::signals::signal_who_won(state, token, *pid, w).await;
            }
        }
    } else if let Ok(w) = uuid::Uuid::parse_str(&winner) {
        let outcome = state
            .match_manager
            .resolve_pong(token, w, &state.store, &state.store, &state.store)
            .await;
        tracing::info!("match {token} rps ended, winner {winner}: {outcome:?}");
        broadcast(
            state,
            players,
            token,
            crate::ws::ServerMessage::MatchResult {
                match_token: token.to_string(),
                outcome: serde_json::to_value(outcome).unwrap_or(serde_json::Value::Null),
            },
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_round_basics() {
        // Draws
        assert_eq!(resolve_round(ROCK, ROCK), None);
        assert_eq!(resolve_round(PAPER, PAPER), None);
        assert_eq!(resolve_round(SCISSORS, SCISSORS), None);
        // a wins: rock>scissors, scissors>paper, paper>rock
        assert_eq!(resolve_round(ROCK, SCISSORS), Some(0));
        assert_eq!(resolve_round(SCISSORS, PAPER), Some(0));
        assert_eq!(resolve_round(PAPER, ROCK), Some(0));
        // b wins: the mirror images
        assert_eq!(resolve_round(SCISSORS, ROCK), Some(1));
        assert_eq!(resolve_round(PAPER, SCISSORS), Some(1));
        assert_eq!(resolve_round(ROCK, PAPER), Some(1));
    }

    #[test]
    fn resolve_round_garbage_is_a_draw() {
        assert_eq!(resolve_round(NO_CHOICE, ROCK), None);
        assert_eq!(resolve_round(ROCK, NO_CHOICE), None);
        assert_eq!(resolve_round(255, 255), None);
        assert_eq!(resolve_round(7, 2), None);
    }
}
