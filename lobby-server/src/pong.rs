//! Server-authoritative pong as a ROLLBACK REFEREE: one tokio task per active
//! p2p match ticks the shared `lobby_core::pong` logic, but only advances to
//! a frame once BOTH players' inputs for it have arrived (frame-gated). It
//! broadcasts the authoritative frame + checksum, acks confirmed frames, and
//! compares each client's reported checksum (RollbackHealth) against its own —
//! a mismatch triggers RollbackResync with the authoritative 74-byte state.
use std::collections::VecDeque;
use std::sync::Arc;

use lobby_core::pong::{PongGame, PongSide, DT_SECS, TICK_MS};

use lobby_core::traits::MatchStore;

use crate::state::AppState;

/// A single frame-stamped paddle-target update from a player.
pub struct PongInput {
    pub side: PongSide,
    pub target: f64,
    pub frame: u32,
}

/// A client's reported checksum for a frame (from `RollbackHealth`).
pub struct RollbackHealth {
    pub from: u64,
    pub frame: u32,
    pub checksum: u64,
}

/// Registry entry for a running pong match.
pub struct ActivePong {
    /// Participants, for disconnect-forfeit lookup.
    pub player_a: u64,
    pub player_b: u64,
    pub input_tx: tokio::sync::mpsc::UnboundedSender<PongInput>,
    pub health_tx: tokio::sync::mpsc::UnboundedSender<RollbackHealth>,
    pub abort: tokio::task::AbortHandle,
}

/// How many authoritative (frame, checksum) entries the referee keeps for
/// health comparison (clients only ever lag a few frames behind).
const HEALTH_RING: usize = 128;


/// Pop every queued input with `frame <= next`, applying the last as the
/// paddle target (inputs for older frames are superseded; between changes the
/// sim holds its goal — hold-last).
fn apply_side(queue: &mut VecDeque<(u32, f64)>, side: PongSide, next: u32, game: &mut PongGame) {
    let mut last = None;
    while let Some(&(f, t)) = queue.front() {
        if f <= next {
            queue.pop_front();
            last = Some(t);
        } else {
            break;
        }
    }
    if let Some(t) = last {
        game.set_target(side, t);
    }
}

/// Spawn the game task for a p2p match that just reached Reporting.
/// Idempotent: no-op if a game already exists for this token.
pub fn spawn_game(state: &Arc<AppState>, m: &lobby_core::types::MatchInfo) {
    let (input_tx, mut input_rx) = tokio::sync::mpsc::unbounded_channel::<PongInput>();
    let (health_tx, mut health_rx) = tokio::sync::mpsc::unbounded_channel::<RollbackHealth>();
    let task_state = state.clone();
    let token = m.match_token.clone();
    let task_token = token.clone();
    let players = [m.player_a, m.player_b];
    let handle = tokio::spawn(async move {
        let mut game = PongGame::new();
        // Per-side ordered (frame, target) queues; hold-last between changes.
        let mut left_inputs: VecDeque<(u32, f64)> = VecDeque::new();
        let mut right_inputs: VecDeque<(u32, f64)> = VecDeque::new();
        // Authoritative checksum ring, newest last (last HEALTH_RING frames).
        let mut checksums: VecDeque<(u32, u64)> = VecDeque::new();
        // Last frame advanced to; -1 = the initial state.
        let mut frame: i64 = -1;
        // Round hold: 3-2-1 countdown between points (0 = disabled). While > 0,
        // the sim is frozen and only the unchanged snapshot is broadcast.
        let countdown_ticks = task_state.config.pong_countdown_ticks;
        let mut hold_remaining: u32 = 0;
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(TICK_MS));
        // Bootstrap: broadcast the initial state once (frame 0). Each client
        // needs a frame to learn which side it is (player_a = Left) and create
        // its rollback session — without this, the referee (waiting for both
        // inputs) and the clients (waiting for the first frame) deadlock.
        {
            let snap = game.snapshot();
            let cksum = game.checksum();
            let connections = task_state.connections.lock().await;
            for pid in players {
                if let Some(e) = connections.get(&pid) {
                    let _ = e.tx.send(crate::ws::ServerMessage::GameState {
                        match_token: task_token.clone(),
                        frame: 0,
                        player_a: players[0],
                        player_b: players[1],
                        left_y: snap.left_y,
                        right_y: snap.right_y,
                        ball_x: snap.ball_x,
                        ball_y: snap.ball_y,
                        left_score: snap.left_score,
                        right_score: snap.right_score,
                        speed: snap.speed,
                        checksum: cksum.to_string(),
                    });
                }
            }
        }
        // Round 0 begins with the 3-2-1 hold (if enabled): the initial state
        // is the frozen frame 0; the ball launches at frame 0 + countdown_ticks.
        if countdown_ticks > 0 {
            hold_remaining = countdown_ticks;
            let connections = task_state.connections.lock().await;
            for pid in players {
                if let Some(e) = connections.get(&pid) {
                    let _ = e.tx.send(crate::ws::ServerMessage::RoundStart {
                        match_token: task_token.clone(),
                        frame: 0,
                        round: 0,
                        countdown_ticks,
                    });
                }
            }
        }
        loop {
            interval.tick().await;
            // 1. stop if the match left Reporting (report/expiry raced us)
            if let Ok(Some(match_info)) = task_state.store.get_match(&task_token).await {
                if match_info.status != lobby_core::types::MatchStatus::Reporting {
                    break;
                }
            }
            // 2. drain inputs into the per-side queues
            while let Ok(i) = input_rx.try_recv() {
                match i.side {
                    PongSide::Left => left_inputs.push_back((i.frame, i.target)),
                    PongSide::Right => right_inputs.push_back((i.frame, i.target)),
                }
            }
            // 3. drain pending health reports (processed even when stalled)
            let mut pending_health: Vec<RollbackHealth> = Vec::new();
            while let Ok(h) = health_rx.try_recv() {
                pending_health.push(h);
            }

            if hold_remaining > 0 {
                // Frozen frame: no input gate, no step. Broadcast the CURRENT
                // (unchanged) snapshot + ack so clients' confirmed frames
                // advance in lockstep during the 3-2-1.
                frame += 1;
                let snap = game.snapshot();   // unchanged during hold
                let cksum = game.checksum();  // constant
                {
                    let connections = task_state.connections.lock().await;
                    for pid in players {
                        if let Some(e) = connections.get(&pid) {
                            let _ = e.tx.send(crate::ws::ServerMessage::GameState {
                                match_token: task_token.clone(),
                                frame: frame as u32,
                                player_a: players[0],
                                player_b: players[1],
                                left_y: snap.left_y,
                                right_y: snap.right_y,
                                ball_x: snap.ball_x,
                                ball_y: snap.ball_y,
                                left_score: snap.left_score,
                                right_score: snap.right_score,
                                speed: snap.speed,
                                checksum: cksum.to_string(),
                            });
                        }
                    }
                }
                {
                    let connections = task_state.connections.lock().await;
                    for pid in players {
                        if let Some(e) = connections.get(&pid) {
                            let _ = e.tx.send(crate::ws::ServerMessage::InputAck {
                                match_token: task_token.clone(),
                                frame: frame as u32,
                            });
                        }
                    }
                }
                hold_remaining -= 1;
            } else {
                let next = (frame + 1) as u32;
                let left_ready = left_inputs.back().is_some_and(|(f, _)| *f >= next);
                let right_ready = right_inputs.back().is_some_and(|(f, _)| *f >= next);
                if left_ready && right_ready {
                    frame += 1;
                    apply_side(&mut left_inputs, PongSide::Left, next, &mut game);
                    apply_side(&mut right_inputs, PongSide::Right, next, &mut game);
                    // 4. advance physics + record the authoritative checksum
                    let before = game.snapshot(); // scores before step
                    game.step(DT_SECS);
                    let cksum = game.checksum();
                    checksums.push_back((next, cksum));
                    if checksums.len() > HEALTH_RING {
                        checksums.pop_front();
                    }
                    // 5. broadcast the authoritative frame to both players
                    let snap = game.snapshot();
                    {
                        let connections = task_state.connections.lock().await;
                        for pid in players {
                            if let Some(e) = connections.get(&pid) {
                                let _ = e.tx.send(crate::ws::ServerMessage::GameState {
                                    match_token: task_token.clone(),
                                    frame: next,
                                    player_a: players[0],
                                    player_b: players[1],
                                    left_y: snap.left_y,
                                    right_y: snap.right_y,
                                    ball_x: snap.ball_x,
                                    ball_y: snap.ball_y,
                                    left_score: snap.left_score,
                                    right_score: snap.right_score,
                                    speed: snap.speed,
                                    checksum: cksum.to_string(),
                                });
                            }
                        }
                    }
                    // 6. ack the confirmed frame to both players
                    {
                        let connections = task_state.connections.lock().await;
                        for pid in players {
                            if let Some(e) = connections.get(&pid) {
                                let _ = e.tx.send(crate::ws::ServerMessage::InputAck {
                                    match_token: task_token.clone(),
                                    frame: next,
                                });
                            }
                        }
                    }
                    // 7. point scored? (replaces the plain winner() check)
                    let scored = snap.left_score != before.left_score
                        || snap.right_score != before.right_score;
                    if scored {
                        if let Some(winner_side) = game.winner() {
                            let winner = match winner_side {
                                PongSide::Left => players[0],
                                PongSide::Right => players[1],
                            };
                            // Step 11: when Temporal owns the match, the
                            // workflow is the sole lifecycle writer — the
                            // referee only PLAYS BACK the game. The clients'
                            // who_won reports drive the workflow's
                            // finish_match; the referee just logs and exits
                            // (the match leaves Reporting once the workflow
                            // resolves, which breaks this loop naturally).
                            let temporal_up = task_state
                                .temporal
                                .read()
                                .ok()
                                .map_or(false, |g| g.is_some());
                            if temporal_up {
                                tracing::info!(
                                    "match {task_token} pong ended (winner {winner}) — \
                                     awaiting workflow finish"
                                );
                                break;
                            }
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
                        } else if countdown_ticks > 0 {
                            // Point scored, game not over: hold the sim frozen
                            // for the 3-2-1 countdown before the next round.
                            hold_remaining = countdown_ticks;
                            let connections = task_state.connections.lock().await;
                            for pid in players {
                                if let Some(e) = connections.get(&pid) {
                                    let _ = e.tx.send(crate::ws::ServerMessage::RoundStart {
                                        match_token: task_token.clone(),
                                        frame: next,
                                        round: (snap.left_score + snap.right_score) as u32,
                                        countdown_ticks,
                                    });
                                }
                            }
                        }
                    }
                }
            }

            // 8. compare client health against the authoritative ring
            for h in pending_health {
                let ours = checksums.iter().find(|(f, _)| *f == h.frame).map(|(_, c)| *c);
                if let Some(ours) = ours {
                    if ours != h.checksum {
                        tracing::warn!(
                            "desync detected match {task_token} player {} frame {}",
                            h.from,
                            h.frame
                        );
                        let state_hex = game
                            .full_state()
                            .iter()
                            .map(|b| format!("{b:02x}"))
                            .collect::<String>();
                        let connections = task_state.connections.lock().await;
                        if let Some(e) = connections.get(&h.from) {
                            let _ = e.tx.send(crate::ws::ServerMessage::RollbackResync {
                                match_token: task_token.clone(),
                                frame: frame as u32,
                                state: state_hex,
                            });
                        }
                    }
                }
            }
        }
        // always remove the registry entry on exit
        task_state.pong_games.lock().remove(&task_token);
    });
    // Register the game; if the two start_match messages raced, the first
    // spawn wins and this one is aborted.
    let mut games = state.pong_games.lock();
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
            health_tx,
            abort: handle.abort_handle(),
        },
    );
}
