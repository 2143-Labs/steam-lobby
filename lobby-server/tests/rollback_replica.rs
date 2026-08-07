//! The true 3-way rollback test against the real server + real WebSocket:
//! two `LobbyClient`s (the test-side stand-in for the JS engine) each run a
//! bit-exact `PongGame` replica, the server runs the authoritative referee.
//!
//! - `pong_three_replicas_converge`: both replicas must match the referee's
//!   checksum frame-by-frame (asserted on every InputAck), and both clients
//!   must agree on the GameOver winner.
//! - `pong_divergence_detected_and_resynced`: client A deliberately applies a
//!   wrong target for one frame; its health reports diverge, the referee sends
//!   RollbackResync, and after restore + replay of its buffered inputs A
//!   re-converges — the eventual winner is unchanged.
//!
//! Requires Postgres (run `just db-up` first), like the rest of the itest
//! suite.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use lobby_client::{LobbyClient, ServerEvent};
use lobby_core::pong::{PongGame, PongSide, DT_SECS};
use tokio::time::timeout;

mod common; // lobby-server/tests/common.rs — TestHarness + setup_pong()
use common::setup_pong;

/// The shared deterministic input schedule (same formula as the JS/Rust
/// determinism gauntlet; off: left = 0, right = 331).
fn schedule(frame: u32, side: PongSide) -> f64 {
    let off = match side {
        PongSide::Left => 0u32,
        PongSide::Right => 331u32,
    };
    ((((frame / 5) * 7919) + off) % 997) as f64 / 997.0
}

fn hex_decode(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap())
        .collect()
}

/// The client-side replica + its driver: applies my schedule inputs (optionally
/// one deliberately wrong frame) and the peer's relayed inputs, stepping the
/// sim in lockstep with the server's acks.
struct ReplicaDriver {
    sim: PongGame,
    side: PongSide,
    other: PongSide,
    stepped: i64, // last frame stepped through (-1 = initial state)
    peer: BTreeMap<u32, f64>,
    peer_last: Option<f64>, // hold-last between peer inputs
    diverge_at: Option<u32>,
    diverged: bool,
}

impl ReplicaDriver {
    fn new(side: PongSide, diverge_at: Option<u32>) -> Self {
        Self {
            sim: PongGame::new(),
            side,
            other: match side {
                PongSide::Left => PongSide::Right,
                PongSide::Right => PongSide::Left,
            },
            stepped: -1,
            peer: BTreeMap::new(),
            peer_last: None,
            diverge_at,
            diverged: false,
        }
    }

    fn peer_input(&mut self, frame: u32, target: f64) {
        self.peer.insert(frame, target);
        self.peer_last = Some(target);
    }

    /// Step the replica through `frame` (inclusive) using the schedule for my
    /// side (with one wrong target at `diverge_at` if configured) and the
    /// peer's real inputs (hold-last between).
    fn step_to(&mut self, frame: u32) {
        while self.stepped < frame as i64 {
            let next = (self.stepped + 1) as u32;
            let mine = if self.diverge_at == Some(next) {
                self.diverge_at = None; // one wrong frame only
                self.diverged = true;
                0.99 // obviously differs from schedule(next, side)
            } else {
                schedule(next, self.side)
            };
            self.sim.set_target(self.side, mine);
            let peer_t = self.peer.get(&next).copied().or(self.peer_last);
            if let Some(t) = peer_t {
                self.sim.set_target(self.other, t);
            }
            self.sim.step(DT_SECS);
            self.stepped += 1;
        }
    }

    /// Resync from the referee's authoritative state at `frame`.
    fn restore_from(&mut self, frame: u32, state: &[u8]) {
        let mut bytes = [0u8; 74];
        bytes.copy_from_slice(state);
        self.sim.restore(&bytes);
        self.stepped = frame as i64;
        self.peer.retain(|f, _| *f > frame);
        self.diverged = false;
    }

    fn checksum(&self) -> u64 {
        self.sim.checksum()
    }
}

/// Drive one client until GameOver or the deadline: learns its side from the
/// first GameState, blasts schedule inputs for all frames, and on every
/// InputAck steps its replica and compares checksums with the referee (the
/// divergence is asserted VISIBLE while diverged, then EQUAL after resync).
/// Returns (winner, side, resynced).
async fn drive_client(
    p: &mut LobbyClient,
    token: &str,
    my_id: u64,
    diverge_at: Option<u32>,
    deadline: Instant,
) -> (Option<u64>, PongSide, bool) {
    let mut driver: Option<ReplicaDriver> = None;
    let mut server_checksums: HashMap<u32, u64> = HashMap::new();
    let mut last_state_frame: Option<u32> = None;
    let mut winner: Option<u64> = None;
    let mut resynced = false;

    while Instant::now() < deadline {
        match timeout(Duration::from_secs(5), p.next_event()).await {
            Ok(Some(Ok(ServerEvent::GameState { frame, player_a, checksum, .. }))) => {
                if driver.is_none() {
                    let side = if my_id == player_a { PongSide::Left } else { PongSide::Right };
                    driver = Some(ReplicaDriver::new(side, diverge_at));
                    // Blast all schedule inputs up front; the referee consumes
                    // them at 33ms/frame and gates on both sides.
                    let my_side = side;
                    for f in 0..600 {
                        p.send_game_input(token, f, schedule(f, my_side)).await.unwrap();
                    }
                }
                if let Some(prev) = last_state_frame {
                    assert!(frame >= prev, "GameState frames must be monotonic");
                }
                last_state_frame = Some(frame);
                server_checksums.insert(frame, checksum.parse::<u64>().unwrap());
            }
            Ok(Some(Ok(ServerEvent::PeerInput { frame, target, .. }))) => {
                if let Some(d) = driver.as_mut() {
                    d.peer_input(frame, target);
                }
            }
            Ok(Some(Ok(ServerEvent::InputAck { frame, .. }))) => {
                if let Some(d) = driver.as_mut() {
                    d.step_to(frame);
                    let local = d.checksum();
                    let srv = server_checksums.get(&frame).copied();
                    if let Some(srv) = srv {
                        if d.diverged {
                            assert_ne!(
                                local, srv,
                                "diverged replica must NOT match the referee at frame {frame}"
                            );
                        } else {
                            assert_eq!(
                                local, srv,
                                "replica diverged from the referee at frame {frame}"
                            );
                        }
                    }
                    // Referee health check, exactly like the demo does.
                    p.send_rollback_health(token, frame, local).await.unwrap();
                }
            }
            Ok(Some(Ok(ServerEvent::RollbackResync { frame, state, .. }))) => {
                if let Some(d) = driver.as_mut() {
                    d.restore_from(frame, &hex_decode(&state));
                    resynced = true;
                }
            }
            Ok(Some(Ok(ServerEvent::GameOver { winner: w, .. }))) => {
                winner = Some(w);
                break;
            }
            Ok(Some(Ok(_))) => {}
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    let d = driver.expect("client never learned its side (no GameState)");
    (winner, d.side, resynced)
}

#[tokio::test]
async fn pong_three_replicas_converge() {
    let h = setup_pong().await;
    let (mut p1, mut p2, token) = pair_up(&h, 110, 210, "ranked_1v1").await;
    accept_and_connect(&h, &mut p1, &mut p2, &token).await;
    let deadline = Instant::now() + Duration::from_secs(30);
    let (w1, side1, _) = drive_client(&mut p1, &token, 110, None, deadline).await;
    let (w2, side2, _) = drive_client(&mut p2, &token, 210, None, deadline).await;
    assert_ne!(side1, side2, "the two clients must be on opposite sides");

    let w1 = w1.expect("p1 must receive GameOver (the schedule wins by frame ~51)");
    let w2 = w2.expect("p2 must receive GameOver");
    assert_eq!(w1, w2, "both clients must agree on the winner");
    // The schedule is deterministic: Left wins. The winner is whoever drew
    // the Left side (player_a), which is racy.
    let left_player = if side1 == PongSide::Left { 110 } else { 210 };
    assert_eq!(w1, left_player, "the Left player must win (schedule outcome)");

    drop(p1);
    drop(p2);
}

#[tokio::test]
async fn pong_divergence_detected_and_resynced() {
    let h = setup_pong().await;
    let (mut p1, mut p2, token) = pair_up(&h, 110, 210, "ranked_1v1").await;
    accept_and_connect(&h, &mut p1, &mut p2, &token).await;
    let deadline = Instant::now() + Duration::from_secs(30);

    let (w1, side1, resynced) = drive_client(&mut p1, &token, 110, Some(25), deadline).await;
    let (w2, side2, _) = drive_client(&mut p2, &token, 210, None, deadline).await;
    assert_ne!(side1, side2, "the two clients must be on opposite sides");

    assert!(resynced, "the referee must detect p1's divergence and resync it");
    let w1 = w1.expect("p1 must receive GameOver");
    let w2 = w2.expect("p2 must receive GameOver");
    assert_eq!(w1, w2, "both clients must agree on the winner");
    let left_player = if side1 == PongSide::Left { 110 } else { 210 };
    assert_eq!(w1, left_player, "the divergence must not change the outcome");

    drop(p1);
    drop(p2);
}

// ── helpers copied from integration.rs (the itest crate has no shared lib) ──

use sqlx::PgPool;

async fn pair_up(h: &common::TestHarness, p1_id: u64, p2_id: u64, mode: &str) -> (LobbyClient, LobbyClient, String) {
    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    p1.authenticate_test_token(p1_id, &h.base_url).await.unwrap();
    p2.authenticate_test_token(p2_id, &h.base_url).await.unwrap();

    p1.begin_matchmaking(mode, "normal").await.unwrap();
    p2.begin_matchmaking(mode, "normal").await.unwrap();

    // Ticker runs every 2s; wait up to 15s.
    let m1 = timeout(Duration::from_secs(15), p1.wait_for_match())
        .await
        .expect("p1 match within 15s")
        .unwrap()
        .unwrap();
    let m2 = timeout(Duration::from_secs(15), p2.wait_for_match())
        .await
        .expect("p2 match within 15s")
        .unwrap()
        .unwrap();
    assert_eq!(m1.match_token, m2.match_token, "both players must get the same match");
    (p1, p2, m1.match_token)
}

async fn accept_and_connect(
    h: &common::TestHarness,
    p1: &mut LobbyClient,
    p2: &mut LobbyClient,
    token: &str,
) {
    p1.accept_match(token).await.unwrap();
    p2.accept_match(token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, token, "InProgress").await,
        "both accepts must transition the match to InProgress"
    );

    p1.p2p_connected(token).await.unwrap();
    p2.p2p_connected(token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, token, "Reporting").await,
        "both connections must transition the match to Reporting"
    );
}

async fn wait_for_status(pool: &PgPool, token: &str, expected: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status: Option<String> = sqlx::query_scalar(
            "SELECT status FROM matches WHERE match_token = $1",
        )
        .bind(token)
        .fetch_optional(pool)
        .await
        .expect("query matches");
        if status.as_deref() == Some(expected) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
