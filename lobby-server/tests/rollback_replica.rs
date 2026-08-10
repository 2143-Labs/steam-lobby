//! The true 3-way rollback test against the real server + real WebSocket:
//! two `LobbyClient`s (the test-side stand-in for the JS engine) each run a
//! bit-exact `PongGame` replica, the server runs the authoritative referee.
//!
//! - `pong_three_replicas_converge`: both replicas must match the referee's
//!   checksum frame-by-frame (asserted on every InputAck), and both clients
//!   must agree on the GameOver winner.
//! - `pong_divergence_detected_and_resynced`: client A deliberately applies a
//!   wrong target for every frame from 25 on (persistent — a one-frame
//!   wrongness is erased when the paddles settle at the same clamped position,
//!   so it would never be observable); its health reports diverge, the referee
//!   sends RollbackResync, and after restore + replay of its buffered inputs A
//!   re-converges — the eventual winner is unchanged.
//!
//! Requires Postgres (run `just db-up` first), like the rest of the itest
//! suite.

use std::collections::{BTreeMap, HashMap};
use std::time::{Duration, Instant};

use lobby_client::{LobbyClient, ServerEvent};
use lobby_core::pong::{DT_SECS, PongGame, PongSide};
use tokio::time::timeout;

mod common; // lobby-server/tests/common.rs — TestHarness + setup_pong()
use common::setup_temporal_pong;

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

    fn peer_input(&mut self, frame: u32, target: &str) {
        // The wire carries the target as its shortest round-trip decimal
        // string; `str::parse` is correctly rounded (serde_json's f64 parser
        // is off by 1 ULP for some values — a real determinism break).
        let target: f64 = target.parse().expect("valid target string");
        self.peer.insert(frame, target);
        self.peer_last = Some(target);
    }

    /// Step the replica through `frame` (inclusive) using the schedule for my
    /// side and the peer's real inputs (hold-last between).
    ///
    /// With `diverge_at` set, EVERY frame from there on applies a wrong target
    /// (`0.99` instead of the schedule): a one-frame wrongness is NOT reliably
    /// observable — the next frame's correct target overwrites it, and both
    /// paddles settle at the same clamped position within a couple of frames,
    /// erasing the divergence before the referee's resync can land. A
    /// persistent wrong target keeps the target field itself differing from
    /// the referee's state, so the checksums provably diverge until the
    /// referee detects the mismatch and resyncs us.
    fn step_to(&mut self, frame: u32) {
        while self.stepped < frame as i64 {
            let next = (self.stepped + 1) as u32;
            let mine = if self.diverged || self.diverge_at == Some(next) {
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
    pid: &str,
    diverge_at: Option<u32>,
    deadline: Instant,
) -> (Option<String>, PongSide, bool) {
    let mut driver: Option<ReplicaDriver> = None;
    let mut server_checksums: HashMap<u32, u64> = HashMap::new();
    let mut last_state_frame: Option<u32> = None;
    let mut winner: Option<String> = None;
    let mut resynced = false;

    while Instant::now() < deadline {
        match timeout(Duration::from_secs(5), p.next_event()).await {
            Ok(Some(Ok(ServerEvent::GameState {
                frame,
                player_a,
                checksum,
                ..
            }))) => {
                if driver.is_none() {
                    let side = if pid == player_a {
                        PongSide::Left
                    } else {
                        PongSide::Right
                    };
                    driver = Some(ReplicaDriver::new(side, diverge_at));
                    // Blast all schedule inputs up front; the referee consumes
                    // them at 33ms/frame and gates on both sides.
                    let my_side = side;
                    for f in 0..600 {
                        p.send_game_input(token, f, schedule(f, my_side))
                            .await
                            .unwrap();
                    }
                }
                if let Some(prev) = last_state_frame {
                    assert!(frame >= prev, "GameState frames must be monotonic");
                }
                last_state_frame = Some(frame);
                server_checksums.insert(frame, checksum.parse::<u64>().unwrap());
            }
            Ok(Some(Ok(ServerEvent::PeerInput { frame, target, .. }))) => {
                // The opponent's real input for `frame` — feed the replica.
                if let Some(d) = driver.as_mut() {
                    d.peer_input(frame, &target);
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
                                "diverged replica must NOT match the referee at frame {frame} (id {pid}, side {:?})",
                                d.side
                            );
                        } else {
                            assert_eq!(
                                local, srv,
                                "replica diverged from the referee at frame {frame} (id {pid}, side {:?})",
                                d.side
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

/// After both clients report the same winner, the workflow's finish_match
/// broadcasts GameOver — wait for it.
async fn wait_game_over(p: &mut LobbyClient, deadline: std::time::Instant) -> String {
    while std::time::Instant::now() < deadline {
        match timeout(Duration::from_secs(5), p.next_event()).await {
            Ok(Some(Ok(ServerEvent::GameOver { winner, .. }))) => return winner,
            Ok(Some(Ok(_))) | Err(_) => continue,
            Ok(Some(Err(e))) => panic!("client error: {e}"),
            Ok(None) => panic!("connection closed while waiting for GameOver"),
        }
    }
    panic!("GameOver not received within deadline");
}

#[sqlx::test]
async fn pong_three_replicas_converge(pool: sqlx::PgPool) {
    let h = setup_temporal_pong(pool).await;
    let (mut p1, mut p2, token, pid1, pid2) = pair_up(&h, 915, 916, "ranked_1v1").await;
    accept_and_connect(&h, &mut p1, &mut p2, &token).await;

    // Both clients must drive CONCURRENTLY: the referee only advances when
    // both players' inputs for the next frame are in, so a sequential drive
    // would leave one client silent and the game permanently stalled.
    // The referee is playback-only: the workflow resolves on the clients'
    // who_won reports. The schedule is deterministic — the Left player wins —
    // so both clients report the Left player after the drive.
    let deadline = Instant::now() + Duration::from_secs(10);
    let ((_, side1, _), (_, side2, _)) = tokio::join!(
        drive_client(&mut p1, &token, &pid1, None, deadline),
        drive_client(&mut p2, &token, &pid2, None, deadline),
    );
    assert_ne!(side1, side2, "the two clients must be on opposite sides");

    let left_player = if side1 == PongSide::Left { pid1 } else { pid2 };
    p1.submit_report(&token, Some(&left_player), Some("demo-a"))
        .await
        .unwrap();
    p2.submit_report(&token, Some(&left_player), Some("demo-b"))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    let w1 = wait_game_over(&mut p1, deadline).await;
    let w2 = wait_game_over(&mut p2, deadline).await;
    assert_eq!(w1, w2, "both clients must agree on the winner");
    assert_eq!(
        w1, left_player,
        "the Left player must win (schedule outcome)"
    );

    drop(p1);
    drop(p2);
}

#[sqlx::test]
async fn pong_divergence_detected_and_resynced(pool: sqlx::PgPool) {
    let h = setup_temporal_pong(pool).await;
    let (mut p1, mut p2, token, pid1, pid2) = pair_up(&h, 913, 914, "ranked_1v1").await;
    accept_and_connect(&h, &mut p1, &mut p2, &token).await;

    // p1 diverges at frame 25 (well before the winner at ~51); both drives
    // run concurrently (see pong_three_replicas_converge).
    // Same report-driven GameOver as pong_three_replicas_converge.
    let deadline = Instant::now() + Duration::from_secs(10);
    let ((_, side1, resynced), (_, side2, _)) = tokio::join!(
        drive_client(&mut p1, &token, &pid1, Some(25), deadline),
        drive_client(&mut p2, &token, &pid2, None, deadline),
    );
    assert_ne!(side1, side2, "the two clients must be on opposite sides");

    assert!(
        resynced,
        "the referee must detect p1's divergence and resync it"
    );
    let left_player = if side1 == PongSide::Left { pid1 } else { pid2 };
    p1.submit_report(&token, Some(&left_player), Some("demo-a"))
        .await
        .unwrap();
    p2.submit_report(&token, Some(&left_player), Some("demo-b"))
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(15);
    let w1 = wait_game_over(&mut p1, deadline).await;
    let w2 = wait_game_over(&mut p2, deadline).await;
    assert_eq!(w1, w2, "both clients must agree on the winner");
    assert_eq!(
        w1, left_player,
        "the divergence must not change the outcome"
    );

    drop(p1);
    drop(p2);
}

// ── helpers copied from integration.rs (the itest crate has no shared lib) ──

use sqlx::PgPool;

async fn pair_up(
    h: &common::TestHarness,
    p1_id: u64,
    p2_id: u64,
    mode: &str,
) -> (LobbyClient, LobbyClient, String, String, String) {
    let mut p1 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let mut p2 = LobbyClient::connect(&h.ws_url).await.unwrap();
    let a1 = p1.authenticate_test_token(p1_id, &h.base_url).await.unwrap();
    let a2 = p2.authenticate_test_token(p2_id, &h.base_url).await.unwrap();

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
    assert_eq!(
        m1.match_token, m2.match_token,
        "both players must get the same match"
    );
    (p1, p2, m1.match_token, a1.player_id, a2.player_id)
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

    p1.start_match(token).await.unwrap();
    p2.start_match(token).await.unwrap();
    assert!(
        wait_for_status(&h.pool, token, "Reporting").await,
        "both connections must transition the match to Reporting"
    );
}

async fn wait_for_status(pool: &PgPool, token: &str, expected: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let status: Option<String> =
            sqlx::query_scalar("SELECT status FROM matches WHERE match_token = $1")
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
