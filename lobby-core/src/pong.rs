//! Server-authoritative Pong game logic, shared by the lobby server's per-match
//! task and the demo's practice sim: pure, deterministic, no stores, no IO.
//!
//! The field is normalized 0..1 on both axes. Paddles are `PADDLE_HALF_HEIGHT`
//! tall at x = 0.03 (Left) / 0.97 (Right); the ball bounces off the top/bottom
//! walls and the paddles, speeding up by 6% on every paddle hit (capped at 4x
//! base so the game stays playable). First to `WIN_SCORE` points wins.

pub const WIN_SCORE: u8 = 3;
pub const TICK_MS: u64 = 33;
/// The one and only sim timestep — fixed for determinism, never wall-clock.
/// (33ms ≈ 30.3Hz; keep 33.0/1000.0 — an inexact-but-identical value in every
/// caller, including the JS mirror and the tests.)
pub const DT_SECS: f64 = 33.0 / 1000.0;

const PADDLE_HALF_HEIGHT: f64 = 0.08;
const PADDLE_X_LEFT: f64 = 0.03;
const PADDLE_X_RIGHT: f64 = 0.97;
const PADDLE_SPEED: f64 = 0.8; // units/sec toward the target
const PADDLE_CLAMP: (f64, f64) = (0.08, 0.92); // center limits so edges stay on screen
const BALL_SPEED: f64 = 0.9; // base serve speed, units/sec (3x the original 0.3)
const SPEED_UP: f64 = 1.06; // per paddle hit — "gets faster and faster"
const MAX_SPEED: f64 = 6.0;
const HIT_RADIUS: f64 = 0.02; // |ball_x - paddle_x| < this counts as a hit
                              // Physics sub-step cap so the ball can never tunnel through a paddle at max
                              // speed (a 0.04-wide hit window needs steps well under 0.02 units).
const MAX_SUBSTEP_TRAVEL: f64 = 0.005;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PongSide {
    Left,
    Right,
}

#[derive(Debug, Clone, Copy)]
pub struct PongSnapshot {
    pub left_y: f64, // paddle centers, 0..1
    pub right_y: f64,
    pub ball_x: f64, // 0..1
    pub ball_y: f64,
    pub left_score: u8,
    pub right_score: u8,
    pub speed: f64, // current ball speed, units/sec
}

#[derive(Debug)]
pub struct PongGame {
    left_y: f64,
    right_y: f64,
    ball_x: f64,
    ball_y: f64,
    ball_vx: f64,
    ball_vy: f64,
    speed: f64,
    left_score: u8,
    right_score: u8,
    left_target: Option<f64>,
    right_target: Option<f64>,
}

impl PongGame {
    pub fn new() -> Self {
        Self {
            left_y: 0.5,
            right_y: 0.5,
            ball_x: 0.5,
            ball_y: 0.5,
            ball_vx: BALL_SPEED, // serves toward Right
            ball_vy: 0.0,
            speed: BALL_SPEED,
            left_score: 0,
            right_score: 0,
            left_target: None,
            right_target: None,
        }
    }

    /// Set a paddle's target (normalized 0..1). A paddle with no target stays put.
    pub fn set_target(&mut self, side: PongSide, target: f64) {
        let t = target.clamp(0.0, 1.0);
        match side {
            PongSide::Left => self.left_target = Some(t),
            PongSide::Right => self.right_target = Some(t),
        }
    }

    /// Advance physics by `dt` seconds (paddles, then ball in sub-steps).
    pub fn step(&mut self, dt: f64) {
        self.move_paddle(PongSide::Left, dt);
        self.move_paddle(PongSide::Right, dt);
        let n = ((self.speed * dt) / MAX_SUBSTEP_TRAVEL).ceil().max(1.0) as usize;
        for _ in 0..n {
            self.step_ball(dt / n as f64);
            self.bounce_walls();
            if self.hit_paddle() {
                self.speed = (self.speed * SPEED_UP).min(MAX_SPEED);
            }
        }
        if self.ball_x < 0.0 {
            self.right_score += 1;
            self.serve(PongSide::Right);
        } else if self.ball_x > 1.0 {
            self.left_score += 1;
            self.serve(PongSide::Left);
        }
    }

    pub fn snapshot(&self) -> PongSnapshot {
        PongSnapshot {
            left_y: self.left_y,
            right_y: self.right_y,
            ball_x: self.ball_x,
            ball_y: self.ball_y,
            left_score: self.left_score,
            right_score: self.right_score,
            speed: self.speed,
        }
    }

    pub const STATE_BYTES: usize = 74; // 9×f64 LE + 2×u8

    /// Canonical serialization of the whole game state: 9 f64 little-endian —
    /// `left_y, right_y, ball_x, ball_y, ball_vx, ball_vy, speed, left_target,
    /// right_target` — then 2 u8 — `left_score, right_score`. `left_target`/
    /// `right_target` use the sentinel `-1.0` for `None` (targets are clamped
    /// to 0..1, so `-1.0` is unreachable). No padding. Bit-identical to the JS
    /// mirror's `fullState()` in `web/pong-sim.mjs`.
    pub fn full_state(&self) -> [u8; Self::STATE_BYTES] {
        let mut out = [0u8; Self::STATE_BYTES];
        let mut i = 0;
        for v in [
            self.left_y,
            self.right_y,
            self.ball_x,
            self.ball_y,
            self.ball_vx,
            self.ball_vy,
            self.speed,
            self.left_target.unwrap_or(-1.0),
            self.right_target.unwrap_or(-1.0),
        ] {
            out[i..i + 8].copy_from_slice(&v.to_le_bytes());
            i += 8;
        }
        out[72] = self.left_score;
        out[73] = self.right_score;
        out
    }

    /// Inverse of `full_state()`. Any byte array produced by `full_state()`
    /// (from this sim or the JS mirror) restores an identical game.
    pub fn restore(&mut self, bytes: &[u8; Self::STATE_BYTES]) {
        let mut vals = [0.0f64; 9];
        for (v, chunk) in vals.iter_mut().zip(bytes.chunks_exact(8)) {
            *v = f64::from_le_bytes(chunk.try_into().unwrap());
        }
        self.left_y = vals[0];
        self.right_y = vals[1];
        self.ball_x = vals[2];
        self.ball_y = vals[3];
        self.ball_vx = vals[4];
        self.ball_vy = vals[5];
        self.speed = vals[6];
        self.left_target = if vals[7] < 0.0 { None } else { Some(vals[7]) };
        self.right_target = if vals[8] < 0.0 { None } else { Some(vals[8]) };
        self.left_score = bytes[72];
        self.right_score = bytes[73];
    }

    /// FNV-1a 64 over `full_state()` — the per-frame determinism checksum
    /// shared with the JS mirror and the server referee protocol.
    pub fn checksum(&self) -> u64 {
        fnv1a64(&self.full_state())
    }

    pub fn winner(&self) -> Option<PongSide> {
        if self.left_score >= WIN_SCORE {
            Some(PongSide::Left)
        } else if self.right_score >= WIN_SCORE {
            Some(PongSide::Right)
        } else {
            None
        }
    }

    fn move_paddle(&mut self, side: PongSide, dt: f64) {
        let y = match side {
            PongSide::Left => self.left_y,
            PongSide::Right => self.right_y,
        };
        let target = match side {
            PongSide::Left => self.left_target,
            PongSide::Right => self.right_target,
        };
        let mut new_y = y;
        if let Some(t) = target {
            let max_move = PADDLE_SPEED * dt;
            new_y = y + (t - y).clamp(-max_move, max_move);
        }
        new_y = new_y.clamp(PADDLE_CLAMP.0, PADDLE_CLAMP.1);
        match side {
            PongSide::Left => self.left_y = new_y,
            PongSide::Right => self.right_y = new_y,
        }
    }

    fn step_ball(&mut self, dt: f64) {
        let len = (self.ball_vx * self.ball_vx + self.ball_vy * self.ball_vy).sqrt();
        if len > 0.0 {
            self.ball_x += (self.ball_vx / len) * self.speed * dt;
            self.ball_y += (self.ball_vy / len) * self.speed * dt;
        }
    }

    fn bounce_walls(&mut self) {
        if self.ball_y < 0.0 {
            self.ball_y = -self.ball_y;
            self.ball_vy = -self.ball_vy;
        } else if self.ball_y > 1.0 {
            self.ball_y = 2.0 - self.ball_y;
            self.ball_vy = -self.ball_vy;
        }
    }

    /// Reflect the ball off a paddle. Returns true when a hit happened.
    /// The `ball_vx` sign guard makes each crossing count exactly one hit.
    fn hit_paddle(&mut self) -> bool {
        if self.ball_vx < 0.0
            && (self.ball_x - PADDLE_X_LEFT).abs() < HIT_RADIUS
            && (self.ball_y - self.left_y).abs() <= PADDLE_HALF_HEIGHT
        {
            self.ball_vx = self.ball_vx.abs();
            self.ball_vy = (self.ball_y - self.left_y) * 3.0;
            return true;
        }
        if self.ball_vx > 0.0
            && (self.ball_x - PADDLE_X_RIGHT).abs() < HIT_RADIUS
            && (self.ball_y - self.right_y).abs() <= PADDLE_HALF_HEIGHT
        {
            self.ball_vx = -self.ball_vx.abs();
            self.ball_vy = (self.ball_y - self.right_y) * 3.0;
            return true;
        }
        false
    }

    /// Reset the ball to center and serve toward the conceding side at base speed.
    fn serve(&mut self, scorer: PongSide) {
        self.ball_x = 0.5;
        self.ball_y = 0.5;
        self.ball_vx = match scorer {
            PongSide::Left => BALL_SPEED,   // Right conceded
            PongSide::Right => -BALL_SPEED, // Left conceded
        };
        self.ball_vy = 0.0;
        self.speed = BALL_SPEED;
    }
}

/// FNV-1a 64 (offset basis `0xcbf29ce484222325`, prime `0x100000001b3`).
/// Hand-written byte loop — `std::collections::hash_map::DefaultHasher` is
/// SipHash with a random per-process key and MUST NOT be used for a checksum
/// that must be reproducible across languages and runs.
pub fn fnv1a64(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

impl Default for PongGame {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ball_serves_toward_right_and_moves() {
        let mut g = PongGame::new();
        for _ in 0..10 {
            g.step(DT_SECS);
        }
        assert!(g.snapshot().ball_x > 0.5, "serve must move right");
    }

    #[test]
    fn paddle_hit_speeds_up_ball_and_caps() {
        let mut g = PongGame::new();
        // Park both paddles on the serve line: the ball bounces forever at
        // y = 0.5, gaining 6% speed per hit until the cap.
        g.set_target(PongSide::Left, 0.5);
        g.set_target(PongSide::Right, 0.5);
        let mut steps = 0;
        while g.snapshot().speed < MAX_SPEED && steps < 200_000 {
            g.step(DT_SECS);
            steps += 1;
        }
        assert!(
            (g.snapshot().speed - MAX_SPEED).abs() < 1e-12,
            "speed must reach the cap, got {} after {steps} steps",
            g.snapshot().speed
        );
        assert_eq!(g.snapshot().left_score, 0, "rally must never score");
        assert_eq!(g.snapshot().right_score, 0, "rally must never score");
    }

    #[test]
    fn ball_passes_paddle_scores() {
        let mut g = PongGame::new();
        g.set_target(PongSide::Right, 0.05); // park Right at the top (clamps to 0.08)
        let mut steps = 0;
        while g.snapshot().left_score == 0 && steps < 100_000 {
            g.step(DT_SECS);
            steps += 1;
        }
        let s = g.snapshot();
        assert_eq!(s.left_score, 1);
        assert_eq!(s.right_score, 0);
        assert_eq!(s.ball_x, 0.5, "ball resets to center after the point");
        assert_eq!(s.ball_y, 0.5);
        assert_eq!(s.speed, BALL_SPEED, "serve resets to base speed");
    }

    #[test]
    fn first_to_three_declares_winner() {
        let mut g = PongGame::new();
        g.set_target(PongSide::Right, 0.05); // Right concedes every serve
        let mut steps = 0;
        while g.winner().is_none() && steps < 100_000 {
            g.step(DT_SECS);
            steps += 1;
        }
        assert_eq!(g.winner(), Some(PongSide::Left));
        let s = g.snapshot();
        assert_eq!(s.left_score, WIN_SCORE, "exactly 3 points, no more");
        assert_eq!(s.right_score, 0);
    }

    #[test]
    fn paddle_moves_toward_target_and_clamps() {
        let mut g = PongGame::new();
        g.set_target(PongSide::Left, 0.0);
        let mut min_y = 1.0f64;
        for _ in 0..30 {
            g.step(DT_SECS);
            min_y = min_y.min(g.snapshot().left_y);
        }
        assert!(
            (g.snapshot().left_y - PADDLE_CLAMP.0).abs() < 1e-9,
            "paddle must reach the clamp floor"
        );
        assert!(
            min_y >= PADDLE_CLAMP.0 - 1e-12,
            "paddle never below the floor"
        );
    }
}
