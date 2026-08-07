// Bit-exact JS mirror of lobby-core/src/pong.rs. Pure module: no DOM, no
// performance, no network. The determinism contract:
//   state[n+1] = step(state[n], inputs[n])
// with IEEE-exact ops only (+ - * / sqrt abs min max clamp ceil). `Math.hypot`
// and friends are NOT spec-guaranteed correctly rounded, so the norm here is
// Math.sqrt(vx*vx + vy*vy), mirroring the Rust `(vx*vx + vy*vy).sqrt()`.
//
// Any change in either language MUST be mirrored in the other and re-verified
// by `web/test/golden.mjs` (five fixed checkpoints) and the M2 differential
// test (all 10,000 frames) before committing.

export const PongSide = Object.freeze({ Left: "left", Right: "right" });

export const WIN_SCORE = 3;
export const TICK_MS = 33;
export const DT_SECS = 33 / 1000; // must equal lobby_core::pong::DT_SECS exactly
export const PADDLE_HALF_HEIGHT = 0.08;
export const PADDLE_X_LEFT = 0.03;
export const PADDLE_X_RIGHT = 0.97;
export const PADDLE_SPEED = 0.8; // units/sec toward the target
export const PADDLE_CLAMP = [0.08, 0.92]; // center limits so edges stay on screen
export const BALL_SPEED = 0.9; // base serve speed, units/sec
export const SPEED_UP = 1.06; // per paddle hit
export const MAX_SPEED = 6.0; // matches Rust; the old demo used 3.6 (wrong)
export const HIT_RADIUS = 0.02;
export const MAX_SUBSTEP_TRAVEL = 0.005; // anti-tunnel substep cap

/** FNV-1a 64 over a byte array — must match lobby_core::pong::fnv1a64. */
export function fnv1a64(bytes) {
  let h = 0xcbf29ce484222325n;
  const prime = 0x100000001b3n;
  for (let i = 0; i < bytes.length; i++) {
    h ^= BigInt(bytes[i]);
    h = (h * prime) & 0xffffffffffffffffn;
  }
  return h;
}

export class PongSim {
  /**
   * @param {{scoring?: boolean}} [opts] `scoring: false` (practice mode) makes
   *   a miss re-serve instead of scoring — the old demo behavior.
   */
  constructor({ scoring = true } = {}) {
    this.scoring = scoring;
    this.reset();
  }

  /** Mirrors PongGame::new(). */
  reset() {
    this.g = { left_y: 0.5, right_y: 0.5, ball_x: 0.5, ball_y: 0.5,
      ball_vx: BALL_SPEED, ball_vy: 0, speed: BALL_SPEED,
      left_score: 0, right_score: 0, left_target: null, right_target: null };
  }

  /** Mirrors PongGame::set_target — clamps to 0..1, stores the goal. */
  setTarget(side, target) {
    const t = Math.max(0, Math.min(1, target));
    if (side === PongSide.Left) this.g.left_target = t;
    else this.g.right_target = t;
  }

  /** Mirrors PongGame::step(dt) — paddles, then ball in anti-tunnel substeps. */
  step(dt) {
    const g = this.g;
    this.movePaddle(PongSide.Left, dt);
    this.movePaddle(PongSide.Right, dt);
    const n = Math.max(1, Math.ceil((g.speed * dt) / MAX_SUBSTEP_TRAVEL));
    for (let i = 0; i < n; i++) {
      const d = dt / n;
      this.stepBall(d);
      this.bounceWalls();
      if (this.hitPaddle()) {
        g.speed = Math.min(MAX_SPEED, g.speed * SPEED_UP);
      }
    }
    // Scoring (match mode): ball off the left edge → Right scores and serves
    // toward Left; off the right edge → Left scores and serves toward Right.
    // Practice mode (scoring: false): a miss never scores and always re-serves
    // toward the right (the pre-rollback demo behavior).
    const scorer = g.ball_x < 0 ? PongSide.Right : g.ball_x > 1 ? PongSide.Left : null;
    if (scorer !== null) {
      if (this.scoring) {
        if (scorer === PongSide.Right) g.right_score += 1; else g.left_score += 1;
        this.serve(scorer);
      } else {
        this.serve(PongSide.Left); // serve(Left) = +BALL_SPEED = toward Right
      }
    }
  }

  /** Mirrors PongGame::snapshot(). */
  snapshot() {
    const g = this.g;
    return { left_y: g.left_y, right_y: g.right_y, ball_x: g.ball_x, ball_y: g.ball_y,
      left_score: g.left_score, right_score: g.right_score, speed: g.speed };
  }

  /** Mirrors PongGame::winner() — null until WIN_SCORE is reached. */
  winner() {
    const g = this.g;
    if (g.left_score >= WIN_SCORE) return PongSide.Left;
    if (g.right_score >= WIN_SCORE) return PongSide.Right;
    return null;
  }

  /**
   * Canonical 74-byte serialization: 9 f64 LE (left_y, right_y, ball_x,
   * ball_y, ball_vx, ball_vy, speed, left_target, right_target) + 2 u8
   * (left_score, right_score). Targets use -1.0 for null (clamped to 0..1).
   * Bit-identical to PongGame::full_state().
   */
  fullState() {
    const out = new Uint8Array(74);
    const dv = new DataView(out.buffer);
    const g = this.g;
    const vals = [g.left_y, g.right_y, g.ball_x, g.ball_y, g.ball_vx, g.ball_vy, g.speed,
      g.left_target === null ? -1.0 : g.left_target,
      g.right_target === null ? -1.0 : g.right_target];
    for (let i = 0; i < 9; i++) dv.setFloat64(i * 8, vals[i], true);
    out[72] = g.left_score;
    out[73] = g.right_score;
    return out;
  }

  /** Inverse of fullState() — restores an identical game from 74 bytes. */
  restore(bytes) {
    const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    const g = this.g;
    g.left_y = dv.getFloat64(0, true);
    g.right_y = dv.getFloat64(8, true);
    g.ball_x = dv.getFloat64(16, true);
    g.ball_y = dv.getFloat64(24, true);
    g.ball_vx = dv.getFloat64(32, true);
    g.ball_vy = dv.getFloat64(40, true);
    g.speed = dv.getFloat64(48, true);
    g.left_target = dv.getFloat64(56, true);
    g.right_target = dv.getFloat64(64, true);
    if (g.left_target < 0) g.left_target = null;
    if (g.right_target < 0) g.right_target = null;
    g.left_score = bytes[72];
    g.right_score = bytes[73];
  }

  /** FNV-1a 64 over fullState() — the per-frame determinism checksum. */
  checksum() {
    return fnv1a64(this.fullState());
  }

  /** Raw internal state — for tests only. */
  stateForTest() {
    return this.g;
  }

  movePaddle(side, dt) {
    const g = this.g;
    const y = side === PongSide.Left ? g.left_y : g.right_y;
    const target = side === PongSide.Left ? g.left_target : g.right_target;
    let newY = y;
    if (target !== null) {
      const maxMove = PADDLE_SPEED * dt;
      newY = y + Math.max(-maxMove, Math.min(maxMove, target - y));
    }
    newY = Math.max(PADDLE_CLAMP[0], Math.min(PADDLE_CLAMP[1], newY));
    if (side === PongSide.Left) g.left_y = newY; else g.right_y = newY;
  }

  stepBall(dt) {
    const g = this.g;
    const len = Math.sqrt(g.ball_vx * g.ball_vx + g.ball_vy * g.ball_vy) || 1;
    // `|| 1` guards len == 0 (unreachable — the ball always has velocity);
    // Rust guards `if len > 0.0`. Observably equivalent; keep both as-is.
    g.ball_x += (g.ball_vx / len) * g.speed * dt;
    g.ball_y += (g.ball_vy / len) * g.speed * dt;
  }

  bounceWalls() {
    const g = this.g;
    if (g.ball_y < 0) { g.ball_y = -g.ball_y; g.ball_vy = -g.ball_vy; }
    else if (g.ball_y > 1) { g.ball_y = 2 - g.ball_y; g.ball_vy = -g.ball_vy; }
  }

  /** Reflect off a paddle; the vx sign guard makes each crossing count once. */
  hitPaddle() {
    const g = this.g;
    if (g.ball_vx < 0
        && Math.abs(g.ball_x - PADDLE_X_LEFT) < HIT_RADIUS
        && Math.abs(g.ball_y - g.left_y) <= PADDLE_HALF_HEIGHT) {
      g.ball_vx = Math.abs(g.ball_vx);
      g.ball_vy = (g.ball_y - g.left_y) * 3;
      return true;
    }
    if (g.ball_vx > 0
        && Math.abs(g.ball_x - PADDLE_X_RIGHT) < HIT_RADIUS
        && Math.abs(g.ball_y - g.right_y) <= PADDLE_HALF_HEIGHT) {
      g.ball_vx = -Math.abs(g.ball_vx);
      g.ball_vy = (g.ball_y - g.right_y) * 3;
      return true;
    }
    return false;
  }

  /** Reset the ball to center and serve toward the conceding side. */
  serve(scorer) {
    const g = this.g;
    g.ball_x = 0.5;
    g.ball_y = 0.5;
    g.ball_vx = scorer === PongSide.Left ? BALL_SPEED : -BALL_SPEED; // Left → toward Right
    g.ball_vy = 0;
    g.speed = BALL_SPEED;
  }
}
