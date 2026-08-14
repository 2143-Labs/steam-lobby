// Pong game logic — mechanical TS port of the demo's pong section
// (web/index.html:856-1000). The hold/countdown/skipTo semantics are
// parity-critical: freeze stepping + input sends during the server's round
// hold, resume via skipTo(confirmed) after the bounded grace. setInterval
import { PongSim, PongSide } from "../../../pong-sim.mjs";
import { RollbackSession } from "../../../pong-rollback.mjs";
import { ensureWrtcLink } from "./webrtc";
import { clearStartCountdown, log, notify, send, state } from "../lobby/store";

export const PONG_H = 400; // canvas height, px
const PADDLE_H = 0.08;
const PADDLE_X_L = 0.03;
const PADDLE_X_R = 0.97;
export const BALL_SPEED0 = 0.9; // only for the speed display; the sim lives in PongSim

let canvas: HTMLCanvasElement | null = null;
let pongCtx: CanvasRenderingContext2D | null = null;

/** Register the canvas element (PongCanvas mounts/unmounts this). */
export function setCanvas(el: HTMLCanvasElement | null) {
  canvas = el;
  pongCtx = el ? el.getContext("2d") : null;
}

/** Practice preview (or queueing backdrop): ball re-serves toward the right. */
export function startPractice() {
  state.gameMode = "practice";
  state.sim = new PongSim({ scoring: false });
  state.gameStatus = "Practice — waiting for opponent";
  if (!state.practiceTimer) {
    state.lastPracticeAt = performance.now();
    state.practiceTimer = setInterval(practiceLoop, 16);
  }
  notify();
}

/** Mode-aware queue preview: rps_1v1 → RPS preview panel; else pong practice. */
export function startQueuePreview(mode: string) {
  if (mode === "rps_1v1") {
    stopGame();
    state.gameMode = "rps_preview";
    state.rpsStatus = "Waiting for an opponent…";
    state.rpsScore = "Rock ✊ · Paper ✋ · Scissors ✌ — first to 3";
    state.rpsButtonsEnabled = false;
    notify();
    return;
  }
  startPractice();
}

function practiceLoop() {
  if (state.gameMode !== "practice") {
    clearInterval(state.practiceTimer);
    state.practiceTimer = undefined;
    return;
  }
  const t = performance.now();
  const dt = Math.min(0.05, (t - state.lastPracticeAt) / 1000);
  state.lastPracticeAt = t;
  const sim = state.sim;
  if (!sim) return;
  sim.setTarget(PongSide.Left, state.inputTarget);
  sim.setTarget(PongSide.Right, sim.snapshot().ball_y); // AI tracks the ball
  sim.step(dt);
  state.game = sim.snapshot();
  renderFrame();
}

function pongLoop() {
  if (state.gameMode !== "pong") {
    clearInterval(state.pongTimer);
    state.pongTimer = undefined;
    return;
  }
  // Round hold (3-2-1): freeze stepping AND input sends until the server's
  // hold expires — the referee is broadcasting unchanged frames; advancing
  // locally would desync the sims.
  if (state.session && state.holdUntilMs !== null) {
    // Expire only once the timer elapsed AND confirmed caught up to the last
    // frozen frame — BUT bounded by a short grace: the referee's final
    // broadcast can arrive a tick late, yet if it never arrives (dropped WS
    // message), waiting forever would deadlock the match (no inputs sent, the
    // referee's gate stalls). After the grace, resume with skipTo(confirmed);
    // a one-frame lag is absorbed by the rollback engine.
    const late = performance.now() - state.holdUntilMs;
    if (late < 0 || (state.session.confirmed < state.holdEndFrame && late < 300)) {
      if (state.game) renderFrame(); // countdown overlay only
      return;
    }
    // Hold over: the referee resumed at confirmed + 1 — jump the local frame
    // counter (sim unchanged: the held frames were identical) so the next
    // step/send targets exactly the referee's frame. Without this the client
    // replays ~90 frozen frames at ~30fps and the referee's input gate stalls
    // silently for ~3s.
    state.session.skipTo(state.session.confirmed);
    state.holdUntilMs = null;
  }
  const t = performance.now();
  state.pongAcc += Math.min(0.05, (t - state.lastPongAt) / 1000);
  state.lastPongAt = t;
  const DT = 33 / 1000;
  while (state.session && state.pongAcc >= DT && !state.stalled) {
    state.pongAcc -= DT;
    const session = state.session;
    // My real input for the next frame: apply locally AND send to the server
    // (per-frame sends are ~360 B/s — no deadband/throttle needed). The
    // target travels as its shortest round-trip decimal string — JS Number
    // and Rust str::parse round-trip exactly; serde_json's f64 parser is not.
    const frame = session.frame + 1;
    session.localTarget(frame, state.inputTarget);
    send({
      type: "game_input",
      match_token: state.matchToken ?? "",
      frame,
      target: state.inputTarget.toString(),
    });
    state.link?.send({ type: "game_input", frame, target: state.inputTarget.toString() });
    state.pendingAcks.set(frame, performance.now());
    if (state.pendingAcks.size > 900) {
      const now = performance.now();
      for (const [f, sentAt] of state.pendingAcks) {
        if (now - sentAt > 2000) state.pendingAcks.delete(f);
      }
    }
    const result = session.step();
    state.stalled = result.stalled;
    if (result.snapshot) state.game = result.snapshot;
    if (result.rolledBack) log("sys", "rolled back to correct trajectory");
  }
  if (state.game) renderFrame();
}

/** Draw the current snapshot + the 3-2-1 countdown overlay. */
export function renderFrame() {
  const g = state.game;
  if (!g || !pongCtx || !canvas) return;
  const w = canvas.width;
  const h = PONG_H;
  pongCtx.fillStyle = "#0a0a0a";
  pongCtx.fillRect(0, 0, w, h);
  pongCtx.strokeStyle = "#333";
  pongCtx.beginPath();
  pongCtx.moveTo(w / 2, 0);
  pongCtx.lineTo(w / 2, h);
  pongCtx.stroke();
  pongCtx.fillStyle = "#eee";
  pongCtx.fillRect(PADDLE_X_L * w - 2, (g.left_y - PADDLE_H) * h, 4, PADDLE_H * 2 * h);
  pongCtx.fillRect(PADDLE_X_R * w - 2, (g.right_y - PADDLE_H) * h, 4, PADDLE_H * 2 * h);
  pongCtx.beginPath();
  pongCtx.arc(g.ball_x * w, g.ball_y * h, 6, 0, Math.PI * 2);
  pongCtx.fill();
  // 3-2-1 countdown overlay (server round hold).
  if (state.holdUntilMs !== null) {
    const remaining = state.holdUntilMs - performance.now();
    const n = Math.ceil(remaining / 1000);
    if (n > 0 && n <= 3) {
      pongCtx.fillStyle = "#fff";
      pongCtx.font = "bold 64px ui-monospace, monospace";
      pongCtx.textAlign = "center";
      pongCtx.textBaseline = "middle";
      pongCtx.fillText(String(n), w / 2, h / 2);
    }
  }
}

/** Stop every loop and tear down the match session (canvas keeps its frame). */
export function stopGame() {
  state.gameMode = "off";
  state.game = null;
  state.sim = null;
  state.session = null;
  state.stalled = false;
  state.holdUntilMs = null;
  state.holdEndFrame = 0;
  clearStartCountdown();
  clearInterval(state.practiceTimer);
  state.practiceTimer = undefined;
  clearInterval(state.pongTimer);
  state.pongTimer = undefined;
  clearInterval(state.rpsTimer);
  state.rpsTimer = undefined;
  if (state.link) {
    state.link.close();
    state.link = null;
  }
  notify();
}

/** Create the local bit-exact sim + rollback session on the first frame. */
export function beginPongSession(msg: {
  frame: number;
  player_a: string;
  player_b: string;
}) {
  state.gameMode = "pong";
  state.iAmPlayerA = state.playerId === msg.player_a;
  if (!state.session) {
    state.sim = new PongSim();
    state.session = new RollbackSession({
      sim: state.sim,
      side: state.iAmPlayerA ? PongSide.Left : PongSide.Right,
    });
    state.lastPongAt = performance.now();
    state.pongAcc = 0;
    state.stalled = false;
    if (!state.pongTimer) state.pongTimer = setInterval(pongLoop, 16);
    // WebRTC data channel for direct peer-to-peer inputs (feature-detected).
    void ensureWrtcLink();
  }
  notify();
}

export function hexToBytes(hex: string): Uint8Array {
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.length; i++) {
    out[i] = parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

/** Paddle target from a canvas-relative event (offsetY in 0..PONG_H). */
export function computeTargetFromOffset(offsetY: number) {
  state.inputTarget = Math.max(0, Math.min(1, offsetY / PONG_H));
}

// Global keyboard input: arrows / WASD move the paddle (same as the demo).
addEventListener("keydown", (e) => {
  const step = 0.08;
  if (e.key === "ArrowDown" || e.key === "s" || e.key === "S") {
    state.inputTarget = Math.min(1, state.inputTarget + step);
  } else if (e.key === "ArrowUp" || e.key === "w" || e.key === "W") {
    state.inputTarget = Math.max(0, state.inputTarget - step);
  }
});
