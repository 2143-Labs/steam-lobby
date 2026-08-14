// Module-level mutable lobby state (port of the demo's `state` object +
// connection metrics), with a tiny subscription mechanism for React.
// Game loops read the fields directly (imperative, no re-render on the 30Hz
// frame path); discrete protocol events call notify() so the UI re-renders.
// Only UI-facing state changes notify — NOT game_state/input_ack frames.

import type { WrtcLink } from "../../../pong-wrtc.mjs";
import type { PongSim, PongSnapshot } from "../../../pong-sim.mjs";
import type { RollbackSession } from "../../../pong-rollback.mjs";
import type { ClientMessage, LeaderboardEntry } from "./protocol";

/** Browser interval handle — owned here so consumers import the name. */
export type TimerHandle = ReturnType<typeof setInterval>;

export type GameMode = "off" | "practice" | "pong" | "rps" | "rps_preview";
export type ControlsPanel = "connected" | "queueing" | "match" | "inmatch";

export interface LobbyState {
  // connection / identity
  ws: WebSocket | null;
  token: string | null;
  playerId: string | null;
  displayName: string | null;
  lastBase: string | null; // last successful server base (for Reconnect)
  lastToken: string | null; // last session token (for Reconnect; cleared on signout)
  // match
  matchToken: string | null;
  opponentId: string | null;
  opponentName: string | null;
  gameType: "p2p" | "server" | null;
  matchMode: string | null; // mode name from match_found (rps_1v1 vs ranked_1v1)
  // pong
  game: PongSnapshot | null;
  sim: PongSim | null;
  session: RollbackSession | null;
  link: WrtcLink | null;
  stalled: boolean;
  inputTarget: number;
  lastPongAt: number;
  lastPracticeAt: number;
  pongAcc: number;
  practiceTimer: TimerHandle | undefined;
  pongTimer: TimerHandle | undefined;
  iAmPlayerA: boolean | undefined;
  holdUntilMs: number | null; // round hold: freeze sim + draw 3-2-1 until this time
  holdEndFrame: number;
  // rps
  rpsRound: number | null;
  rpsChosen: boolean;
  rpsTimer: TimerHandle | undefined;
  // rps panel UI
  rpsStatus: string;
  rpsScore: string;
  rpsButtonsEnabled: boolean;
  // in-match panel UI
  serverInfo: string | null;
  oppP2p: string;
  reportStatus: string;
  startBtn: { visible: boolean; disabled: boolean; label: string };
  gameStatus: string;
  // UI state
  gameMode: GameMode;
  controls: ControlsPanel;
  statusText: string;
  statusCls: string;
  logLines: { kind: string; text: string }[];
  pendingAcks: Map<number, number>; // game_input frame -> performance.now() when sent
  queue: {
    wait: string;
    band: string;
    candidates: string;
    size: string;
    myMu: string;
    mySigma: string;
    myRating: string;
    leaderboard: LeaderboardEntry[];
  };
  // derived convenience for components
  connected: boolean;
  /** The currently selected matchmaking mode ("ranked_1v1" etc.). */
  selectedMode: string;
}

export const state: LobbyState = {
  ws: null,
  token: null,
  playerId: null,
  displayName: null,
  lastBase: null,
  lastToken: null,
  matchToken: null,
  opponentId: null,
  opponentName: null,
  gameType: null,
  matchMode: null,
  game: null,
  sim: null,
  session: null,
  link: null,
  stalled: false,
  inputTarget: 0.5,
  lastPongAt: 0,
  lastPracticeAt: 0,
  pongAcc: 0,
  practiceTimer: undefined,
  pongTimer: undefined,
  iAmPlayerA: undefined,
  holdUntilMs: null,
  holdEndFrame: 0,
  rpsRound: null,
  rpsTimer: undefined,
  rpsChosen: false,
  rpsStatus: "—",
  rpsScore: "—",
  rpsButtonsEnabled: false,
  serverInfo: null,
  oppP2p: "Waiting for opponent to start…",
  reportStatus: "—",
  startBtn: { visible: false, disabled: true, label: "START Match" },
  gameStatus: "—",
  pendingAcks: new Map(),
  gameMode: "off",
  controls: "connected",
  statusText: "Disconnected",
  statusCls: "",
  logLines: [],
  queue: {
    wait: "—",
    band: "—",
    candidates: "—",
    size: "—",
    myMu: "—",
    mySigma: "—",
    myRating: "—",
    leaderboard: [],
  },
  connected: false,
  selectedMode: "ranked_1v1",
};

// ── subscription (React) ─────────────────────────────────────────────────

const listeners = new Set<() => void>();

export function subscribe(fn: () => void): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function showControls(name: ControlsPanel) {
  state.controls = name;
  notify();
}

/** UI-facing change — re-render React. NOT called on the 30Hz frame path. */
export function notify() {
  for (const fn of listeners) fn();
}

export function setStatus(text: string, cls = "") {
  state.statusText = text;
  state.statusCls = cls;
  notify();
}

/** Append a protocol log line (kind: "in" | "out" | "sys"). */
export function log(kind: string, text: string) {
  // Mask raw Steam IDs (17 digits) in the protocol dump — the UI and wire
  // are player-id-only, so this only guards against stray legacy values.
  const masked = text.replace(/\b7\d{16}\b/g, "…");
  state.logLines.push({
    kind,
    text: new Date().toISOString().slice(11, 23) + "  " + masked,
  });
  if (state.logLines.length > 500) state.logLines.splice(0, state.logLines.length - 500);
  notify();
}

/** Send a protocol message, logging a sanitized copy (never credentials). */
export function send(msg: ClientMessage) {
  const shown = { ...msg } as Record<string, unknown>;
  if ("session_token" in shown) shown.session_token = "***";
  if ("ticket" in shown) shown.ticket = "***";
  log("out", "→ " + JSON.stringify(shown));
  state.ws?.send(JSON.stringify(msg));
}

/** Short display form of a player id (first 8 chars of the account UUID). */
export function shortId(id: string | null | undefined): string {
  return id && id.length >= 8 ? id.slice(0, 8) : id || "—";
}

/** A player's display label: name when known, always with the short id. */
export function playerLabel(player_id: string | undefined, display_name?: string | null): string {
  const id = shortId(player_id);
  return display_name && display_name !== "Unknown"
    ? display_name + " · " + id
    : "Player " + id;
}

/** Decode the JWT payload's `sub` claim (base64url middle segment). */
export function jwtSub(token: string | null): string | null {
  if (!token) return null;
  try {
    const b64 = token.split(".")[1].replace(/-/g, "+").replace(/_/g, "/");
    const pad = b64.length % 4 ? "=".repeat(4 - (b64.length % 4)) : "";
    return JSON.parse(atob(b64 + pad)).sub || null;
  } catch {
    return null;
  }
}

// ── START countdown (match_started → reveal + count down the START window) ──

let startCountdownTimer: TimerHandle | undefined;

/** match_started → reveal the START button with the server's actual window
 *  length (msg.start_timeout_secs, NOT a hardcoded 15) and count it down. */
export function beginStartCountdown(timeoutSecs: number) {
  clearStartCountdown();
  state.startBtn.visible = true; // REVEAL only now — match_started means both accepted
  state.startBtn.disabled = false;
  let n = timeoutSecs; // seconds — the server's LOBBY_START_TIMEOUT_SECS
  state.startBtn.label = "START Match (" + n + "s)";
  notify();
  startCountdownTimer = setInterval(() => {
    n -= 1;
    if (n <= 0) {
      clearStartCountdown();
      state.startBtn.disabled = true;
      state.startBtn.label = "Didn't start in time — awaiting result";
      notify();
      return;
    }
    state.startBtn.label = "START Match (" + n + "s)";
    notify();
  }, 1000);
}

export function clearStartCountdown() {
  if (startCountdownTimer) {
    clearInterval(startCountdownTimer);
    startCountdownTimer = undefined;
  }
}
