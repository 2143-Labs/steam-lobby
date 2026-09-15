// WebSocket lobby client. Browser-provider sessions authenticate with the
// HttpOnly cookie; dev/guest/native sessions use an in-memory bearer token.
import { BALL_SPEED } from "../../../pong-sim.mjs";
import { beginPongSession, hexToBytes, startPractice, stopGame } from "../game/pong";
import { beginRpsRound, rpsGameOver, rpsRoundResult } from "../game/rps";
import { fetchModes, fetchSession } from "../api";
import type { SessionInfo } from "../types";
import { parseServerMessage } from "./protocol";
import {
  beginStartCountdown,
  clearStartCountdown,
  jwtSub,
  log,
  notify,
  send,
  setModes,
  setStatus,
  showControls,
  state,
} from "./store";
import type { AuthMode, TimerHandle } from "./store";

// ── connection ───────────────────────────────────────────────────────────

let hbTimer: TimerHandle | null = null;
let connTimer: TimerHandle | null = null;
const metrics = { serverRtt: null as number | null };

function startHeartbeat(intervalMs: number) {
  stopHeartbeat();
  hbTimer = setInterval(() => send({ type: "heartbeat" }), intervalMs);
}

function stopHeartbeat() {
  if (hbTimer) {
    clearInterval(hbTimer);
    hbTimer = null;
  }
}

function startConnMetrics() {
  if (!connTimer) connTimer = setInterval(updateConnMetrics, 2000);
}

function stopConnMetrics() {
  if (connTimer) {
    clearInterval(connTimer);
    connTimer = null;
  }
}

async function updateConnMetrics() {
  let opp = "Opponent: —";
  const pc = state.link?.pc;
  if (pc) {
    const probe = { value: null as number | null };
    try {
      const stats = await pc.getStats();
      stats.forEach((s) => {
        const pair = s as {
          type: string;
          nominated?: boolean;
          selected?: boolean;
          currentRoundTripTime?: number;
        };
        if (
          pair.type === "candidate-pair" &&
          (pair.nominated || pair.selected) &&
          typeof pair.currentRoundTripTime === "number"
        ) {
          probe.value = pair.currentRoundTripTime;
        }
      });
    } catch {
      /* getStats throws when the pc is closing — keep last value */
    }
    const link = state.link;
    const ch = link ? link.channelState : "new";
    opp =
      "Opponent: " +
      (typeof probe.value === "number" ? probe.value.toFixed(0) + "ms " : "") +
      (ch === "open" ? "(direct)" : "(relay)");
  }
  const sv = typeof metrics.serverRtt === "number" ? metrics.serverRtt.toFixed(0) + "ms" : "—";
  const el = document.getElementById("conn-metrics");
  if (el) el.textContent = "Server: " + sv + " · " + opp;
}

/**
 * Adopt the server's advertised modes. On failure the list is emptied rather
 * than fabricated: inventing a game the server is not running is exactly the
 * bug this replaces, so an unreachable API shows "No game modes advertised by
 * this server." instead of degrading into a demo mode.
 */
export async function loadModes(base: string) {
  try {
    setModes(await fetchModes(base));
  } catch (e) {
    log("sys", "modes fetch failed: " + (e as Error).message);
    setModes([]);
  }
}

async function connect(base: string, mode: AuthMode, token: string | null) {
  if (state.ws && state.ws.readyState < WebSocket.CLOSING) return;
  await loadModes(base);

  const wsUrl = base.replace(/^http/, "ws") + "/ws";
  let ws: WebSocket;
  try {
    ws = new WebSocket(wsUrl);
  } catch (e) {
    setStatus("Error: " + (e as Error).message, "err");
    return;
  }

  state.ws = ws;
  ws.onopen = () => {
    log("sys", "WS open: " + wsUrl);
    state.token = token;
    state.authMode = mode;
    state.lastBase = base;
    state.lastAuthMode = mode;
    state.lastToken = mode === "token" ? token : null;
    state.connected = true;
    setStatus("Connecting…");
    if (mode === "token" && token) send({ type: "auth", session_token: token });
    notify();
  };
  ws.onmessage = (ev) => handleServer(ev.data);
  ws.onclose = () => {
    stopHeartbeat();
    stopConnMetrics();
    stopGame();
    if (state.link) {
      state.link.close();
      state.link = null;
    }
    log("sys", "WS closed");
    state.ws = null;
    state.connected = false;
    state.authMode = null;
    setStatus("Disconnected", "");
    showControls("connected");
    state.token = null;
    notify();
  };
  ws.onerror = () => log("sys", "WS error");
}

/** Connect with a dev, guest, or native token retained only in memory. */
export async function connectWithToken(base: string, token: string) {
  await connect(base, "token", token);
}

/** Validate the HttpOnly browser session, then let its cookie authenticate WS. */
export async function connectWithSession(base: string, session?: SessionInfo): Promise<boolean> {
  const live = session ?? (await fetchSession(base));
  if (!live) return false;
  state.playerId = live.user_id;
  state.displayName = live.display_name;
  state.csrfToken = live.csrf_token;
  state.authProvider = live.auth_provider;
  await connect(base, "cookie", null);
  return true;
}

/** Disconnect: clean close handshake; the server sweeps the queue entry. */
export function disconnect() {
  log("sys", "disconnecting");
  if (state.ws) state.ws.close();
  // Clear match state while retaining the authenticated account for reconnect.
  state.matchToken = null;
  state.opponentId = null;
  state.gameType = null;
  state.opponentName = null;
  notify();
}

/** Reconnect using the remembered authentication mode. */
export function reconnect() {
  if (!state.lastBase || !state.lastAuthMode) return;
  if (state.lastAuthMode === "cookie") {
    void connectWithSession(state.lastBase).then((connected) => {
      if (!connected) setStatus("Browser session expired", "err");
    }).catch((e) => {
      setStatus("Session check failed: " + (e as Error).message, "err");
    });
  } else if (state.lastToken) {
    void connectWithToken(state.lastBase, state.lastToken);
  }
}

/** Revoke the current browser or bearer session, then forget local auth state. */
export async function signout() {
  const base = state.lastBase || "";
  const mode = state.authMode ?? state.lastAuthMode;
  const headers: Record<string, string> = {};
  if (mode === "cookie" && state.csrfToken) headers["X-CSRF-Token"] = state.csrfToken;
  const bearer = state.token ?? state.lastToken;
  if (mode === "token" && bearer) headers.Authorization = "Bearer " + bearer;
  if (mode) {
    try {
      await fetch(base + "/api/logout", { method: "POST", credentials: "include", headers });
    } catch (e) {
      log("sys", "logout request failed: " + (e as Error).message);
    }
  }
  state.token = null;
  state.lastToken = null;
  state.lastBase = null;
  state.authMode = null;
  state.lastAuthMode = null;
  state.csrfToken = null;
  state.authProvider = null;
  state.playerId = null;
  state.displayName = null;
  if (state.ws) state.ws.close();
  notify();
}

// ── inbound protocol ─────────────────────────────────────────────────────

function handleServer(raw: string) {
  log("in", "← " + raw);
  const msg = parseServerMessage(raw);
  if (!msg) return;
  switch (msg.type) {
    case "auth_ok": {
      setStatus(msg.state === "Queueing" ? "Connected — still in queue" : "Connected", "ok");
      showControls(msg.state === "Queueing" ? "queueing" : "connected");
      if (msg.state === "Queueing") startPractice();
      // Passive liveness: heartbeat for as long as we're connected. The server
      // drops the connection after 30s without a heartbeat (10s cadence gives
      // margin); while queueing this also keeps the queue entry alive.
      startHeartbeat(10000);
      startConnMetrics();
      log("sys", "server state on connect: " + msg.state);
      // Player ID: the abstract account id (users.id, the JWT `sub`) that
      // every provider's session will share — never the per-provider steam_id.
      const sub = msg.player_id || jwtSub(state.token);
      state.playerId = sub || null;
      state.displayName = msg.display_name || null;
      notify();
      break;
    }
    case "match_found": {
      stopGame(); // the match replaces the practice game
      setStatus("MatchFound", "ok");
      state.matchToken = msg.match_token;
      state.gameType = msg.game_type;
      state.matchMode = msg.game_mode;
      state.opponentId = msg.opponent.player_id;
      state.opponentName = msg.opponent.display_name;
      state.oppP2p = "Waiting for opponent to start…";
      state.reportStatus = "—";
      // Server-authoritative matches show the server info line; p2p hides it.
      state.serverInfo = msg.game_type === "server" ? "Waiting for server allocation…" : null;
      showControls("match");
      break;
    }
    case "queue_status": {
      const q = state.queue;
      q.wait = Math.floor(msg.elapsed_ms / 1000) + "s";
      q.band = msg.band_lo.toFixed(1) + " – " + msg.band_hi.toFixed(1);
      q.candidates = String(msg.candidates);
      q.size = String(msg.queue_size);
      q.myMu = msg.my_mu.toFixed(1);
      q.mySigma = msg.my_sigma.toFixed(1);
      q.myRating = msg.my_rating.toFixed(1);
      q.leaderboard = msg.leaderboard;
      notify();
      break;
    }
    case "opponent_connected": {
      state.oppP2p = "Opponent ready ✓";
      log("sys", "opponent started");
      notify();
      break;
    }
    case "report_received": {
      const who = msg.reporting_player === state.playerId ? "You" : "Opponent";
      const w =
        msg.winner === null
          ? "Draw"
          : msg.winner === state.playerId
            ? "Win (you)"
            : "Win (opponent)";
      state.reportStatus = who + " reported: " + w;
      log("sys", who + " reported: " + w);
      notify();
      break;
    }
    case "match_expired": {
      setStatus("Match expired — no one accepted in time", "");
      log("sys", "match expired — no one accepted in time");
      showControls("connected");
      stopGame();
      state.matchToken = null;
      notify();
      break;
    }
    case "queue_expired": {
      setStatus("Removed from queue — no opponent found in time", "");
      log("sys", "removed from queue (no heartbeat / no match found)");
      showControls("connected");
      stopGame();
      const q = state.queue;
      q.wait = "—";
      q.band = "—";
      q.candidates = "—";
      q.size = "—";
      notify();
      break;
    }
    case "match_declined": {
      setStatus("Match declined", "");
      showControls("connected");
      stopGame();
      state.matchToken = null;
      state.gameType = null;
      state.serverInfo = null;
      notify();
      break;
    }
    case "game_server_ready": {
      state.serverInfo =
        "Server ready: " + msg.address + (msg.join_token ? " (join token " + msg.join_token + ")" : "");
      log("sys", "game server ready: " + msg.address);
      setStatus("Playing on game server — awaiting result", "ok");
      notify();
      break;
    }
    case "game_server_error": {
      setStatus("Game server error: " + msg.message, "err");
      log("sys", "game server error: " + msg.message);
      notify();
      break;
    }
    case "peer_input": {
      // The opponent's real input for `frame` — feed the rollback engine.
      // `target` is a decimal string (bit-exact wire); parseFloat is exact.
      if (state.session) state.session.remoteTarget(msg.frame, parseFloat(msg.target));
      break;
    }
    case "webrtc_offer":
      state.link?.handleSignal("offer", msg.sdp);
      break;
    case "webrtc_answer":
      state.link?.handleSignal("answer", msg.sdp);
      break;
    case "webrtc_ice":
      state.link?.handleIce(msg.candidate);
      break;
    case "match_result": {
      clearStartCountdown();
      state.holdUntilMs = null;
      // The server stores and broadcasts ONE outcome, keyed to player_a's
      // perspective (Win = player_a won, Loss = player_b won). The pong
      // frames told us our side, so render OUR verdict — the two players must
      // see opposite results. mu_change is only ours when we are player_a.
      const o = msg.outcome as
        | { Win: { mu_change?: number } }
        | { Loss: { mu_change?: number } }
        | { Draw: { mu_change?: number } }
        | { Forfeit: { mu_change?: number } }
        | "Disputed"
        | "UnreviewableDispute"
        | null;
      let txt: string;
      if (o && typeof o === "object") {
        const kind = Object.keys(o)[0] as "Win" | "Loss" | "Draw" | "Forfeit";
        const body = (o as Record<string, { mu_change?: number } | undefined>)[kind];
        const muChange = typeof body === "object" && body !== null ? body.mu_change : undefined;
        if (kind === "Forfeit") {
          // Neither player started — double loss; both players see this.
          txt =
            "Match forfeited" +
            (muChange !== undefined ? " (mu change " + muChange.toFixed(2) + ")" : "");
        } else if (state.iAmPlayerA === undefined) {
          // Manual-report flow: sides unknown, fall back to the raw outcome.
          txt = kind + (muChange !== undefined ? " (mu change " + muChange.toFixed(2) + ")" : "");
        } else if (kind === "Draw") {
          txt = "Draw";
        } else {
          const iWon = (kind === "Win" && state.iAmPlayerA) || (kind === "Loss" && !state.iAmPlayerA);
          txt =
            (iWon ? "You won" : "You lost") +
            (state.iAmPlayerA && muChange !== undefined ? " (mu change " + muChange.toFixed(2) + ")" : "");
        }
      } else {
        txt = typeof o === "string" ? (o === "Disputed" ? "Match disputed" : o) : "Unrated";
      }
      setStatus("Match resolved: " + txt, "ok");
      log("sys", "match resolved: " + JSON.stringify(o));
      showControls("connected");
      state.matchToken = null;
      state.gameType = null;
      state.serverInfo = null;
      notify();
      break;
    }
    case "match_started":
      beginStartCountdown(msg.start_timeout_secs);
      state.oppP2p = "Waiting for opponent to start…";
      notify();
      break;
    case "round_start":
      // Referee froze the sim for the 3-2-1: hold rendering at the current
      // frame (no stepping, no input send) until the hold expires.
      state.holdUntilMs = performance.now() + msg.countdown_ticks * 33;
      // The last frame the referee broadcasts while frozen (the hold spans
      // frames `msg.frame + 1 .. msg.frame + countdown_ticks`; the ball
      // launches at `+ 1` after that). Resume must wait until confirmed has
      // reached it — the referee's final frozen broadcast can land a tick
      // after the client-side timer fires, and jumping early would make the
      // client step a frame the referee never steps.
      state.holdEndFrame = (msg.frame ?? 0) + msg.countdown_ticks;
      break;
    case "game_state": {
      // The referee's authoritative frame (~30Hz). The CANVAS renders the
      // local rollback sim (responsive to your input immediately); the score
      // and speed displays come from the server (authoritative).
      beginPongSession({ frame: msg.frame, player_a: msg.player_a, player_b: msg.player_b });
      state.session?.setConfirmed(msg.frame);
      // Round hold (3-2-1): the local sim is frozen and may not have a snapshot
      // yet (round 0) — render the referee's authoritative frozen frame so the
      // canvas shows the ball + paddles under the countdown instead of a black
      // screen. Safe: the sim is identical every held frame.
      if (state.holdUntilMs !== null) {
        state.game = {
          left_y: msg.left_y,
          right_y: msg.right_y,
          ball_x: msg.ball_x,
          ball_y: msg.ball_y,
          left_score: msg.left_score,
          right_score: msg.right_score,
          speed: msg.speed,
        };
      }
      // Local desync check — meaningful only when no rollback is pending for
      // this frame: a prediction that hasn't been replayed yet legitimately
      // differs from the server. A persistent divergence with no pending
      // rollback is a real bug → loud log.
      const localCksum = state.session?.checksumAt(msg.frame) ?? null;
      const rollbackPending =
        state.session?.minIncorrect !== null &&
        state.session !== null &&
        msg.frame >= state.session.minIncorrect;
      if (localCksum !== null && !rollbackPending && localCksum !== BigInt(msg.checksum)) {
        log("sys", `desync at frame ${msg.frame} (server: ${msg.checksum}, local: ${localCksum})`);
      }
      const iAmLeft = state.iAmPlayerA;
      const myScore = iAmLeft ? msg.left_score : msg.right_score;
      const oppScore = iAmLeft ? msg.right_score : msg.left_score;
      state.gameStatus =
        "You " + myScore + " – " + oppScore + " Opponent — ball speed ×" + (msg.speed / BALL_SPEED).toFixed(2);
      break;
    }
    case "input_ack": {
      if (state.session) {
        state.session.setConfirmed(msg.frame);
        // Server RTT: measure the game_input → input_ack round trip (the real
        // gameplay path — network + the referee's 33ms tick).
        const sentAt = state.pendingAcks.get(msg.frame);
        if (sentAt !== undefined) {
          metrics.serverRtt = performance.now() - sentAt;
          state.pendingAcks.delete(msg.frame);
        }
        // Referee health check: report the settled checksum for the confirmed
        // frame. Skip while a rollback is pending — the local engine is
        // converging on its own; a real (unfixable) divergence is reported on
        // the next settled frame.
        const pending =
          state.session.minIncorrect !== null && msg.frame >= state.session.minIncorrect;
        const cksum = state.session.checksumAt(msg.frame);
        if (cksum !== null && !pending) {
          send({
            type: "rollback_health",
            match_token: state.matchToken ?? "",
            frame: msg.frame,
            checksum: cksum.toString(),
          });
        }
      }
      break;
    }
    case "rollback_resync":
      if (state.session) {
        state.session.restore(msg.frame, hexToBytes(msg.state));
        state.stalled = false; // resume stepping from the authoritative state
        log("sys", `resynced to frame ${msg.frame}`);
      }
      break;
    case "game_over": {
      stopGame(); // stop both interval loops; the canvas keeps the final frame
      rpsGameOver(msg.winner);
      state.gameStatus = msg.winner === state.playerId ? "You win!" : "You lose";
      notify();
      break;
    }
    case "error":
      if (msg.code === "invalid_report") {
        // A rejected (e.g. duplicate) report: log it, don't yank a mid-match
        // player back to the connected panel.
        log("sys", msg.message);
        break;
      }
      setStatus("Error: " + msg.message, "err");
      showControls("connected");
      break;
    case "rps_begin": {
      // Switch the game area to the RPS panel (the pong canvas is unused here).
      stopGame();
      beginRpsRound({ round: msg.round, player_a: msg.player_a });
      log("sys", `rps round ${msg.round} open (${msg.timeout_ms}ms window)`);
      break;
    }
    case "rps_round":
      rpsRoundResult(msg);
      log(
        "sys",
        `rps round ${msg.round}: a=${msg.a_choice} b=${msg.b_choice} winner=${msg.winner} score ${msg.a_score}-${msg.b_score}`
      );
      break;
  }
}

