// The 3-replica convergence test: a server-style referee (bare PongSim gated
// exactly like the M3 server task — advance only when BOTH players' newest
// known input >= the next frame) plus two RollbackSession clients (A = left,
// B = right) fed the same inputs through an in-memory pump with per-link
// delays [0, 3, 10]. Every frame the referee reached, the clients' checksums
// must equal the referee's, and all three winners must agree.
//
// Also verifies resync recovery: corrupt A's sim mid-match, observe the
// divergence, restore the referee's authoritative state, and watch A
// re-converge — the match outcome is unchanged.

import { PongSim, PongSide, DT_SECS } from "../pong-sim.mjs";
import { RollbackSession } from "../pong-rollback.mjs";
import { schedule } from "./inputs.mjs";

const FRAMES = 600;

/** Server stand-in: only advances to a frame once BOTH inputs are known. */
class Referee {
  constructor() {
    this.sim = new PongSim();
    this.frame = -1;
    this.known = { left: new Map(), right: new Map() };
    this.checksums = []; // checksums[f] = checksum after stepping frame f
  }
  receiveInput(side, frame, target) {
    this.known[side].set(frame, target);
  }
  newest(side) {
    let m = -1;
    for (const f of this.known[side].keys()) if (f > m) m = f;
    return m;
  }
  /** Advance while both sides have input for the next frame; returns the frames advanced. */
  advance() {
    const advanced = [];
    while (this.newest("left") >= this.frame + 1 && this.newest("right") >= this.frame + 1) {
      const next = this.frame + 1;
      this.sim.setTarget(PongSide.Left, this.known.left.get(next));
      this.sim.setTarget(PongSide.Right, this.known.right.get(next));
      this.sim.step(DT_SECS);
      this.frame = next;
      this.checksums[next] = this.sim.checksum();
      advanced.push(next);
    }
    return advanced;
  }
}

const slot = (f, ringSize) => ((f % ringSize) + ringSize) % ringSize;

/** Winner on the saved state at `frame` (reads the session's snapshot ring). */
function winnerAt(session, frame) {
  const entry = session.snapshots[slot(frame, session.ringSize)];
  if (!entry || entry.frame !== frame) return null;
  const s = new PongSim();
  s.restore(entry.bytes);
  return s.winner();
}

function assertConverged(referee, A, B, at, delay) {
  for (let g = 0; g <= referee.frame; g++) {
    const ac = A.checksumAt(g);
    const bc = B.checksumAt(g);
    if (ac === null || bc === null) continue; // evicted / not yet reached
    if (ac !== referee.checksums[g] || bc !== referee.checksums[g]) {
      throw new Error(
        `replica: divergence at frame ${g} (delay ${delay}, at ${at}): A=${ac} B=${bc} ref=${referee.checksums[g]}`
      );
    }
  }
}

function runReplica(delay) {
  const referee = new Referee();
  const A = new RollbackSession({ sim: new PongSim(), side: "left", windowSize: 10, ringSize: 128 });
  const B = new RollbackSession({ sim: new PongSim(), side: "right", windowSize: 10, ringSize: 128 });
  const pendingA = []; // B's inputs → A (delivered at t + delay)
  const pendingB = []; // A's inputs → B
  const pendingRef = []; // both → referee

  for (let t = 0; t < FRAMES; t++) {
    const aT = schedule(t, "left");
    const bT = schedule(t, "right");
    A.localTarget(t, aT);
    B.localTarget(t, bT);
    (pendingA[t + delay] ??= []).push(() => A.remoteTarget(t, bT));
    (pendingB[t + delay] ??= []).push(() => B.remoteTarget(t, aT));
    (pendingRef[t + delay] ??= []).push(() => {
      referee.receiveInput("left", t, aT);
      referee.receiveInput("right", t, bT);
    });

    for (const q of [pendingA, pendingB, pendingRef]) {
      for (const fn of q[t] ?? []) fn();
    }
    const advanced = referee.advance();
    for (const g of advanced) {
      A.setConfirmed(g);
      B.setConfirmed(g);
    }
    A.step();
    B.step();
    assertConverged(referee, A, B, t, delay);
  }

  // Tail: let the last delayed inputs land and the referee reach the end.
  for (let t = FRAMES; t < FRAMES + delay + 5; t++) {
    for (const q of [pendingA, pendingB, pendingRef]) {
      for (const fn of q[t] ?? []) fn();
    }
    const advanced = referee.advance();
    for (const g of advanced) {
      A.setConfirmed(g);
      B.setConfirmed(g);
    }
    A.step();
    B.step();
  }
  // The sessions keep predicting past the referee; stop them at the referee's
  // last frame so all three winners are compared on the SAME state.
  assertConverged(referee, A, B, "tail", delay);
  const wA = winnerAt(A, referee.frame);
  const wB = winnerAt(B, referee.frame);
  const wR = referee.sim.winner();
  if (wA !== wB || wB !== wR) {
    throw new Error(`replica: winner disagreement (delay ${delay}): A=${wA} B=${wB} ref=${wR}`);
  }
  return referee.frame;
}

function runResync() {
  // Same pump, delay 0 (no pending predictions — a pending rollback would
  // silently heal a corrupted sim from the local snapshot ring; the scenario
  // under test is a client that needs the server's AUTHORITATIVE resync).
  const delay = 0;
  const referee = new Referee();
  const A = new RollbackSession({ sim: new PongSim(), side: "left", windowSize: 10, ringSize: 128 });
  const B = new RollbackSession({ sim: new PongSim(), side: "right", windowSize: 10, ringSize: 128 });
  const pendingA = [];
  const pendingB = [];
  const pendingRef = [];

  const pumpIteration = (t) => {
    if (t < FRAMES) {
      const aT = schedule(t, "left");
      const bT = schedule(t, "right");
      A.localTarget(t, aT);
      B.localTarget(t, bT);
      (pendingA[t + delay] ??= []).push(() => A.remoteTarget(t, bT));
      (pendingB[t + delay] ??= []).push(() => B.remoteTarget(t, aT));
      (pendingRef[t + delay] ??= []).push(() => {
        referee.receiveInput("left", t, aT);
        referee.receiveInput("right", t, bT);
      });
    }
    for (const q of [pendingA, pendingB, pendingRef]) {
      for (const fn of q[t] ?? []) fn();
    }
    const advanced = referee.advance();
    for (const g of advanced) {
      A.setConfirmed(g);
      B.setConfirmed(g);
    }
    A.step();
    B.step();
    return referee.frame;
  };

  for (let t = 0; t < 201; t++) pumpIteration(t); // referee and A at frame 200
  const refState200 = referee.sim.fullState(); // authoritative state AT frame 200
  const frame200 = referee.frame;
  if (frame200 !== 200) throw new Error(`resync: referee not at frame 200 (got ${frame200})`);

  // Corrupt A's sim: flip a byte in the middle of ball_x.
  const evil = A.sim.fullState();
  evil[20] ^= 0xff;
  A.sim.restore(evil);

  pumpIteration(201); // A steps one frame on the corrupted sim
  const aCksum = A.checksumAt(201);
  const refCksum = referee.checksums[201];
  if (aCksum === refCksum) {
    throw new Error(`resync: corruption was not detected (A=${aCksum} ref=${refCksum} at frame 201)`);
  }

  // Resync A to the referee's authoritative state at frame 200.
  A.restore(frame200, refState200);
  if (A.checksumAt(frame200) !== referee.checksums[frame200]) {
    throw new Error("resync: A did not restore to the referee state");
  }

  for (let t = 202; t < FRAMES + 5; t++) pumpIteration(t); // 50+ more frames

  // Re-converged: A matches the referee at its last frame, and the winner
  // (on the shared state) is unchanged.
  const last = referee.frame;
  if (A.checksumAt(last) !== referee.checksums[last] || B.checksumAt(last) !== referee.checksums[last]) {
    throw new Error(
      `resync: A did not re-converge (last ${last}): A=${A.checksumAt(last)} B=${B.checksumAt(last)} ref=${referee.checksums[last]}`
    );
  }
  const wA = winnerAt(A, last);
  const wB = winnerAt(B, last);
  const wR = referee.sim.winner();
  if (wA !== wB || wB !== wR) {
    throw new Error(`resync: winner disagreement after recovery: A=${wA} B=${wB} ref=${wR}`);
  }
}

for (const delay of [0, 3, 10]) runReplica(delay);
console.log("replica: OK (delays 0/3/10)");
runResync();
console.log("resync: OK");
