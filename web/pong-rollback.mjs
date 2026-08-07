// Client-side rollback session — the simplified 1v1 core of the sync design,
// mirroring GekkoNet's InputBuffer/SyncSystem:
//
//   - Per-player input ring (`{frame, target}`, `frame % ringSize` indexing,
//     sequential insertion only — out-of-order arrivals are dropped; a
//     reliable ordered channel means this never fires).
//   - Prediction: a frame with no real input uses hold-last (the newest real
//     target below it; null → the paddle stays). The sim stores the paddle
//     goal, so hold-last is the natural predictor.
//   - Never predict more than `windowSize` frames past `confirmed` — beyond
//     that, `step()` returns `{ stalled: true }` and nothing advances.
//   - Snapshot ring: every stepped frame saves its 74-byte state + FNV-1a 64
//     checksum (74 B × 128 ≈ 9.5 KB — no limited-saving optimization needed).
//   - When the opponent's REAL input for an already-stepped frame arrives and
//     differs from what we applied, the frame is marked incorrect; the next
//     `step()` rolls back to `minIncorrect - 1` (restoring the saved state)
//     and replays with the real inputs.
//
// `side` is which sim side THIS client is (PongSide.Left → `localTarget`
// drives the left paddle, `remoteTarget` the right — and vice versa).
// Checksums are `bigint`; the decimal-string wire conversion lives in the
// caller (the demo / tests), never here.

import { PongSide, DT_SECS, fnv1a64 } from "./pong-sim.mjs";

const OPPOSITE = { [PongSide.Left]: PongSide.Right, [PongSide.Right]: PongSide.Left };

export class RollbackSession {
  /**
   * @param {{sim: PongSim, side?: string, windowSize?: number, ringSize?: number}} opts
   *   `sim` must be a fresh PongSim at the initial state (frame -1).
   */
  constructor({ sim, side = PongSide.Left, windowSize = 10, ringSize = 128 }) {
    this.sim = sim;
    this.side = side;
    this.other = OPPOSITE[side];
    this.windowSize = windowSize;
    this.ringSize = ringSize;
    this.rings = {
      [PongSide.Left]: new Array(ringSize).fill(null),
      [PongSide.Right]: new Array(ringSize).fill(null),
    };
    this.snapshots = new Array(ringSize).fill(null);
    this.frame = -1; // last frame advanced to; -1 = the initial state
    this.confirmed = -1; // newest frame acked by the server (InputAck)
    this.incorrect = new Set(); // frames whose prediction was wrong
    this.minIncorrect = null; // earliest incorrect frame, or null
    // The initial state IS the snapshot at frame -1 (rollback can go there).
    this.snapshots[this.slot(-1)] = {
      frame: -1,
      bytes: this.sim.fullState(),
      checksum: this.sim.checksum(),
      applied: { [PongSide.Left]: null, [PongSide.Right]: null },
    };
  }

  slot(f) {
    return ((f % this.ringSize) + this.ringSize) % this.ringSize;
  }

  /** My real input for `frame`. */
  localTarget(frame, target) {
    this.insert(this.rings[this.side], frame, target);
  }

  /** The opponent's real input for `frame` (arrives via PeerInput relay). */
  remoteTarget(frame, target) {
    const slot = this.snapshots[this.slot(frame)];
    if (slot && slot.frame === frame) {
      // Already stepped past `frame`: if what we applied differs from the real
      // input, the prediction was wrong → mark for rollback.
      if (slot.applied[this.other] !== target) this.incorrect.add(frame);
      else this.incorrect.delete(frame);
      this.minIncorrect = this.incorrect.size ? Math.min(...this.incorrect) : null;
    }
    this.insert(this.rings[this.other], frame, target);
  }

  /** Advance `confirmed` from the server's InputAck. */
  setConfirmed(frame) {
    if (frame > this.confirmed) this.confirmed = frame;
  }

  /**
   * Advance one frame. Returns `{ snapshot, rolledBack, stalled }`; when
   * stalled (prediction window cap), `snapshot` is null and nothing advanced.
   */
  step() {
    const next = this.frame + 1;
    if (next > this.confirmed + this.windowSize) {
      return { snapshot: null, rolledBack: false, stalled: true };
    }
    let rolledBack = false;
    if (this.minIncorrect !== null && this.minIncorrect <= this.frame) {
      const from = this.minIncorrect - 1;
      const snap = this.snapshots[this.slot(from)];
      if (!snap || snap.frame !== from) {
        throw new Error(
          `rollback snapshot for frame ${from} evicted (windowSize ${this.windowSize}, ringSize ${this.ringSize}) — cannot recover locally; need server resync`
        );
      }
      this.sim.restore(snap.bytes);
      this.frame = from;
      this.incorrect.clear();
      this.minIncorrect = null;
      rolledBack = true;
      for (let f = from + 1; f <= next; f++) this.advanceTo(f);
    } else {
      this.advanceTo(next);
    }
    return { snapshot: this.sim.snapshot(), rolledBack, stalled: false };
  }

  /** FNV-1a 64 of the saved state at `frame` (null when evicted). */
  checksumAt(frame) {
    const slot = this.snapshots[this.slot(frame)];
    return slot && slot.frame === frame ? slot.checksum : null;
  }

  /**
   * Resync from the server's authoritative state: replace the sim, truncate
   * both rings to future frames only, and keep the resynced snapshot alone.
   */
  restore(frame, stateBytes) {
    this.sim.restore(stateBytes);
    this.frame = frame;
    this.confirmed = frame;
    this.incorrect.clear();
    this.minIncorrect = null;
    for (const side of [PongSide.Left, PongSide.Right]) {
      const ring = this.rings[side];
      for (let i = 0; i < this.ringSize; i++) {
        if (ring[i] && ring[i].frame <= frame) ring[i] = null;
      }
    }
    // Everything after `frame` was computed on the divergent trajectory —
    // only the resynced state survives.
    this.snapshots.fill(null);
    const bytes = new Uint8Array(stateBytes);
    this.snapshots[this.slot(frame)] = {
      frame,
      bytes,
      checksum: fnv1a64(bytes),
      applied: {
        [PongSide.Left]: this.inputFor(this.rings[PongSide.Left], frame),
        [PongSide.Right]: this.inputFor(this.rings[PongSide.Right], frame),
      },
    };
  }

  /** The winner on the CURRENT sim state (ahead of confirmed). */
  winner() {
    return this.sim.winner();
  }

  // ── internals ──────────────────────────────────────────────────────────

  /** Sequential ring insert; a newer frame already in the slot = out of order → drop. */
  insert(ring, frame, target) {
    const slot = ring[frame % this.ringSize];
    if (slot && slot.frame > frame) return;
    ring[frame % this.ringSize] = { frame, target };
  }

  /** Real input for `frame`, else hold-last (newest real target with frame < f). */
  inputFor(ring, frame) {
    const slot = ring[frame % this.ringSize];
    if (slot && slot.frame === frame) return slot.target;
    let best = null;
    let bestFrame = -Infinity;
    for (const e of ring) {
      if (e && e.frame < frame && e.frame > bestFrame) {
        best = e.target;
        bestFrame = e.frame;
      }
    }
    return best;
  }

  /** Apply the frame's inputs, step, and save the snapshot. */
  advanceTo(f) {
    const left = this.inputFor(this.rings[PongSide.Left], f);
    const right = this.inputFor(this.rings[PongSide.Right], f);
    if (left !== null) this.sim.setTarget(PongSide.Left, left);
    if (right !== null) this.sim.setTarget(PongSide.Right, right);
    this.sim.step(DT_SECS);
    this.snapshots[this.slot(f)] = {
      frame: f,
      bytes: this.sim.fullState(),
      checksum: this.sim.checksum(),
      applied: { [PongSide.Left]: left, [PongSide.Right]: right },
    };
    this.frame = f;
  }
}
