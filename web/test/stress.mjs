// GekkoNet-style rollback torture: one session + a straight-line reference
// sim. Every remote input arrives DELAY frames late (so frames are predicted
// with hold-last, then corrected); every 50 frames we ALSO deliver a
// deliberately WRONG target for an old frame, then the correct one — forcing
// an extra rollback per cycle. After each correction the session must match
// the reference checksum-for-checksum. Prints "stress: OK".

import { PongSim, PongSide, DT_SECS } from "../pong-sim.mjs";
import { RollbackSession } from "../pong-rollback.mjs";
import { schedule } from "./inputs.mjs";

const N = 500;
const DELAY = 3;
const TOTAL = N + DELAY + 5; // tail iterations to flush the last deliveries

const reference = new PongSim();
const refChecksums = []; // refChecksums[f] = checksum after stepping frame f
const session = new RollbackSession({ sim: new PongSim(), side: "left", windowSize: 10, ringSize: 128 });

const pending = []; // pending[time] = array of delivery thunks
let newestDelivered = -1; // newest frame whose real input reached the session

function assertConverged(upTo, at) {
  for (let g = 0; g <= upTo; g++) {
    const got = session.checksumAt(g);
    if (got === null) continue; // evicted from the snapshot ring (older than ringSize)
    if (got !== refChecksums[g]) {
      throw new Error(`stress: divergence at frame ${g} (at ${at}): session=${got} reference=${refChecksums[g]}`);
    }
  }
}

for (let t = 0; t < TOTAL; t++) {
  if (t < N) {
    // Reference: real inputs every frame, no prediction.
    reference.setTarget(PongSide.Left, schedule(t, "left"));
    reference.setTarget(PongSide.Right, schedule(t, "right"));
    reference.step(DT_SECS);
    refChecksums[t] = reference.checksum();
    session.setConfirmed(t);

    // Session inputs: mine are real immediately; the opponent's arrive late.
    session.localTarget(t, schedule(t, "left"));
    (pending[t + DELAY] ??= []).push(() => {
      session.remoteTarget(t, schedule(t, "right"));
      newestDelivered = Math.max(newestDelivered, t);
    });
    // Torture: a wrong target for this frame, then the correct one — forces
    // the session to roll back twice (wrong → mark; correct → mark again).
    if (t % 50 === 0 && t > 0) {
      (pending[t + DELAY + 1] ??= []).push(() => {
        session.remoteTarget(t, schedule(t, "right") + 0.37);
        newestDelivered = Math.max(newestDelivered, t);
      });
      (pending[t + DELAY + 2] ??= []).push(() => {
        session.remoteTarget(t, schedule(t, "right"));
        newestDelivered = Math.max(newestDelivered, t);
      });
    }
  }

  for (const fn of pending[t] ?? []) fn();
  session.step();

  // After each torture cycle's correction has been applied, every delivered
  // frame must match the reference exactly.
  const cycleFrame = t - (DELAY + 2);
  if (cycleFrame >= 50 && cycleFrame % 50 === 0) {
    assertConverged(newestDelivered, t);
  }
}

// Final: flush every remaining delivery and let the session settle past the
// reference's last frame, then verify the whole 0..N-1 range.
for (let t = TOTAL; t < N + DELAY + 3; t++) {
  for (const fn of pending[t] ?? []) fn();
}
for (let i = 0; i < N + DELAY + 3; i++) {
  session.step();
}
assertConverged(N - 1, "final");
console.log("stress: OK");
