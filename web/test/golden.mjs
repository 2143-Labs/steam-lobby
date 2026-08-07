// JS-side golden: run the shared schedule through PongSim and assert the five
// checkpoints equal the Rust golden_frame_hashes literals (generated once,
// pasted into BOTH this file and lobby-core/tests/determinism.rs — never
// regenerate one side alone). Prints "golden: OK" on success.

import { PongSim, PongSide, DT_SECS } from "../pong-sim.mjs";
import { schedule, runLength } from "./inputs.mjs";

const expected = [
  [0, 6342930175324611133n],
  [10, 864245196399882248n],
  [100, 1310121980415934884n],
  [1000, 7894357006564238283n],
  [10000, 10952538874955448200n],
];

const sim = new PongSim();
const got = [[0, sim.checksum()]];
for (let frame = 0; frame < runLength; frame++) {
  sim.setTarget(PongSide.Left, schedule(frame, "left"));
  sim.setTarget(PongSide.Right, schedule(frame, "right"));
  sim.step(DT_SECS);
  const n = frame + 1;
  if (n === 10 || n === 100 || n === 1000 || n === 10000) got.push([n, sim.checksum()]);
}

for (let i = 0; i < expected.length; i++) {
  const [frame, want] = expected[i];
  const [gotFrame, gotCksum] = got[i];
  if (gotFrame !== frame || gotCksum !== want) {
    console.error(`golden mismatch at frame ${frame}: JS=${gotCksum} want=${want}`);
    process.exit(1);
  }
}
console.log("golden: OK");
