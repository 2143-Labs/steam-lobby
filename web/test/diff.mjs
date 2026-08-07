// Cross-language differential harness (JS side): run the shared schedule for
// all 10,000 frames and write every frame's checksum as a decimal string to
// web/test/.js-hashes.json. The Rust test differential_js_matches_rust
// (lobby-core/tests/determinism.rs, #[ignore]d) recomputes the same hashes
// with the Rust sim and asserts every entry matches — proving bit-exact parity
// across the whole trajectory, not just the five golden checkpoints.

import { writeFileSync } from "node:fs";
import { PongSim, PongSide, DT_SECS } from "../pong-sim.mjs";
import { schedule, runLength } from "./inputs.mjs";

const sim = new PongSim();
const checkpoints = { "0": sim.checksum().toString() };
for (let frame = 0; frame < runLength; frame++) {
  sim.setTarget(PongSide.Left, schedule(frame, "left"));
  sim.setTarget(PongSide.Right, schedule(frame, "right"));
  sim.step(DT_SECS);
  checkpoints[String(frame + 1)] = sim.checksum().toString();
}

const out = new URL("./.js-hashes.json", import.meta.url);
writeFileSync(out, JSON.stringify({ checkpoints }, null, 0) + "\n");
console.log("wrote " + out.pathname);
