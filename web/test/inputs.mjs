// Shared deterministic input schedule for every JS determinism test — the
// exact mirror of the Rust schedule in lobby-core/tests/determinism.rs:
//   target(side, frame) = (((floor(frame/5) * 7919 + off) % 997) / 997.0)
// off: left = 0, right = 331. Pure integer math, exact in both languages.

export const runLength = 10000;

const OFF = { left: 0, right: 331 };

export function schedule(frame, side) {
  const off = side === "left" ? OFF.left : OFF.right;
  return (((Math.floor(frame / 5) * 7919 + off) % 997)) / 997;
}
