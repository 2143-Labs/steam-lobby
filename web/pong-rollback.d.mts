// Type declarations for web/pong-rollback.mjs (the client rollback session).
// Hand-maintained mirror of the exported surface — the .mjs is the source of
// truth; keep in sync when the module changes.

import type { PongSim, PongSnapshot } from "./pong-sim.mjs";

export declare class RollbackSession {
  constructor(opts: {
    sim: PongSim;
    side?: string;
    windowSize?: number;
    ringSize?: number;
  });

  sim: PongSim;
  side: string;
  other: string;
  windowSize: number;
  ringSize: number;
  frame: number; // last frame advanced to; -1 = the initial state
  confirmed: number; // newest frame acked by the server (InputAck)
  minIncorrect: number | null; // earliest incorrect frame, or null

  slot(f: number): number;
  localTarget(frame: number, target: number | null): void;
  remoteTarget(frame: number, target: number | null): void;
  setConfirmed(frame: number): void;
  skipTo(frame: number): void;
  step(): { snapshot: PongSnapshot | null; rolledBack: boolean; stalled: boolean };
  checksumAt(frame: number): bigint | null;
  restore(frame: number, stateBytes: Uint8Array): void;
  winner(): string | null;
}
