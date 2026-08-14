// Type declarations for web/pong-sim.mjs (the node-tested bit-exact sim).
// Hand-maintained mirror of the exported surface — the .mjs is the source of
// truth; keep in sync when the module changes.

export declare const PongSide: {
  readonly Left: "left";
  readonly Right: "right";
};
export type PongSideValue = "left" | "right";

export declare const WIN_SCORE: number;
export declare const TICK_MS: number;
export declare const DT_SECS: number;
export declare const PADDLE_HALF_HEIGHT: number;
export declare const PADDLE_X_LEFT: number;
export declare const PADDLE_X_RIGHT: number;
export declare const PADDLE_SPEED: number;
export declare const PADDLE_CLAMP: [number, number];
export declare const BALL_SPEED: number;
export declare const SPEED_UP: number;
export declare const MAX_SPEED: number;
export declare const HIT_RADIUS: number;
export declare const MAX_SUBSTEP_TRAVEL: number;

export declare function fnv1a64(bytes: Uint8Array): bigint;

export interface PongSnapshot {
  left_y: number;
  right_y: number;
  ball_x: number;
  ball_y: number;
  left_score: number;
  right_score: number;
  speed: number;
}

export declare class PongSim {
  constructor(opts?: { scoring?: boolean });
  scoring: boolean;
  reset(): void;
  setTarget(side: string, target: number): void;
  step(dt: number): void;
  snapshot(): PongSnapshot;
  winner(): string | null;
  fullState(): Uint8Array;
  restore(bytes: Uint8Array): void;
  checksum(): bigint;
  stateForTest(): unknown;
  movePaddle(side: string, dt: number): void;
  stepBall(dt: number): void;
  bounceWalls(): void;
  hitPaddle(): boolean;
  serve(scorer: string): void;
}
