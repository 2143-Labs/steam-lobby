// Type declarations for web/pong-wrtc.mjs (the WebRTC data-channel glue).
// Hand-maintained mirror of the exported surface — the .mjs is the source of
// truth; keep in sync when the module changes.

export type WrtcRole = "offer" | "answer";
export type WrtcLinkState =
  | "unsupported"
  | "connected"
  | "disconnected"
  | "closed"
  | "failed"
  | string;

export declare class WrtcLink {
  constructor(opts: {
    role: WrtcRole;
    iceServers: RTCIceServer[];
    sendSignal: (kind: string, payload: Record<string, unknown>) => void;
    onMessage: (msg: unknown) => void;
    onStateChange: (state: WrtcLinkState) => void;
  });

  pc: RTCPeerConnection | null;
  start(): void | Promise<void>;
  handleSignal(kind: WrtcRole, sdp: string): Promise<void>;
  handleIce(candidateJson: string): Promise<void>;
  send(msg: unknown): void;
  close(): void;
  readonly channelState: string;
}
