// WebRTC data-channel setup for pong peer-to-peer inputs, with the
// RTCPeerConnection feature-detect (fix: browsers without WebRTC crashed at
// `new RTCPeerConnection`). The WS relay carries every input as the fallback,
// so the game stays fully playable when the data channel is unavailable.
import { WrtcLink } from "../../../pong-wrtc.mjs";
import type { ClientMessage } from "../lobby/protocol";
import { log, send, state } from "../lobby/store";

/**
 * Create (once) the WrtcLink for the current match and start the handshake.
 * Player A is the offerer, player B the answerer. Safe to call when a link
 * already exists (no-op).
 */
export async function ensureWrtcLink() {
  if (state.link) return;
  if (typeof RTCPeerConnection === "undefined") {
    log("sys", "WebRTC unavailable — inputs via server relay only");
    return;
  }
  const base = state.lastBase || "";
  let iceServers: RTCIceServer[] = [];
  try {
    const resp = await fetch(base + "/internal/turn-credentials");
    if (resp.ok) {
      const body = await resp.json();
      iceServers = [{ urls: body.uris, username: body.username, credential: body.password }];
    } else {
      log("sys", "TURN unavailable — host candidates only");
    }
  } catch {
    log("sys", "TURN unavailable — host candidates only");
  }
  state.link = new WrtcLink({
    role: state.iAmPlayerA ? "offer" : "answer",
    iceServers,
    sendSignal: (kind, payload) => {
      const msg = {
        type: kind,
        match_token: state.matchToken,
        ...(payload as { sdp?: string; candidate?: string }),
      } as ClientMessage;
      send(msg);
    },
    onMessage: (m) => {
      const g = m as { type?: string; frame?: number; target?: string };
      if (g.type === "game_input" && state.session) {
        state.session.remoteTarget(g.frame ?? 0, parseFloat(g.target ?? ""));
      }
    },
    onStateChange: (s) => {
      log("sys", "WebRTC " + s);
      if (s === "connected") log("sys", "inputs via data channel");
    },
  });
  state.link.start();
}
