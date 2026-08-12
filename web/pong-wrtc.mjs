// pong-wrtc.mjs — WebRTC data-channel glue for pong peer-to-peer inputs.
//
// Pure module: no DOM id access, no logging (the demo logs via its own `log`
// helper). Browser RTCPeerConnection / RTCDataChannel APIs only.
//
// Usage in index.html:
//   import { WrtcLink } from "/pong-wrtc.mjs";
//   const link = new WrtcLink({ role, iceServers, sendSignal, onMessage, onStateChange });
//   await link.start();
//
// The ws relay carries every input as an automatic fallback — the data channel
// is additive for lower-latency prediction. Double-feeding remoteTarget() with
// the same frame+target is a no-op.

export class WrtcLink {
  /**
   * @param {Object} opts
   * @param {"offer"|"answer"} opts.role — "offer" for player_a, "answer" for player_b.
   * @param {Array}      opts.iceServers — passed to RTCPeerConnection.
   * @param {Function}   opts.sendSignal — (kind, payload) => void; kind is "webrtc_offer"|"webrtc_answer"|"webrtc_ice".
   * @param {Function}   opts.onMessage — (msg) => void; called with parsed JSON from the data channel.
   * @param {Function}   opts.onStateChange — (state) => void; called with pc.connectionState changes.
   */
  constructor({ role, iceServers, sendSignal, onMessage, onStateChange }) {
    this._role = role;
    this._iceServers = iceServers;
    this._sendSignal = sendSignal;
    this._onMessage = onMessage;
    this._onStateChange = onStateChange;
    this.pc = null;
    this._channel = null;
    this._iceBuffer = []; // buffered until setRemoteDescription runs
  }

  /** Returns a promise so tests can await signal delivery. */
  start() {
    if (this.pc) return;
    this.pc = new RTCPeerConnection({ iceServers: this._iceServers });
    this.pc.onicecandidate = (e) => {
      if (e.candidate) {
        const cand = e.candidate.toJSON ? e.candidate.toJSON() : e.candidate;
        this._sendSignal("webrtc_ice", { candidate: JSON.stringify(cand) });
      }
    };
    this.pc.onconnectionstatechange = () => {
      this._onStateChange(this.pc.connectionState);
    };

    if (this._role === "offer") {
      const channel = this.pc.createDataChannel("pong");
      this._wire(channel);
      return this.pc.createOffer()
        .then((offer) => this.pc.setLocalDescription(offer))
        .then(() => {
          this._sendSignal("webrtc_offer", { sdp: this.pc.localDescription.sdp });
        })
        .catch(() => this._onStateChange("failed"));
    } else {
      this.pc.ondatachannel = (e) => this._wire(e.channel);
    }
  }

  /**
   * Handle an incoming signaling message from the peer.
   * @param {"offer"|"answer"} kind
   * @param {string} sdp
   */
  async handleSignal(kind, sdp) {
    if (!this.pc) return;
    try {
      if (kind === "offer") {
        await this.pc.setRemoteDescription({ type: "offer", sdp });
        this._flushIce();
        const answer = await this.pc.createAnswer();
        await this.pc.setLocalDescription(answer);
        this._sendSignal("webrtc_answer", { sdp: this.pc.localDescription.sdp });
      } else if (kind === "answer") {
        await this.pc.setRemoteDescription({ type: "answer", sdp });
        this._flushIce();
      }
    } catch {
      this._onStateChange("failed");
    }
  }

  /**
   * Handle an inbound ICE candidate JSON string.
   * @param {string} candidateJson
   */
  async handleIce(candidateJson) {
    if (!this.pc) return;
    if (!this.pc.remoteDescription) {
      this._iceBuffer.push(candidateJson);
      return;
    }
    try {
      await this.pc.addIceCandidate(new RTCIceCandidate(JSON.parse(candidateJson)));
    } catch {
      // Browser rejects duplicate/malformed candidates — ignore.
    }
  }

  /**
   * Send a message over the data channel (JSON-serialized).
   * Drops silently when the channel is not open — the ws relay covers it.
   * @param {*} msg
   */
  send(msg) {
    if (this._channel && this._channel.readyState === "open") {
      this._channel.send(JSON.stringify(msg));
    }
  }

  close() {
    if (this.pc) {
      this.pc.close();
      this.pc = null;
    }
    this._channel = null;
    this._iceBuffer = [];
    this._onStateChange("closed");
  }

  /** The data channel's readyState ("new"|"connecting"|"open"|"closed"). */
  get channelState() { return this._channel ? this._channel.readyState : "new"; }

  // ── private ──

  _wire(channel) {
    this._channel = channel;
    channel.onopen = () => this._onStateChange("connected");
    channel.onclose = () => this._onStateChange("disconnected");
    channel.onmessage = (e) => {
      try {
        this._onMessage(JSON.parse(e.data));
      } catch {
        // Ignore garbled messages.
      }
    };
  }

  _flushIce() {
    while (this._iceBuffer.length > 0) {
      const c = this._iceBuffer.shift();
      this.handleIce(c);
    }
  }
}
