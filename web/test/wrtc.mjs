// wrtc.mjs — offline WebRTC glue test against a scripted FakePc.
//
// Exercises the WrtcLink class against a mock RTCPeerConnection that records
// calls in order — no browser, no network. Run: `node web/test/wrtc.mjs`

import { WrtcLink } from "../pong-wrtc.mjs";

let failures = 0;
function assert(cond, msg) {
  if (!cond) { console.error("FAIL: " + msg); failures++; }
  else console.log("  ok  " + msg);
}

// ── Scripted RTCPeerConnection (mirrors the DOM API just enough) ──

class FakePc {
  constructor() {
    this.localDescription = null;
    this.remoteDescription = null;
    this._dcCreated = null;
    this._dcRemote = null;
    this._callOrder = [];
    this._iceCount = 0;
  }

  createDataChannel(label) {
    this._dcCreated = new FakeChannel();
    return this._dcCreated;
  }

  async createOffer() {
    this._callOrder.push("createOffer");
    return { type: "offer", sdp: "offer-sdp" };
  }

  async createAnswer() {
    this._callOrder.push("createAnswer");
    return { type: "answer", sdp: "answer-sdp" };
  }

  async setLocalDescription(desc) {
    this._callOrder.push("setLocal" + desc.type);
    this.localDescription = { type: desc.type, sdp: desc.sdp };
    // Fire one ICE candidate
    this._iceCount++;
    if (this.onicecandidate) {
      this.onicecandidate({ candidate: { toJSON: () => ({ candidate: "candidate-" + this._iceCount }) } });
    }
  }

  async setRemoteDescription(desc) {
    this._callOrder.push("setRemote" + desc.type);
    this.remoteDescription = { type: desc.type, sdp: desc.sdp };
  }

  async addIceCandidate(cand) {
    this._callOrder.push("addIce(" + cand.candidate + ")");
  }

  close() {
    this._callOrder.push("close");
    if (this.onconnectionstatechange) {
      this.connectionState = "closed";
      this.onconnectionstatechange();
    }
  }
}

class FakeChannel {
  constructor() {
    this.readyState = "open";
    this.sent = [];
  }
  send(data) {
    this.sent.push(JSON.parse(data));
  }
}

// ── Test ──

const signalsFromA = [];
const signalsFromB = [];
const statesA = [];
const statesB = [];
const messagesA = [];
const messagesB = [];

// Patch constructors
globalThis.RTCPeerConnection = FakePc;
globalThis.RTCIceCandidate = class { constructor(d) { this.candidate = d.candidate; } };

let pcA, pcB;

// Offer side (player A)
const linkA = new WrtcLink({
  role: "offer",
  iceServers: [],
  sendSignal: (kind, payload) => signalsFromA.push({ kind, ...payload }),
  onMessage: (m) => messagesA.push(m),
  onStateChange: (s) => statesA.push(s),
});

// Answer side (player B)
const linkB = new WrtcLink({
  role: "answer",
  iceServers: [],
  sendSignal: (kind, payload) => signalsFromB.push({ kind, ...payload }),
  onMessage: (m) => messagesB.push(m),
  onStateChange: (s) => statesB.push(s),
});

await linkA.start();
pcA = linkA.pc;
await linkB.start();
pcB = linkB.pc;

// Process signals manually — the test is the relay.
const deliverIceFirst = signalsFromA.find(s => s.kind === "webrtc_ice");

// Offer flows A → B
for (const s of [...signalsFromA]) {
  if (s.kind === "webrtc_offer") await linkB.handleSignal("offer", s.sdp);
}
// Flush ICE after remote description set
for (const s of [...signalsFromA]) {
  if (s.kind === "webrtc_ice") await linkB.handleIce(s.candidate);
}

// Answer flows B → A
for (const s of [...signalsFromB]) {
  if (s.kind === "webrtc_answer") await linkA.handleSignal("answer", s.sdp);
}
for (const s of [...signalsFromB]) {
  if (s.kind === "webrtc_ice") await linkA.handleIce(s.candidate);
}

// ── Assertions ──

assert(signalsFromA.some(s => s.kind === "webrtc_offer" && s.sdp === "offer-sdp"),
  "offer SDP relayed A → B");
assert(signalsFromB.some(s => s.kind === "webrtc_answer" && s.sdp === "answer-sdp"),
  "answer SDP relayed B → A");

// Candidate buffering: the ice candidate from A was sent BEFORE offer arrived at B,
// so B must call addIceCandidate only AFTER setRemoteDescription.
const orderB = pcB._callOrder;
const setRemoteIdx = orderB.indexOf("setRemoteoffer");
const addIceIdx = orderB.findIndex(x => x.startsWith("addIce"));
assert(addIceIdx > setRemoteIdx && addIceIdx !== -1,
  "ICE candidate buffered until after setRemoteDescription (addIce at " + addIceIdx + " > " + setRemoteIdx + ")");

// Verify no crash: at least one candidate from each side.
assert(pcA._callOrder.some(x => x.startsWith("addIce")), "A processes ICE candidate");
assert(pcB._callOrder.some(x => x.startsWith("addIce")), "B processes ICE candidate");

// Input round-trip: A sends a game_input, verifies it hits the channel.
linkA.send({ type: "game_input", frame: 42, target: "0.555" });
assert(pcA._dcCreated.sent.length === 1, "A sent 1 message on data channel");
assert(pcA._dcCreated.sent[0].frame === 42, "sent frame 42");
assert(pcA._dcCreated.sent[0].target === "0.555", "sent target 0.555");

// Send when channel closed drops silently (no throw).
linkA._channel = null;
linkA.send({ type: "game_input", frame: 1, target: "0.1" }); // no throw

// Teardown
linkA.close();
linkB.close();
assert(statesA.at(-1) === "closed", "A closed");
assert(statesB.at(-1) === "closed", "B closed");
assert(pcA._callOrder.at(-1) === "close", "A's pc closed");
assert(pcB._callOrder.at(-1) === "close", "B's pc closed");

if (failures === 0) console.log("\nwrtc: OK");
else { console.error("\n" + failures + " test(s) FAILED"); process.exit(1); }
