# Steam Lobby P2P Roadmap

This document replaces `next.txt`; every future P2P step is concrete enough to execute independently.

## 1. Current state (replaces `next.txt`)

- **Queue stats** (elapsed, band, candidates, queue size, my MMR, leaderboard): implemented in the demo.
- **"Is P2P actually working?":** currently server-authoritative relay over WebSocket — not true P2P. This roadmap makes it real.
- **Opponent's match result:** implemented for manual-report flow via `report_received`; pong auto-resolves server-side so this works without client reporting.

## 2. TURN credential minting (lobby-server)

New `GET /internal/turn-credentials` endpoint. Returns:

```json
{
  "username": "1786091000:steam-lobby",
  "password": "<base64(hmac-sha1(secret, username))>",
  "ttl": 3600,
  "uris": ["turn:turn.john2143.com:3478?transport=udp"]
}
```

**The exact REST scheme (verified against coturn 4.17.0, 2026-08-07):**
- `username = "<unix-expiry-seconds>:<user>"` (or the timestamp alone)
- `password = base64(HMAC-SHA1(shared-secret, username))` — the secret is the *literal* value, key = the full username string
- The TURN client then does the 401 challenge dance: it receives `realm` (`turns-steam-lobby.john2143.com`) and `nonce`, and sends the authenticated Allocate with:
  - `MESSAGE-INTEGRITY = HMAC-SHA1(key, message)` where `key = MD5(username:realm:password)` and the message is the STUN message **up to (excluding) the MI attribute**, with the header length field set to the final length
  - All attributes padded to 4-byte boundaries (STUN wire format — unpadded attributes silently break parsing)
- This is exactly what `lobby-server` must not reimplement: the `iceServers` config only needs `{ urls, username, credential }` — WebRTC's `RTCPeerConnection` handles the 401/realm/nonce/MI dance natively.

Server-side gotchas (all fixed in the deployment):
- coturn's `static-auth-secret` takes a **literal secret value, NOT a file path** — a path is used as the secret itself (this silently broke the original config, which pointed at the mounted Secret file). The deployment now renders the config via a `render-auth` init container that appends `static-auth-secret=<mounted secret content>`.
- The deployment advertises the MetalLB IP via `external-ip=192.168.6.14` so relayed transports come back as `192.168.6.14:<port>` (reachable through the LB), never the pod IP.

Needs `LOBBY_TURN_SECRET` env var on the lobby-server process (read in `main.rs`, wired into `AppState`). If unset, the endpoint returns 503. When the lobby moves in-cluster, mount the `steam-lobby-turn` Secret directly.

Dependencies: ✅ `hmac = "0.12"` + `sha1 = "0.10"` + `base64 = "0.22"` (added 2026-08-07 in lobby-server/Cargo.toml). ✅ `LOBBY_TURN_SECRET` env var wired via `AppConfig`. ✅ `/internal/turn-credentials` endpoint live in `routes.rs`.
## 3. WebRTC signaling + data channel ✅ implemented (2026-08-07)

Signaling messages (`WebrtcOffer/Answer/Ice`) are live in ws.rs (server ↔ client, participant-validated relay, no echo to sender). The demo creates an `RTCPeerConnection` per match via `WrtcLink` (`web/pong-wrtc.mjs`) and fetches TURN credentials at `/internal/turn-credentials`. Per-frame `game_input` flows over the `pong` data channel with the ws relay as automatic fallback (double-feeding is idempotent). Verified: offline handshake glue test (`web/test/wrtc.mjs`, 12/12), signaling relay itest (`webrtc_signaling.rs`, 2/2), all existing regression (js-test + rollback_replica) green.

## 4. Rollback networking
**Demo client (`web/index.html`):** after both players P2P-connected:
1. `fetch("/internal/turn-credentials")` → `RTCPeerConnection({iceServers: [{urls: turnUris, username, credential}]})`
2. Create `pong` data channel (ordered, reliable)
3. First player (by convention: player_a, or the one receiving `OpponentConnected` first) creates offer; other creates answer
4. Exchange offer/answer/ICE via the existing lobby WebSocket using the new message types
5. Once data channel open, send `game_input` targets over it instead of the WebSocket relay

**MQTT option (roadmap item 6):** the existing mosquitto broker (`mosquitto-nodeport`, LB 192.168.6.19) can carry ICE candidates for lower latency than the lobby-server ws relay — offer/answer still go through the lobby-server (validates match participation), but ICE candidates go over MQTT topic `steam-lobby/{match_token}/ice/{steam_id}`. This is a performance optimization, not a requirement.

## 4. Rollback networking

**Implemented (2026-08-07) — deterministic core + server-referee sync + full test gauntlet:**

- **Deterministic core, bit-exact in two languages:** `PongGame` in Rust (`lobby-core/src/pong.rs`) and `PongSim` in JS (`web/pong-sim.mjs`). Fixed `DT_SECS = 33/1000`, IEEE-exact ops only (`+ - * / sqrt abs min max clamp ceil` — never `hypot`), a canonical 74-byte `full_state()` (9 f64 LE + 2 u8 scores) with `restore()`, and a hand-written FNV-1a 64 checksum shared by both.
- **Client rollback engine:** `web/pong-rollback.mjs` (`RollbackSession`) — per-player frame-stamped input rings, hold-last prediction, 128-frame snapshot ring, min-incorrect rollback, `restore()` resync.
- **Server referee protocol:** the game task is now frame-gated (advances only when BOTH players' inputs for the next frame are known), broadcasts `GameState { frame, checksum }`, sends `InputAck`, relays `PeerInput`, compares client `RollbackHealth` reports against its own checksum ring, and sends `RollbackResync` (74-byte hex state) on divergence. The server's sim remains the reconciliation authority — a desync can never produce a wrong winner.
- **Test gauntlet:** golden hashes (Rust + JS, identical literals), snapshot roundtrip, rollback equivalence, no-NaN/-0.0, arrival-order convergence, FNV vectors, GekkoNet-style rollback torture (`stress.mjs`), 3-replica convergence at delays 0/3/10 (`replica.mjs`), resync recovery, the all-10,000-frame Rust↔JS differential, and a true 3-way test against the real server (`lobby-server/tests/rollback_replica.rs`).

**Remaining: the WebRTC data-channel migration.** Both peers run the full sim locally, inputs travel on the data channel instead of the ws relay, and the server becomes a pure referee (no per-frame broadcast) — the client engine and protocol messages already exist and are ready to be re-pointed.

The design (deterministic core, input rings, min-incorrect rollback, confirmed frames, checksum health) mirrors GekkoNet — github.com/HeatXD/GekkoNet — the studied reference implementation.

**Checksum format:** FNV-1a 64 over the 74-byte canonical state (each f64 serialized as `to_le_bytes()` — never via `std::hash`, which is SipHash with random keys).

## 5. Ping display

**Pre-WebRTC (immediate):** new `ping` / `pong` messages over the existing ws:
- Client sends `ping { client_ts }`
- Server replies `pong { client_ts, server_ts }`
- Demo shows `now - client_ts` as "Latency: Xms"

**Post-WebRTC:** `RTCPeerConnection.getStats()` → `selectedCandidatePair.currentRoundTripTime` — the actual data-channel RTT. Render in `game-status`. Keep the ws `ping`/`pong` as fallback.

## 6. Internet play

**Current state (2026-08-07):** the MikroTik dst-nat for `3478` (comments "Coturn TURN TCP/UDP") already targets `192.168.6.14`, and the relay range `45000-45063` UDP → `192.168.6.14` (comment "steam-lobby coturn relay") is in place. The LB path needs **no node firewall changes** (kube-proxy DNATs in PREROUTING; the `nixos-fw` INPUT chain on the nodes only filters host-destined traffic).

**Only remaining step — Verizon forward:** add `45000-45063 UDP → 192.168.0.2` (3478 Both is already forwarded). Existing table forwards 50000-60000 to LiveKit — DO NOT touch that range.

**Optional TLS TURN (5349):** if the lobby server's `/internal/turn-credentials` returns `turns:` URIs, add `cert` + `pkey` paths to coturn config (remove `no-tls`), provision a cert via cert-manager, and re-point the MikroTik dst-nat rule "Coturn TURN TLS" (`5349`, currently → dormant `.6.21`) at `192.168.6.14`. DNS: `turn.john2143.com` (A record → public IP, already live).
## 7. Lobby server in-cluster (future)

`Dockerfile` already exists in the steam-lobby repo. Deploy via `workloads/steam-lobby/`:
- Postgres via CNPG operator (matching the repo's existing postgres pattern)
- HTTPRoute on `<sub>.ts.2143.me` per `argo/docs/adding-a-workload.md`
- Mount `steam-lobby-turn` Secret for credential minting
- MQTT broker `mosquitto-nodeport` (existing, LB 192.168.6.19) for ICE relay

---

## Coturn deployment (2026-08-07, verified end-to-end)

The TURN server is deployed in `argo/workloads/steam-lobby/`:
- Image: `coturn/coturn:4.17.0-alpine3.24`
- Namespace: `steam-lobby`; Deployment is **unpinned** (no nodeSelector), 1 replica, `externalTrafficPolicy: Cluster` — any node forwards to the pod, so the MetalLB IP survives pod rescheduling
- Service: LoadBalancer `192.168.6.14`, 66 declared ports: `3478` UDP+TCP and relay `45000-45063` UDP (each relay port must be a declared Service port — kube-proxy DNATs per declared port, there is no port-range concept)
- Config: `realm=turns-steam-lobby.john2143.com`, `external-ip=turn.john2143.com` (resolved at startup via getaddrinfo — no hardcoded public IP; relayed transports advertise the public IP), `use-auth-secret`, relay `min-port=45000 max-port=45063`, `no-tls`
- Auth: `render-auth` init container appends `static-auth-secret=<content of mounted steam-lobby-turn Secret>` (the option takes a literal value, not a path)
- Router: MikroTik dst-nat `3478` TCP+UDP → `192.168.6.14` (comments "Coturn TURN TCP/UDP") and `45000-45063` UDP → `192.168.6.14` (comment "steam-lobby coturn relay"); `5349` still points at the dormant matrix coturn (we are no-TLS)
- The matrix coturn (namespace `matrix`, realm `turns.john2143.com`) remains at 0 replicas — untouched

**Verified (2026-08-07):** STUN Binding OK; TURN Allocate returns 401 challenge; **authenticated REST allocation succeeds** with relayed transport `<public-ip>:<relay-port>` (advertised via `turn.john2143.com`) — server log shows `user <…:steam-lobby>: ALLOCATE processed, success`.

**Remaining for internet play:** add a Verizon forward row `45000-45063 UDP → 192.168.0.2` (3478 Both is already forwarded). LAN play needs nothing — host candidates + this STUN suffice.
