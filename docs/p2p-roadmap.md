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
  "username": "1723000000:steam-lobby",
  "password": "<base64(hmac-sha1(secret, username))>",
  "ttl": 86400,
  "uris": ["turn:192.168.5.68:3478?transport=udp"]
}
```

coturn `use-auth-secret` validates this scheme exactly. The HMAC key is the `turn_secret` from the k8s Secret `steam-lobby-turn`. Needs `LOBBY_TURN_SECRET` env var on the lobby-server process (read in `main.rs`, wired into `AppState`). If the env var is absent, the endpoint returns 503.

Dependencies: `hmac` + `sha1` + `base64` crates in `lobby-server/Cargo.toml` (check if available; otherwise add `hmac = "0.12"` and `sha1 = "0.10"`; no existing HMAC dep in the workspace).

## 3. WebRTC signaling + data channel

**New message types** in `lobby-server/src/ws.rs` (`ClientMessage` / `ServerMessage` — use snake_case variants matching existing `lobby_match_*` / `game_input` style):

| Direction | Variant | Fields |
|-----------|---------|--------|
| client→server | `webrtc_offer` | `match_token: String`, `sdp: String` |
| client→server | `webrtc_answer` | `match_token: String`, `sdp: String` |
| client→server | `webrtc_ice` | `match_token: String`, `candidate: String` |
| server→client | `webrtc_offer` | `match_token: String`, `from: String`, `sdp: String` |
| server→client | `webrtc_answer` | `match_token: String`, `from: String`, `sdp: String` |
| server→client | `webrtc_ice` | `match_token: String`, `from: String`, `candidate: String` |

**Server behavior:** validate sender is a participant of a `Reporting`/`InProgress` p2p match, then relay to the opponent only. No SDP inspection. If the match is resolved, drop silently.

**Demo client (`web/index.html`):** after both players P2P-connected:
1. `fetch("/internal/turn-credentials")` → `RTCPeerConnection({iceServers: [{urls: turnUris, username, credential}]})`
2. Create `pong` data channel (ordered, reliable)
3. First player (by convention: player_a, or the one receiving `OpponentConnected` first) creates offer; other creates answer
4. Exchange offer/answer/ICE via the existing lobby WebSocket using the new message types
5. Once data channel open, send `game_input` targets over it instead of the WebSocket relay

**MQTT option (roadmap item 6):** the existing mosquitto broker (`mosquitto-nodeport`, LB 192.168.6.19) can carry ICE candidates for lower latency than the lobby-server ws relay — offer/answer still go through the lobby-server (validates match participation), but ICE candidates go over MQTT topic `steam-lobby/{match_token}/ice/{steam_id}`. This is a performance optimization, not a requirement.

## 4. Rollback networking

The deterministic `PongGame` (fixed 33ms tick, velocity normalization on `sqrt` — keep the current implementation, never switch to `hypot`) is the rollback foundation.

**Both peers run the full sim locally.** Each input carries `(seq, target)` on the data channel. Each peer:
1. Predicts ahead with its own inputs immediately
2. On receiving the opponent's input for an older tick, rolls back to the last common acknowledged state and replays queued inputs
3. Every 64 ticks, sends a state checksum (FNV-1a of snapshot fields `[ball_x, ball_y, ball_vx, ball_vy, paddle_positions]`); both sides log/report desync

**Server arbitration stays:** `MatchManager::resolve_pong` + disconnect forfeit are unchanged. The server's authoritative sim remains a reconciliation authority — if peers diverge, the server can force a full-state sync.

**CRC32 or FNV-1a:** FNV-1a is simple, no-alloc, and already possible with `std::hash::Hasher` on a u64. For the deterministic snapshot, hash each field as `f64::to_bits()` to avoid NaN/float-cmp issues.

## 5. Ping display

**Pre-WebRTC (immediate):** new `ping` / `pong` messages over the existing ws:
- Client sends `ping { client_ts }`
- Server replies `pong { client_ts, server_ts }`
- Demo shows `now - client_ts` as "Latency: Xms"

**Post-WebRTC:** `RTCPeerConnection.getStats()` → `selectedCandidatePair.currentRoundTripTime` — the actual data-channel RTT. Render in `game-status`. Keep the ws `ping`/`pong` as fallback.

## 6. Internet play

**MikroTik dst-nat** (manual, outside repo):
- WAN UDP 3478 → 192.168.5.68:3478 (STUN/TURN allocation)
- WAN UDP 45000-49999 → 192.168.5.68:45000-49999 (TURN relay range)
- Add matching Verizon Fios forward rows (existing table forwards 50000-60000 to LiveKit — DO NOT touch that range)

**Optional TLS TURN (5349):** if the lobby server's `/internal/turn-credentials` returns `turns:` URIs, add `cert` + `pkey` paths to coturn config (remove `no-tls`) and provision a cert via cert-manager. DNS: `turns-steam-lobby.john2143.com` → MikroTik → 192.168.6.14 (or node IP).

**Node firewall:** the k3s node nftables on `big` currently blocks UDP traffic outside established/related + flannel VXLAN. The NodePort range (30000-32767) allows TCP only. Internet play needs one of:
- Runtime `iptables -I nixos-fw -p udp --dport 3478 -j nixos-fw-accept` on each relevant node, OR
- A NixOS `networking.firewall.allowedUDPPorts = [ 3478 45000 45100 ... 49999 ]` on the k3s nodes' NixOS config, OR
- MetalLB with BGP properly routing to the workstation LAN so the LB IP reachable path bypasses node INPUT

## 7. Lobby server in-cluster (future)

`Dockerfile` already exists in the steam-lobby repo. Deploy via `workloads/steam-lobby/`:
- Postgres via CNPG operator (matching the repo's existing postgres pattern)
- HTTPRoute on `<sub>.ts.2143.me` per `argo/docs/adding-a-workload.md`
- Mount `steam-lobby-turn` Secret for credential minting
- MQTT broker `mosquitto-nodeport` (existing, LB 192.168.6.19) for ICE relay

---

## Coturn deployment (2026-08-07)

The TURN server is deployed in `argo/workloads/steam-lobby/`:
- Image: `coturn/coturn:4.17.0-alpine3.24`
- Namespace: `steam-lobby`
- Service: LoadBalancer `192.168.6.14:3478` (STUN/TURN allocation)
- Relay range: 45000-49999 (configured in coturn, but reachability depends on firewall — see section 6)
- Auth: HMAC-SHA1 shared-secret (`steam-lobby-turn` Secret, key `turn_secret`)
- The matrix coturn (namespace `matrix`, realm `turns.john2143.com`) remains at 0 replicas — untouched

**Status (2026-08-07):** STUN binding verified working end-to-end. A raw STUN Binding Request (`00 01 00 00 21 12 A4 42` + 12-byte txid) sent to `192.168.6.14:3478/UDP` from the office LAN returns a Binding Success. The pod IP, LoadBalancer IP, and NodePort paths all respond. The earlier "no STUN response" failures were caused by a malformed test probe (an extra 4 zero bytes shifted the magic cookie), not by coturn or the network.

**Note on the node firewall:** the k3s node nftables (`nixos-fw` on `big`) allows TCP NodePorts (30000-32767) but drops UDP NodePorts; a runtime rule `iptables -I nixos-fw 1 -p udp --dport 30000:32767 -j nixos-fw-accept` was added (2026-08-07) so the NodePort path works today. The LoadBalancer path bypasses `nixos-fw` entirely (DNAT in PREROUTING → FORWARD chain is ACCEPT) and needs no firewall change. If NodePort UDP reachability must survive a node reboot, add `networking.firewall.allowedUDPPorts = [ "30000:32767" ]` (or 3478 + the relay range) to the k3s nodes' NixOS config.
