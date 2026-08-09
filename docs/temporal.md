# Temporal architecture

The p2p match lifecycle is orchestrated by Temporal workflows running on an
**in-process Rust worker** inside the lobby-server binary (same Deployment —
no separate worker pod, no WorkerDeployment CRDs). The SDK is
`temporalio-sdk =0.6.0` (Public Preview; pinned exactly — every release breaks
the API).

## Why Temporal here

Crash recovery. The old in-process timers (accept expiry, START-window forfeit,
report window) lived in tokio tasks: when the server restarted mid-match, a
match could sit in `InProgress`/`Reporting` forever. Workflow timers are
durable — a killed server resumes the match from its last checkpoint, and every
lifecycle timer is inspectable in the Temporal UI (`localhost:8233` locally).

## The four workflows

```
                    ┌──────────────────────────────────────────┐
                    │  UserSessionWorkflow                     │
                    │  id: user-session-{steam_id}-{queue}     │
                    │  lives until the disconnect signal       │
                    └───────┬──────────────┬───────────────────┘
                    queue/  │              │ match_found /
                    unqueue │              │ match_complete
                            ▼              ▼
              ┌─────────────────┐   ┌──────────────────────┐
              │ QueueWorkflow   │   │ P2PMatchWorkflow     │
              │ id: queue-{id}- │   │ id: match-{token}    │
              │      {mode}     │   │ accept → start →     │
              │ enter_queue +   │   │ report (timers)      │
              │ wait for cancel │   └──────────┬───────────┘
              └─────────────────┘              │
                              ┌────────────────┴─────────┐
                              │ MatchmakerWorkflow        │
                              │ id: matchmaker-{mode}-    │
                              │      {queue}              │
                              │ every 2s: pair_matches →  │
                              │ start P2PMatchWorkflow    │
                              └──────────────────────────┘
```

- **`UserSessionWorkflow`** — one per logged-in player. Holds the player's
  queue/match state; ends on the `disconnect` signal (cancelling a pending
  queue child).
- **`QueueWorkflow`** (child of a session) — pure queueing state holder: runs
  `enter_queue` on start, waits for cancellation. The MatchmakerWorkflow does
  the actual pairing.
- **`MatchmakerWorkflow`** — one per **P2P** game mode, started at server boot.
  Every 2s runs the `pair_matches` activity: MMR-band pairing (the
  `lobby_core::queue` logic), `create_match` + `Paired` event, MatchFound
  broadcast, session `match_found` signals, then starts the P2P workflow.
- **`P2PMatchWorkflow`** — the coordinator and **sole lifecycle writer** for a
  match. Phases, each a workflow timer raced against client signals:
  1. **Accept window** (30s, `MATCH_ACCEPT_TIMEOUT_S`): both
     `match_choice{accept:true}` → `mark_accepts` (InProgress + `match_started`
     broadcast); any decline or timeout → `handle_decline` (Disputed +
     `match_declined` to both).
  2. **START window** (15s, `LOBBY_START_TIMEOUT_SECS`): both `start` signals
     → `mark_connected` (Reporting + `opponent_connected` + spawn the pong
     referee for playback); timeout → `resolve_start_forfeit` (starter wins,
     or double loss if neither started).
  3. **Report window** (300s, `REPORT_TIMEOUT_S`): both `who_won` +
     `submit_demo` → agree → `finish_match` (ratings + `match_results` +
     `game_over`/`match_result` broadcasts); disagree or timeout →
     `resolve_dispute` (Disputed).

## Activities

`lobby-server/src/temporal/activities.rs` — `LobbyActivities { state:
Arc<AppState> }`, the only place DB + broadcast work happens (workflow code
itself is pure and deterministic):

`accept_match`, `mark_connected` (also spawns the playback referee),
`mark_accepts`, `handle_decline`, `finish_match`, `resolve_dispute`,
`resolve_start_forfeit`, `pair_matches`, `enter_queue` (also refreshes
heartbeat liveness), `leave_queue`, `set_player_state`, `verify_match`.

## Signals (WS handler → workflow)

The WS handlers (`ws.rs`) never call `MatchManager` directly; they signal
workflows via `state.temporal` (`lobby-server/src/temporal/signals.rs`):
`start_user_session` (auth), `queue`/`unqueue` (matchmaking),
`match_choice` (accept/decline), `start`, `who_won` + `submit_demo` (report),
`disconnect`. If Temporal is down (`state.temporal` is `None`), the helpers
no-op — the server is considered unavailable for matchmaking (there is no
in-process fallback; the cutover deleted it).

## The referee is playback-only

The server-authoritative pong referee (`pong.rs::spawn_game`) stays in-process
as a transitional artifact: it renders frames/checksums and holds the 3-2-1
round countdown so the demo still plays. Its **resolve path is disabled** when
Temporal is up — at game end it logs `pong ended — awaiting workflow finish`
and exits; the workflow resolves on the clients' `who_won` reports (or the
report-window dispute timer). It dies when the WebRTC/rollback roadmap lands.

## server_arena is out of scope

Server-authoritative matches (`GameType::Server`) keep their in-process path:
the ticker still pairs them, the gameserver allocation/expiry machinery is
untouched, and the AcceptMatch handler routes server matches to the in-process
`accept_match` (they have no START phase and resolve via the gameserver
webhook).

## Config

| Env | Default | Meaning |
|-----|---------|---------|
| `TEMPORAL_ADDRESS` | `http://localhost:7233` | Temporal frontend (plaintext gRPC; cluster: `temporal-frontend.default.svc.cluster.local:7233`) |
| `TEMPORAL_NAMESPACE` | `pvp` | Namespace the worker registers workflows/activities on |
| `TEMPORAL_TASK_QUEUE` | `lobby` | Task queue |
| `MATCH_ACCEPT_TIMEOUT_S` | `30` | Accept window (workflow timer) |
| `LOBBY_START_TIMEOUT_SECS` | `15` | START window (workflow timer) |
| `REPORT_TIMEOUT_S` | `300` | Report window (workflow timer) |

## Local run

```bash
just temporal-up   # podman play kube deploy/temporal.yaml (postgres + auto-setup + UI)
just db-up
just run           # starts the in-process worker; UI at http://localhost:8233
```

The worker logs `temporal worker started on task queue lobby (namespace pvp)`
on boot; if Temporal is unreachable it logs `temporal unavailable` and exits.

## Cluster connection story

The lobby Deployment (argo `workloads/steam-lobby/deployment.yaml`) sets
`TEMPORAL_ADDRESS=temporal-frontend.default.svc.cluster.local:7233`,
`TEMPORAL_NAMESPACE=pvp`, `TEMPORAL_TASK_QUEUE=lobby`. The worker connects as a
non-mesh pod in `steam-lobby`; the cluster Temporal frontend (bare
LoadBalancer) accepts in-cluster plaintext. The `pvp` namespace must be
registered on the cluster Temporal before rollout:

```
tctl --address temporal-frontend.default.svc.cluster.local:7233 namespace register pvp
```

If the smoke test shows the frontend's Linkerd mesh policy (`default-inbound-policy:
all-authenticated`) refusing the plaintext connection, add the steam-lobby
service account to `argo/workloads/temporal/server-authz.yaml` (option (b) in
the migration plan).

## Tests

Integration tests run through the workflows: `setup_temporal*` harnesses in
`lobby-server/tests/common.rs` build the AppState, start an in-process worker
on a **unique per-test task queue** (so parallel tests never collide on
workflow IDs), and shut the worker down at harness drop (the SDK's
`Worker::shutdown_handle`). Run with `--test-threads 4` — the default (all
cores) exhausts Postgres' connection limit.

`lobby-server/tests/live_e2e.rs` (`#[ignore]`d) is a live-server smoke test:
connect two clients to a running dev server and drive
queue → match_found → accept → start → report against the workflows.
