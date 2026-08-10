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

## The workflows

```
          Schedule (per P2P mode)          ┌───────────────────────────┐
          matchmaker-{mode}-{queue}  ────▶ │ PairOnceWorkflow          │
          id: pair-{mode}-{queue}-{ts}     │
          (single writer)                  │ one pair_matches activity │
                                          └──────────┬────────────────┘
                                                     │ creates the match
                                                     ▼
┌────────────────────────────────────────┐   ┌──────────────────────┐
│  UserSessionWorkflow                   │   │ P2PMatchWorkflow     │
│  id: user-session-{player_id}-          │   │ id: match-{token}    │
│       {session_id}  (per connection)   │   │ accept → start →     │
│  queue/unqueue/match_found/            │   │ report (timers)      │
│  queue_expired/disconnect signals      │   └──────────┬───────────┘
│  ends on disconnect or 24h TTL         │              │
└────────────────────────────────────────┘              │
```

- **`UserSessionWorkflow`** — one per **WS connection** (session UUID in the
  workflow ID). On start it `sync_session`-recovers the player's queue entry
  from the DB (reconnect-while-queued), then lives on signals:
  `queue`/`unqueue`/`match_found`/`queue_expired`/`disconnect`. It ends when
  the `disconnect` signal arrives **or** the 24h TTL fires — there is no flow
  that runs forever.
- **`PairOnceWorkflow`** — the Schedule's per-tick pairing run: one
  `pair_matches` activity, then returns. A 2s **Schedule** per P2P mode fires
  it (`ScheduleOverlapPolicy::Skip` = single writer: the server appends a
  timestamp to each scheduled workflow ID — the task-queue suffix keeps
  parallel workers' runs distinct, and Skip — not the ID — prevents concurrent
  pairing runs). Pairing itself is one transaction
  (`PostgresStore::pair_next_match`, `FOR UPDATE`): scan the
  queue, MMR-band pair, delete both rows + insert the match + `Paired` event
  atomically.
- **`P2PMatchWorkflow`** — the coordinator and **sole lifecycle writer** for a
  match. Phases, each a workflow timer raced against client signals:
  1. **Accept window** (30s, `MATCH_ACCEPT_TIMEOUT_S`): both
     `match_choice{accept:true}` → `mark_accepts` (InProgress + `match_started`
     broadcast); any decline → `handle_decline` (Disputed + `match_declined`
     to both, recording the **real decliner**); timeout → `handle_decline`
     with no actor.
  2. **START window** (15s, `LOBBY_START_TIMEOUT_SECS`): both `start` signals
     → `mark_connected` (Reporting + `opponent_connected` + spawn the pong
     referee for playback); timeout → `resolve_start_forfeit` (starter wins,
     or double loss if neither started).
  3. **Report window** (300s, `REPORT_TIMEOUT_S`): both `who_won` +
     `submit_demo` → agree → `finish_match` (ratings + `match_results` +
     `game_over`/`match_result` broadcasts); disagree or timeout →
     `resolve_dispute` (Disputed).

**Design rule: no workflow loops, no unending flows.** Pairing is a Schedule
of short-lived `PairOnceWorkflow` runs (the old `MatchmakerWorkflow`'s
`loop {}` is gone — it grew ~345k history events/day, past Temporal's 50k
limit). Every workflow terminates on all paths. The queue itself is the
`matchmaking_queue` DB row, not a workflow: the per-player lifecycle is the
session's signals, and the ticker's stale-entry sweep runs **out of Temporal**
and notifies the session via the `queue_expired` signal (so a queue-expired
player can re-queue).

**The schedule pauses when idle.** After each run, `pair_matches` pauses the
mode's schedule if fewer than two players remain in the queue (no pair
possible), and `enter_queue` unpauses it when a player enqueues — so an idle
server creates ~zero workflows instead of a `PairOnceWorkflow` every 2s. The
in-process ticker re-checks every 2s and unpauses if ≥2 players are queued
(safety net for a lost resume). A side benefit: when the worker is down the
schedule is usually paused, so no pending-task pileup during a restart.

`accept_match`, `mark_connected` (also spawns the playback referee),
`mark_accepts`, `handle_decline`, `finish_match`, `resolve_dispute`,
`resolve_start_forfeit`, `pair_matches` (one `pair_next_match` transaction),
`sync_session` (state + queue-entry recovery for session start),
`enter_queue` (also refreshes heartbeat liveness), `leave_queue`,
`set_player_state`, `verify_match`.

## Signals (WS handler / ticker → workflow)

The WS handlers (`ws.rs`) never call `MatchManager` directly; they signal
workflows via `state.temporal` (`lobby-server/src/temporal/signals.rs`):
`start_user_session` (auth, per connection), `queue`/`unqueue` (matchmaking,
scoped to the connection's session), `match_choice` (accept/decline), `start`,
`who_won` + `submit_demo` (report), `disconnect`. The ticker's out-of-Temporal
stale-entry sweep signals `queue_expired` to the session. If Temporal is down
(`state.temporal` is `None`), the helpers no-op — the server is considered
unavailable for matchmaking (there is no in-process fallback; the cutover
deleted it).

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
