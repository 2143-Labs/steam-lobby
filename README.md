# Steam Lobby

A matchmaking and MMR service for Steam games, built with Axum + PostgreSQL.

## Prerequisites

- **Docker** — for PostgreSQL (`docker compose` or just Docker Engine; `just
  db-up` falls back to a plain `docker run` when the compose plugin is missing)
- **Rust toolchain + just** — via `nix-shell shell.nix` if you have Nix,
  otherwise install `rustup` and `cargo install just` yourself

No system OpenSSL is required anywhere — all TLS is pure-Rust (rustls).
`just` uses the pinned `shell.nix` toolchain when Nix is installed and plain
`cargo` otherwise, so the same commands work on any platform.

### Non-NixOS / non-Nix

Install Rust (`rustup`) and `just` (`cargo install just`). That's it — no
OpenSSL, no Nix needed.


## Quickstart (NixOS)

```bash
cp .env.example .env
openssl rand -base64 48   # paste the output into JWT_SECRET in .env
nix-shell shell.nix       # enter dev shell (provides cargo, rustc, just)
just db-up                # start PostgreSQL in Docker
just temporal-up          # start the local Temporal stack (podman) on :7233 / UI :8233
just test                 # run unit tests
just itest                # run integration tests against Postgres (needs db-up + temporal-up)
just run                  # start server on :8080
```

The match lifecycle is orchestrated by Temporal workflows (see "Temporal
Architecture" below): `just run` starts an in-process worker that connects to
the local Temporal at `localhost:7233`. Matchmaking requires `just temporal-up`
to be running — without it the server still boots but queueing does nothing.
Every lifecycle timer (accept window, START window, report window) is visible
as a workflow timer in the Temporal UI at `http://localhost:8233`.

To emulate multiple users without Steam, set `AUTH_DEV_MODE=true` in `.env`
(it must be `false` in production), then open `http://localhost:8080/` in two
browser tabs (the server serves the demo page itself; `web/index.html` in the
repo is the same file). Give each tab a distinct steam ID, connect both, and
start matchmaking in each — the demo talks to the dev-only `/auth/test-token`
endpoint, no real Steam API calls involved.

For a **genuine** Steam login, click the "Sign in through Steam" button in the
demo instead. It redirects to `steamcommunity.com/openid/login` and back with
a `#token=` session — no Steam API key needed (display names then read
"Unknown" unless `STEAM_API_KEY` is set). In dev, point `PUBLIC_URL` at a
public origin that reaches your server (reverse proxy or tunnel) and browse
the demo through that origin — Steam OpenID requires an absolute callback URL.

## Quickstart (non-NixOS)

Works on any Linux, macOS, or WSL2 — and on Windows via WSL2 or Git Bash.
Install [rustup](https://rustup.rs) and Docker (Docker Desktop on
macOS/Windows). The `just` commands are the same as the NixOS quickstart:

```bash
cp .env.example .env
openssl rand -base64 48   # paste the output into JWT_SECRET in .env
cargo install just
just db-up
just test
just run
```

Native Windows (PowerShell/cmd) is not covered by the `just` recipes — they
need a POSIX shell. Use WSL2 or Git Bash, or run the server directly:
`cargo run -p lobby-server` after exporting the variables from `.env`.

## API Endpoints

| Method | Path | Description |
| GET | `/` | The browser demo page (same as `web/index.html`) |
| GET | `/health` | Health check, returns `"ok"` |
| GET | `/auth/steam/login` | Steam OpenID login redirect |
| GET | `/auth/steam/callback` | Steam OpenID callback |
| GET | `/ws` | WebSocket upgrade (game protocol) |
| POST | `/auth/ticket` | Steam ticket auth -> JWT (rate-limited 10/min per IP) |
| POST | `/auth/test-token` | Dev-only: JWT for any steam_id (only when `AUTH_DEV_MODE=true`) |
| POST | `/auth/logout` | Revoke the current session token (all earlier tokens die) |
| GET | `/modes` | Configured matchmaking modes + their game types (the demo dropdown) |
| GET | `/auth/config` | Auth surface capabilities `{ steam_login, dev_mode }` — the demo gates its login UI on this |
| POST | `/internal/game-result/{token}/{secret}` | Gameserver result webhook; the URL itself is the auth |

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://lobby:lobby@localhost:5432/lobby` | PostgreSQL connection string |
| `JWT_SECRET` | — | Required, ≥32 random bytes — signs session JWTs (compose fails fast without it) |
| `STEAM_API_KEY` | — | Steam Web API key; empty = OpenID works, ticket auth disabled |
| `STEAM_APP_ID` | `480` | Steam App ID for the game |
| `AUTH_DEV_MODE` | `false` | `true` enables the unauthenticated `/auth/test-token` dev endpoint — never in production |
| `JWT_TTL_S` | `86400` | Seconds a session JWT stays valid |
| `CORS_ORIGINS` | — | Comma-separated browser origins allowed to call the API (the web demo origin) |
| `MATCH_ACCEPT_TIMEOUT_S` | `30` | Seconds players have to accept a match |
| `REPORT_TIMEOUT_S` | `300` | Seconds after match ends to report outcome |
| `LOBBY_PAIR_COOLDOWN_S` | `300` | Seconds before the same two accounts can re-pair after a match; set `0` for the demo to rematch instantly |
| `RUST_LOG` | `info,lobby_server=debug` | Tracing log level |
| `LOBBY_HOST` | `0.0.0.0` | Bind address |
| `LOBBY_PORT` | `8080` | Bind port |
| `PUBLIC_URL` | — | Required for Steam OpenID login (absolute public origin); login returns `400` without it |
| `GAME_MODES` | `ranked_1v1:p2p,server_arena:server` | Comma-separated `mode:type` pairs; `type` = `p2p` \| `server` |
| `GAMESERVER_CREATOR_URL` | — | Absolute URL of the gameserver creator's allocate endpoint (required for server modes) |
| `GAMESERVER_ALLOC_TIMEOUT_S` | `60` | Seconds before an unallocated server match is disputed |
| `GAMESERVER_RESULT_TIMEOUT_S` | `300` | Seconds after the server is ready before a missing result is disputed |

> **Production:** `JWT_SECRET` is required (≥32 random bytes) and `AUTH_DEV_MODE`
> must stay `false`. Set `STEAM_API_KEY` to a real [Steam Web API
> key](https://steamcommunity.com/dev/apikey) only if you need ticket auth; the
> OpenID browser login needs no key at all.


### How to tell which auth mode the server is in

The server logs the active mode at startup:

```
INFO lobby_server: auth mode: TEST  — /auth/test-token enabled
INFO lobby_server: auth mode: STEAM — ticket + OpenID verification against Steam (appid 480)
```

Or probe `GET /auth/config` — it returns `{ "steam_login": …, "dev_mode": … }`
(`steam_login` = `PUBLIC_URL` is set, so the Steam button is offered; `dev_mode`
= `/auth/test-token` is exposed). `POST /auth/test-token` returns a JWT when
`AUTH_DEV_MODE=true` and `404` otherwise. The web demo (`web/index.html`) fetches
`/auth/config` on load and shows only the login surfaces the server actually
offers: the Steam button, the dev steam-ID field, or both. Offline (e.g. opened
from `file://`) it shows both so the dev flow still works. Its Connect button
uses the test-token endpoint (or a `#token=` fragment from a Steam login, if
present); when dev mode is off and no fragment token exists, Connect shows an
error — only genuine Steam logins work in production.

## WebSocket Quick Test

With `AUTH_DEV_MODE=true`, the dev-only token endpoint is enabled. Get a JWT, then
send it in the **first WebSocket frame** — the server reads the session token
from the opening `auth` message, not from an HTTP header:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/auth/test-token \
  -H 'Content-Type: application/json' \
  -d '{"steam_id": 12345}' | jq -r '.token')
wscat -c "ws://localhost:8080/ws"
# first message, once connected:
# {"type":"auth","session_token":"<paste TOKEN>"}
```

The Rust reference client (`lobby-client`) has a helper for this flow:
`LobbyClient::authenticate_test_token(steam_id, base_url)` — POSTs
`/auth/test-token` and authenticates over WebSocket in one call. The
integration tests in `lobby-server/tests/` use it to emulate players.

Then send: `{"type":"begin_matchmaking","mode":"ranked_1v1","difficulty":"normal"}`

The token is only ever sent in the first WS `auth` frame (or delivered as a URL
**fragment** — `#token=…` — after Steam OpenID login), never as a query
parameter.


## Steam Ticket Auth (production)

In production (`STEAM_API_KEY` = a real key) clients authenticate with a Steam
ticket instead of the dev test-token. The flow follows the Steamworks Web API
docs, `GetAuthTicketForWebApi` → `AuthenticateUserTicket`:

1. The client (game) mints a ticket with the Steamworks SDK:
   `ISteamUser::GetAuthTicketForWebApi("matchmaking")` — **not**
   `GetAuthSessionTicket`, which Steam's docs explicitly disallow for this Web
   API. The identity string must be exactly `matchmaking`; Steam only
   authenticates tickets created with that parameter, and the server verifies
   with `identity=matchmaking` (`lobby-server/src/steam_auth.rs`).
2. Wait for `GetTicketForWebApiResponse_t`, hex-encode the binary ticket, and
   send `{"type":"auth_ticket","ticket":"<hex>"}` over the WebSocket (or
   `POST /auth/ticket` with the same JSON body). Tickets that are not hex (or
   longer than 8192 chars) are rejected locally before any Steam API call, and
   both ticket paths are rate-limited per IP (10/min).
3. `STEAM_APP_ID` must be the app that minted the ticket — the game's real
   appid. The default `480` (Spacewar) only works for testing.
OpenID browser login requires `PUBLIC_URL` set to the public origin (e.g.
`https://lobby.example.com`) — the login endpoint answers `400` without it.
The demo's "Sign in through Steam" button (Valve's official asset, embedded as
a data URI so the page stays CSP-clean and offline-capable) hits
`GET /auth/steam/login?return_to=/`. Each login gets a one-time random `state`,
bound to the exact `return_to`; replaying a callback, or presenting a
`return_to` that does not match the issued state, is rejected. The session JWT
is delivered as a URL **fragment** (`#token=…`), never as a query parameter.
Development can exercise genuine logins too: set `PUBLIC_URL` to a public
origin that reaches the dev server (reverse proxy/tunnel), keep
`AUTH_DEV_MODE=true` if you also want the test-token field, and browse the demo
via that origin.

Transport note: the server is plain HTTP by design — terminate TLS at a reverse
proxy (nginx/Caddy) in front of it. Without TLS the JWT and tickets travel in
cleartext.

**Security notes:** `JWT_SECRET` is required and must be ≥32 random bytes
(`docker compose` fails fast without it, never falling back to a known value).
The Postgres port is not published outside the compose network, and the
container runs as a non-root user (`app`) with all capabilities dropped. TLS
must terminate at a reverse proxy in front of the server — JWTs and tickets
otherwise travel cleartext. The web demo requires `AUTH_DEV_MODE=true` and its
origin listed in `CORS_ORIGINS`; production deployments must keep
`AUTH_DEV_MODE=false`.


## Temporal Architecture

The p2p match lifecycle is orchestrated by Temporal workflows running on an
**in-process Rust worker** inside the lobby-server binary (same Deployment,
no separate worker pod). The worker connects to the Temporal frontend at
`TEMPORAL_ADDRESS` (local: `just temporal-up`; cluster:
`temporal-frontend.default.svc.cluster.local:7233`) in namespace
`TEMPORAL_NAMESPACE` (default `pvp`), task queue `lobby`.

| Workflow / Schedule | ID | Job |
|----------|----|-----|
| **Schedule** (per P2P mode) | `matchmaker-{mode}-{queue}` | Fires a short `PairOnceWorkflow` every 2s (`ScheduleOverlapPolicy::Skip` = single writer); deleted on worker shutdown so tests don't accumulate schedules. |
| `PairOnceWorkflow` | `pair-{mode}-{queue}-{timestamp}` | One `pair_matches` activity (a single `FOR UPDATE` transaction: MMR-band pair, create match, signal both sessions, start the P2P workflow), then returns. Replaces the old infinite `MatchmakerWorkflow` loop. |
| `UserSessionWorkflow` | `user-session-{steam_id}-{session_id}` | Per **WS connection** (session UUID). Recovers queue/match state from the DB on start; driven by `queue`/`unqueue`/`match_found`/`queue_expired`/`disconnect` signals; ends on disconnect or the 24h TTL. |
| `P2PMatchWorkflow` | `match-{match_token}` | The coordinator: accept window (30s) → START window (15s, forfeit) → report window (300s) — each a workflow timer racing the clients' signals. Sole lifecycle writer. |

The queue is the `matchmaking_queue` DB row — there is **no queue workflow**.
Every workflow terminates on all paths (no loops, no unending flows).

The WS handlers only **signal** workflows (`queue`, `unqueue`, `match_choice`,
`start`, `who_won`, `submit_demo`, `disconnect`); the ticker's stale-queue
sweep (out of Temporal) signals `queue_expired`. All DB + broadcast work
happens in activities (`lobby-server/src/temporal/activities.rs`). The
server-authoritative pong referee stays in-process for **playback only**
(frames/checksums/round-hold); it no longer resolves matches — the workflow
resolves on the clients' `who_won` reports, the START-window forfeit, or the
report-window dispute timer. `server_arena` gameserver matches are out of
scope for the migration and keep their in-process path.


See `docs/temporal.md` for the full design.

## How It Works

Players connect over WebSocket and authenticate with a JWT (obtained via Steam OpenID or the dev token endpoint). Once authenticated, a player enters the queue with a difficulty (Easy/Normal/Hard) at their current MMR.

When a match is found, both players receive a `MatchFound` message. Both must accept before the match transitions to `InProgress`. After the match, each player submits a report (winner + optional demo hash). If both agree, ratings update via the Weng-Lin algorithm (a modern Bayesian system with uncertainty tracking). If they disagree or demo hashes mismatch, the match is disputed.

Every pairing, accept, and decline is appended to the `match_events` audit table (`event_type` `paired`/`accepted`/`declined`, actor `steam_id`, timestamp), and a declined match is immediately marked `Disputed`.

## Codebase Map

| Crate | File | Responsibility |
|-------|------|----------------|
| lobby-core | `types.rs` | Wire types + Steam-ID serde helpers |
| lobby-core | `traits.rs` | Storage/callback traits |
| lobby-core | `player.rs` | `PlayerManager` state machine |
| lobby-core | `queue.rs` | Matchmaking + expanding search band |
| lobby-core | `match_lifecycle.rs` / `match_expiry.rs` | MatchManager player actions / expiry |
| lobby-core | `mmr.rs` | Weng-Lin rating math |
| lobby-core | `error.rs` | `LobbyError` + `Result` |
| lobby-server | `steam_auth.rs` | Steam ticket/OpenID auth + JWT (claims: `sub` = account UUID, `sid` = SteamID64) |
| lobby-server | `db/players.rs` | `PlayerStore` impl + `find_or_create_user` (find-or-create identity attach) |
| lobby-server | `migrations/` | Schema; `users.id` (UUID) is the provider-agnostic account key, `user_identities` maps `(provider, provider_uid)` → account |
| lobby-server | `db/` | Other `PostgresStore` impls (one file per store trait) |
| lobby-server | `state.rs` | `AppState` composition root |
| lobby-server | `ws.rs` | WebSocket protocol |
| lobby-server | `ticker.rs` | 2s maintenance loop (7 phases) |
| lobby-server | `routes.rs` | HTTP endpoints |
| lobby-server | `gameserver.rs` | Gameserver creator client |
| lobby-server | `rate_limit.rs` | Rate limiter |
| lobby-client | `lib.rs` | WS client + `ServerEvent` types (demo + integration tests) |
| tests | `lobby-core/tests/` | Common mocks + lifecycle/player/rating suites |
| tests | `lobby-server/tests/` | Integration suite + common harness |


## Adding another login provider (Discord / au.2143.me — blueprint)

Steam login is implemented directly (OpenID 2.0 against `steamcommunity.com`).
Discord, au.2143.me (Pocket ID OIDC), and any future provider follow the
registry pattern proven by john2143.com — a declarative provider registry plus
generic login/callback dispatch, **not** per-provider route copies. Nothing in
the schema or JWT changes when a second provider lands: the JWT `sub` is
already the abstract `users.id`, and `user_identities` maps
`(provider, provider_uid)` → account.

**Provider config** (new `lobby-server/src/auth_providers.rs`; mirror
`john2143.com/src/auth/providers.ts`):

```rust
struct Provider {
    id: String,                                   // "discord", "au2143", …
    kind: ProviderKind,                           // openid2 | oauth2 | oidc
    authorization_endpoint: Option<String>,       // oauth2/oidc
    issuer: Option<String>,                       // oidc: discovery well-known
    token_endpoint: Option<String>,
    userinfo_endpoint: Option<String>,
    client_id: String,
    client_secret: String,
    scopes: Vec<String>,
    id_field: String,                             // claim holding provider_uid
    map_user: fn(userinfo) -> (String, String),   // (provider_uid, display_name)
}
```

Steam is `kind: openid2` (no token endpoint or code; the existing
`openid_redirect_url`/`verify_openid`). Discord is `oauth2`: `identify` scope,
`state` for CSRF, token exchange at `discord.com/api/oauth2/token`
(form-urlencoded, Basic auth), identity from `GET /users/@me`, `id_field: "id"`.
Pocket ID is `oidc`: discovery at `https://au.2143.me/.well-known/openid-configuration`,
scopes `openid profile email groups`, `id_field: "sub"`.

**Generic routes:** `GET /auth/{provider}/login` issues a one-time `state` (and,
for `oauth2`/`oidc`, a PKCE S256 `code_verifier` stored beside it);
`GET /auth/{provider}/callback` consumes the state, exchanges/verifies, then
calls `find_or_create_user` generalized to `(provider, provider_uid,
display_name, verified)` and mints the same JWT. The `UNIQUE (user_id, provider)`
constraint enforces one identity per provider per account.

**State storage:** keep the in-memory `openid_states` map (600s TTL, 4096 cap)
while the server is single-instance; move state to a DB table with a TTL index
if multi-instance deployment ever happens.

**Account linking:** a signed-in user clicks "Link \<provider>" →
`GET /auth/link/{provider}`, which issues the same one-time state but stores
`linking_user_id` (from the session JWT) in it. The callback verifies the
provider identity, then runs
`INSERT INTO user_identities (provider, provider_uid, user_id) VALUES ($1, $2, <linking_user_id>)
ON CONFLICT (provider, provider_uid) DO NOTHING` — attaching instead of
find-or-create. `UNIQUE (user_id, provider)` rejects linking a second identity
of the same provider; the `(provider, provider_uid)` PK makes an identity
already owned by another user an error (never silently re-attach).

**Display names:** future providers supply one via `map_user` (Discord
`global_name`/`username`, Pocket ID `preferred_username`/`name`); Steam keeps
`GetPlayerSummaries`.

**Out of scope by design:** Steam's partner-gated Web API OAuth
(`ISteamUserOAuth` — Client ID granted only for Cloud/Workshop delegation) is
not a login mechanism and is not planned; login stays OpenID 2.0. No IdP
delegation for Steam either (Keycloak lacks native OpenID 2.0 support) —
au.2143.me/Pocket ID is consumed only as a secondary `oidc` provider.

## Client Protocol

All communication happens over a single WebSocket connection at `/ws`. Messages are JSON with a `"type"` field (`snake_case`).

### Client → Server

| Type | Fields | Description |
|------|--------|-------------|
| `auth` | `session_token: String` | Authenticate with a JWT |
| `auth_ticket` | `ticket: String` | Authenticate with a Steam session ticket |
| `begin_matchmaking` | `mode: String`, `difficulty: String` | Enter queue (`"ranked_1v1"`, difficulty: `"easy"`/`"normal"`/`"hard"`) |
| `cancel_matchmaking` | — | Leave queue |
| `accept_match` | `match_token: String` | Accept a found match |
| `decline_match` | `match_token: String` | Decline a found match (rare — acceptance is the default) |
| `start_match` | `match_token: String` | Click START = the P2P connection to the opponent is established; begin the match. Sent within the START window opened once both players accept (`match_started`); if a player doesn't start in time they forfeit (or both do — double loss — if neither does) |
| `match_report` | `match_token: String`, `winner: u64?`, `demo_hash: String?` | Submit match result (winner is the victor's steam_id; `null` for draw) |
| `heartbeat` | — | Client liveness signal. Send every ~10s for as long as you're connected — the server drops the connection 30s after the last heartbeat, and while queueing the queue entry is dropped 30s after the last heartbeat too (so a 10s cadence keeps both alive indefinitely) |

### Server → Client

| Type | Fields | Description |
|------|--------|-------------|
| `auth_ok` | `steam_id: u64`, `display_name: String`, `state: string` | Authentication succeeded; `state` is the player's persisted status (`in_menus`/`queueing`/`match_accepted`/`in_match`/`reporting`) so a reconnect knows where it left off |
| `match_found` | `match_token: String`, `opponent: { steam_id, display_name }`, `timeout_ms: u64`, `game_type: "p2p" \| "server"` | A match is ready — accept or it expires; `game_type` tells the client which lifecycle to run |
| `queue_status` | `elapsed_ms`, `band_lo/hi`, `candidates`, `queue_size`, `my_mu/sigma/rating`, `leaderboard: [{steam_id, mu, sigma, rating}]` | Live queue stats pushed every ~2s while queueing (wait time, expanding MMR band, opponents available, your rating, full MMR leaderboard) |
| `opponent_connected` | `match_token` | The opponent's `start_match` signal was accepted — the opponent is ready to begin |
| `match_started` | `match_token: String`, `start_timeout_secs: u64` | Both players accepted — the START window is open; each must send `start_match` within `start_timeout_secs` or forfeit (double loss if neither does) |
| `round_start` | `match_token: String`, `frame: u32`, `round: u32`, `countdown_ticks: u32` | A pong round begins: the referee holds the sim frozen for `countdown_ticks` 33ms ticks (3-2-1); the ball launches at `frame + countdown_ticks` |
| `report_received` | `match_token`, `reporting_player`, `winner: Option<steam_id>`, `demo_hash` | A player submitted a match report — sent to both players before resolution |
| `error` | `message: String` | An error occurred processing a message |
| `match_result` | `match_token: String`, `outcome: Value` | The reports agreed and the match resolved (`Win`/`Loss`/`Draw`/`Disputed` with mu change) — sent to **both** players |
| `match_declined` | `match_token: String` | A player declined the found match — sent to **both** players (the decliner's ack + the opponent's notification) |
| `queue_expired` | — | The player's queue entry was dropped (30s without a heartbeat) — sent to the player so the UI never freezes on a dead queue |
| `game_server_ready` | `match_token`, `address: String`, `join_token: String?` | A server-authoritative match's gameserver is up — join it |
| `game_server_error` | `match_token`, `message: String` | The gameserver could not be provisioned (allocation failed or timed out) |

### Typical flow

```
Client                           Server
  |--- auth { session_token } --->|
  |<--- auth_ok ------------------|
  |                                |
  |--- begin_matchmaking -------->|
  |        (waiting...)           |
  |<--- match_found { token } ----|
  |                                |
  |--- accept_match { token } --->|
  |--- start_match { token } --->|
  |        (match in progress)    |
  |                                |
  |--- match_report { ... } ----->|
  |                                |
  |<--- match_result -------------|
  |                                |

### Server-authoritative games

When a mode's game type is `server` (see `GAME_MODES`), the coordinator does not
relay P2P state — it provisions a dedicated game server and the **server**
reports the result via webhook:

1. Both players accept (same as P2P). The match becomes `InProgress`.
2. The coordinator calls the **creator**: `POST {GAMESERVER_CREATOR_URL}` with
   `{ match_token, game_mode, player_a, player_b, result_callback_url }` →
   `{ server_address, join_token? }`. Retried every 2s until
   `GAMESERVER_ALLOC_TIMEOUT_S` elapses, then the match is disputed.
3. Both players receive `game_server_ready { address, join_token? }` and play
   on the server. The match status is now `Playing`; the p2p signal is rejected
   for server matches.
4. The gameserver reports the outcome by POSTing to the callback URL
   (`/internal/game-result/{token}/{secret}`, body `{ winner: steam_id | null }`).
   **The unguessable URL is the authentication** — possession of the secret is
   proof. The coordinator resolves ratings exactly as for agreed player reports
   and sends `match_result` to both players.
5. If no result arrives within `GAMESERVER_RESULT_TIMEOUT_S` of the server being
   ready, the match is disputed.

For development, set `AUTH_DEV_MODE=true` and leave `GAMESERVER_CREATOR_URL`
unset: the coordinator falls back to a built-in **mock creator**
(`POST /internal/mock/allocate`) that returns `127.0.0.1:25565` and auto-reports
player_a's win 3 seconds after allocation, so the whole flow is visible without
any external service. In production set `GAMESERVER_CREATOR_URL` and `PUBLIC_URL`
(the gameserver must be able to reach the callback URL).

## Just Recipes

| Recipe | What it does |
|--------|-------------|
| `just build` | Compile all crates |
| `just test` | Run all unit tests + doc test |
| `just itest` | Run the DB-backed integration tests (needs `just db-up` + `just temporal-up`) |
| `just test-verbose` | Run all tests with `--nocapture` |
| `just lint` | Clippy with `-D warnings` |
| `just fmt` | Auto-format with rustfmt |
| `just fmt-check` | Check formatting without changing files |
| `just run` | Start the server (needs `.env` + PostgreSQL + `just temporal-up`) |
| `just db-up` | Start PostgreSQL in Docker on port 5432 |
| `just db-down` | Stop the database container |
| `just temporal-up` | Start the local Temporal stack (podman): frontend :7233, UI :8233 |
| `just temporal-down` | Stop the Temporal stack |
| `just up` | Full Docker stack (lobby + db, builds the image) |
| `just down` | Stop the full Docker stack |
| `just clean` | Remove build artifacts (`cargo clean`) |

- **lobby-client** — async Rust client library (reference implementation for the WS protocol)
- **lobby-macros** — proc macros used by lobby-core

## Testing

**Unit tests** (no Postgres needed): `just test` — the match-lifecycle /
rating / player-state suites in `lobby-core/tests/` using in-memory mock
stores.

**Integration tests** (Postgres + Temporal needed): `just db-up` and
`just temporal-up`, then `cargo test -p lobby-server -- --test-threads 4` —
tests that start the real server in-process (`lobby-server/src/lib.rs`'s
`build_app`), run it against PostgreSQL, and drive it with `lobby-client`.
The matchmaking/lifecycle tests run through the Temporal workflows (each test
spins its own worker on a unique task queue); keep `--test-threads 4` — the
default (all cores) exhausts the local Postgres connection limit.
`lobby-server/tests/live_e2e.rs` (`#[ignore]`d) drives the same flow against a
running dev server (`just run`) with Temporal up.

- `full_match_lifecycle` — two players queue, match, accept, connect, report;
  asserts the match resolves, ratings update, and a `match_results` row is written.
- `logout_revokes_token` — a logged-out token no longer authenticates a WebSocket.
- `ws_frame_size_limit` — oversized WS frames are rejected.
- `replaced_connection_keeps_new` — a second connection replaces the first.
- `ws_origin_restriction` — non-allowlisted browser origins get 403 on `/ws`.
- `dispute_on_winner_mismatch` — players report different winners; asserts the
  match is marked `Disputed` and no outcome is persisted.
- `queue_cancel` — a player cancels matchmaking; asserts no match ever arrives.
- `server_game_full_lifecycle` — a server-authoritative match: creator
  allocation, `game_server_ready` to both players, webhook result, ratings
  update, and the callback URL carrying token + secret.
- `server_game_alloc_timeout` — a failing creator; the match is disputed after
  `GAMESERVER_ALLOC_TIMEOUT_S` with `game_server_error` + `match_result`.
- `server_game_result_timeout` — a ready server that never reports; the match
  is disputed after `GAMESERVER_RESULT_TIMEOUT_S`.
- `game_result_callback_security` — wrong secret → 401, duplicate callback →
  409, unknown token → 404.

The DB-backed tests use `#[sqlx::test]`: each test gets a fresh, migrated,
per-test database (created and dropped automatically), so tests run in
parallel against Postgres without interfering. `just db-up` must be running or
the tests fail fast with a connection error.

## Web Demo

`web/index.html` is a zero-dependency browser client (native `fetch` +
`WebSocket`, no build step) that emulates a player. The server embeds and
serves it at `/` — with the server running, just open `http://localhost:8080/`
in two browser tabs. To test multiple users locally:

4. Pick a **mode** — the dropdown is populated from `GET /modes`
   (`ranked_1v1 (p2p)` by default; `server_arena (server)` when the default
   `GAME_MODES` is in force). Click **Start Matchmaking** in both — the
   queueing panel shows live wait time, the expanding MMR band and opponents in
   it, your own μ/σ/rating, and a leaderboard of every player's rating.
5. **Accept** on both. Once both have accepted, the server opens a **START
   window** (`match_started`, default 15s): click **START Match** in each tab
   — the button simulates the P2P connection being established (the real
   WebRTC data channel is created on the first game frame). Each tab shows
   the other's signal as `Opponent ready ✓`; a player who doesn't start in
   time forfeits (or both do — a double loss — if neither starts). For a
   server mode the panel shows
   "Waiting for server allocation…", then the (simulated) server address; the
   built-in mock creator auto-reports player_a's win ~3s later, and both tabs
   show the resolved result.
6. Report a result (p2p) — each tab immediately shows what the other player
   selected (`You reported: Win (you)` / `Opponent reported: …`) before the
   match resolves.

The event log shows every JSON message sent and received — a live protocol
reference. No Steam account or API key is involved.

The match lifecycle is being migrated to activity-based Temporal workflows
(session → queue → match, with the START window, connect handshake and report
timeouts as workflow timers) — see the plan's Part B design before touching
the in-process lifecycle code.

## Contributing

```bash
just lint     # clippy with -D warnings
just test     # all unit tests must pass
just itest    # integration tests must pass (Postgres running)
just fmt      # format with rustfmt
```

Uses conventional commits for PRs.
