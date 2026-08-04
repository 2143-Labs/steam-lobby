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
just test                 # run unit tests
just itest                # run integration tests against Postgres (9 pass)
just run                  # start server on :8080
```

To emulate multiple users without Steam, set `AUTH_DEV_MODE=true` in `.env`
(it must be `false` in production), then open `web/index.html` in two browser
tabs (distinct steam IDs), connect both, and start matchmaking in each — the
demo talks to the dev-only `/auth/test-token` endpoint, no real Steam API
calls involved.
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
|--------|------|-------------|
| GET | `/health` | Health check, returns `"ok"` |
| GET | `/auth/steam/login` | Steam OpenID login redirect |
| GET | `/auth/steam/callback` | Steam OpenID callback |
| GET | `/ws` | WebSocket upgrade (game protocol) |
| POST | `/auth/ticket` | Steam ticket auth -> JWT (rate-limited 10/min per IP) |
| POST | `/auth/test-token` | Dev-only: JWT for any steam_id (only when `AUTH_DEV_MODE=true`) |
| POST | `/auth/logout` | Revoke the current session token (all earlier tokens die) |

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
| `RUST_LOG` | `info,lobby_server=debug` | Tracing log level |
| `LOBBY_HOST` | `0.0.0.0` | Bind address |
| `LOBBY_PORT` | `8080` | Bind port |
| `PUBLIC_URL` | — | Required for Steam OpenID login (absolute public origin); login returns `400` without it |

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

Or probe it: `POST /auth/test-token` returns a JWT when `AUTH_DEV_MODE=true` and
`404` otherwise. The web demo (`web/index.html`) needs `AUTH_DEV_MODE=true` and
your demo origin listed in `CORS_ORIGINS`; its Connect button uses the
test-token endpoint (or a `#token=` fragment from a Steam login, if present).

## WebSocket Quick Test

With `AUTH_DEV_MODE=true`, the dev-only token endpoint is enabled. Get a JWT and connect:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/auth/test-token \
  -H 'Content-Type: application/json' \
  -d '{"steam_id": 12345}' | jq -r '.token')
wscat -c "ws://localhost:8080/ws" -H "Authorization: Bearer $TOKEN"
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
Each login gets a one-time random `state`, bound to the exact `return_to`;
replaying a callback, or presenting a `return_to` that does not match the
issued state, is rejected. The session JWT is delivered as a URL **fragment**
(`#token=…`), never as a query parameter.

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

## How It Works

Players connect over WebSocket and authenticate with a JWT (obtained via Steam OpenID or the dev token endpoint). Once authenticated, a player enters the queue with a difficulty (Easy/Normal/Hard) at their current MMR.

When a match is found, both players receive a `MatchFound` message. Both must accept before the match transitions to `InProgress`. After the match, each player submits a report (winner + optional demo hash). If both agree, ratings update via the Weng-Lin algorithm (a modern Bayesian system with uncertainty tracking). If they disagree or demo hashes mismatch, the match is disputed.

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
| `p2p_connected` | `match_token: String` | Notify server that P2P connection to opponent is established |
| `match_report` | `match_token: String`, `winner: u64?`, `demo_hash: String?` | Submit match result (winner is the victor's steam_id; `null` for draw) |

### Server → Client

| Type | Fields | Description |
|------|--------|-------------|
| `auth_ok` | `steam_id: u64`, `display_name: String` | Authentication succeeded |
| `match_found` | `match_token: String`, `opponent: { steam_id, display_name }`, `timeout_ms: u64` | A match is ready — accept or it expires |
| `error` | `message: String` | An error occurred processing a message |
| `match_result` | `match_token: String`, `outcome: Value` | A report resolved the match (`Win`/`Loss`/`Draw`/`Disputed` with mu change) |
| `match_declined` | `match_token: String` | Your opponent declined the found match |

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
  |--- p2p_connected { token } -->|
  |        (match in progress)    |
  |                                |
  |--- match_report { ... } ----->|
  |<--- match_found (if re-queued)|
```

## Just Recipes

| Recipe | What it does |
|--------|-------------|
| `just build` | Compile all crates |
| `just test` | Run all unit tests (10) + doc test |
| `just itest` | Run 9 DB-backed integration tests (needs `just db-up`) |
| `just test-verbose` | Run all tests with `--nocapture` |
| `just lint` | Clippy with `-D warnings` |
| `just fmt` | Auto-format with rustfmt |
| `just fmt-check` | Check formatting without changing files |
| `just run` | Start the server (needs `.env` + PostgreSQL) |
| `just db-up` | Start PostgreSQL in Docker on port 5432 |
| `just db-down` | Stop the database container |
| `just up` | Full Docker stack (lobby + db, builds the image) |
| `just down` | Stop the full Docker stack |
| `just clean` | Remove build artifacts (`cargo clean`) |

- **lobby-client** — async Rust client library (reference implementation for the WS protocol)
- **lobby-macros** — proc macros used by lobby-core

## Testing

**Unit tests** (no Postgres needed): `just test` — 10 tests covering the match
lifecycle (accept/connect/report/expiry, winner validation, double-accept
idempotency, auto-loss on report timeout) and the Weng-Lin rating algorithm,
using in-memory mock stores.

**Integration tests** (Postgres needed): `just db-up` then `just itest` — 9
tests that start the real server in-process (`lobby-server/src/lib.rs`'s
`build_app`), run it against PostgreSQL, and drive it with `lobby-client`:

- `full_match_lifecycle` — two players queue, match, accept, connect, report;
  asserts the match resolves, ratings update, and a `match_results` row is written.
- `dispute_on_winner_mismatch` — players report different winners; asserts the
  match is marked `Disputed` and no outcome is persisted.
- `queue_cancel` — a player cancels matchmaking; asserts no match ever arrives.
- `rate_limited_test_token` — the dev token endpoint rate-limits per IP.
- `logout_revokes_token` — a logged-out token no longer authenticates a WebSocket.
- `ws_frame_size_limit` — oversized WS frames are rejected.
- `replaced_connection_keeps_new` — a second connection replaces the first.
- `ws_origin_restriction` — non-allowlisted browser origins get 403 on `/ws`.
- `dispute_on_winner_mismatch` — players report different winners; asserts the
  match is marked `Disputed` and no outcome is persisted.
- `queue_cancel` — a player cancels matchmaking; asserts no match ever arrives.

The tests use a shared `lobby_test` database (auto-created on first run,
truncated before each test). It is intentionally never dropped — the next run
just truncates again. `just db-up` must be running or the tests fail fast with a
connection error.

## Web Demo

`web/index.html` is a zero-dependency browser client (native `fetch` +
`WebSocket`, no build step) that emulates a player. To test multiple users
locally:

1. `just db-up` + `just run` (with `AUTH_DEV_MODE=true` in `.env`).
2. Open `web/index.html` in two browser tabs.
3. Give each tab a distinct steam ID, click **Connect** on both.
4. Click **Start Matchmaking** in both — each tab shows the other as its
   opponent.
5. **Accept** on both, then **P2P Connected**, then report a result.

The event log shows every JSON message sent and received — a live protocol
reference. No Steam account or API key is involved.

## Contributing

```bash
just lint     # clippy with -D warnings
just test     # all unit tests must pass
just itest    # integration tests must pass (Postgres running)
just fmt      # format with rustfmt
```

Uses conventional commits for PRs.
