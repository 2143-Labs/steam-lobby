# Steam Lobby

A matchmaking and MMR service for Steam games, built with Axum + PostgreSQL.

## Prerequisites

- **Docker** — for PostgreSQL
- **Nix** — provides Rust + just + OpenSSL via `nix-shell shell.nix`

### Non-NixOS

Install Rust (`rustup`), OpenSSL (system package manager), and `just` (`cargo install just`). Then use the same `just` commands below.

## Quickstart (NixOS)

```bash
cp .env.example .env
nix-shell shell.nix     # enter dev shell (provides cargo, rustc, just)
just db-up              # start PostgreSQL in Docker
just test               # run all tests (8 pass)
just run                # start server on :8080
```

## Quickstart (non-NixOS)

```bash
cp .env.example .env
cargo install just
just db-up
just test
just run
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check, returns `"ok"` |
| GET | `/auth/steam/login` | Steam OpenID login redirect |
| GET | `/auth/steam/callback` | Steam OpenID callback |
| GET | `/ws` | WebSocket upgrade (game protocol) |
| POST | `/auth/ticket` | Steam ticket auth -> JWT |
| POST | `/auth/test-token` | Dev-only: JWT for any steam_id (only when `STEAM_API_KEY=test`) |

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgres://lobby:lobby@localhost:5432/lobby` | PostgreSQL connection string |
| `JWT_SECRET` | — | Secret for signing JWTs |
| `STEAM_API_KEY` | `test` | Steam Web API key; `test` enables `/auth/test-token` |
| `STEAM_APP_ID` | `480` | Steam App ID for the game |
| `MATCH_ACCEPT_TIMEOUT_S` | `30` | Seconds players have to accept a match |
| `REPORT_TIMEOUT_S` | `300` | Seconds after match ends to report outcome |
| `RUST_LOG` | `info,lobby_server=debug` | Tracing log level |
| `LOBBY_HOST` | `0.0.0.0` | Bind address |
| `LOBBY_PORT` | `8080` | Bind port |

> **Production:** Set `STEAM_API_KEY` to a real [Steam Web API key](https://steamcommunity.com/dev/apikey). The `/auth/test-token` endpoint is disabled automatically when the key is not `"test"`. Change `JWT_SECRET` to a strong random value.

## WebSocket Quick Test

When `STEAM_API_KEY=test`, the dev-only token endpoint is enabled. Get a JWT and connect:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/auth/test-token \
  -H 'Content-Type: application/json' \
  -d '{"steam_id": 12345}' | jq -r '.token')

wscat -c "ws://localhost:8080/ws" -H "Authorization: Bearer $TOKEN"
```

Then send: `{"type":"begin_matchmaking","mode":"ranked_1v1","difficulty":"normal"}`

## How It Works

Players connect over WebSocket and authenticate with a JWT (obtained via Steam OpenID or the dev token endpoint). Once authenticated, a player enters the queue with a difficulty (Easy/Normal/Hard) at their current MMR.

A ticker runs every 2 seconds, pairing the longest-waiting player with the nearest opponent within an expanding MMR band. The band starts at 50 and grows by 25 every 10 seconds of wait time, capped at 400. Difficulty applies an MMR offset (±150) so Easy players match with slightly higher-rated opponents and Hard players with lower-rated ones.

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
| `just test` | Run all 8 tests |
| `just lint` | Clippy with `-D warnings` |
| `just fmt` | Auto-format with rustfmt |
| `just fmt-check` | Check formatting without changing files |
| `just run` | Start the server (needs `.env` + PostgreSQL) |
| `just db-up` | Start PostgreSQL in Docker on port 5432 |
| `just db-down` | Stop the database container |
| `just up` | Full Docker stack (lobby + db, builds the image) |
| `just down` | Stop the full Docker stack |
| `just clean` | Remove build artifacts (`cargo clean`) |
## Architecture

- **lobby-core** — traits, types, MMR algorithm, queue logic, match lifecycle
- **lobby-server** — axum HTTP server, WebSocket handler, PostgreSQL store, ticker loop
- **lobby-client** — async Rust client library (reference implementation for the WS protocol)
- **lobby-macros** — proc macros used by lobby-core

## Contributing

```bash
just lint     # clippy with -D warnings
just test     # all tests must pass
just fmt      # format with rustfmt
```

Uses conventional commits for PRs.
