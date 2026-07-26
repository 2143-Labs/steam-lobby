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

## WebSocket Quick Test

When `STEAM_API_KEY=test`, the dev-only token endpoint is enabled. Get a JWT and connect:

```bash
TOKEN=$(curl -s -X POST http://localhost:8080/auth/test-token \
  -H 'Content-Type: application/json' \
  -d '{"steam_id": 12345}' | jq -r '.token')

wscat -c "ws://localhost:8080/ws" -H "Authorization: Bearer $TOKEN"
```

Then send: `{"type":"begin_matchmaking","mode":"ranked_1v1","difficulty":"normal"}`

## Architecture

- **lobby-core** — traits, types, MMR algorithm, queue logic, match lifecycle
- **lobby-server** — axum HTTP server, WebSocket handler, PostgreSQL store, ticker loop
- **lobby-macros** — proc macros used by lobby-core

## Contributing

```bash
just lint     # clippy with -D warnings
just test     # all tests must pass
just fmt      # format with rustfmt
```

Uses conventional commits for PRs.
