# umvc3-client

A reference native client for the ranked service's HTTP surface. It exists to
pin down the wire contract a real game hook will implement: it drives two
players through queue → match → accept → connect → report, and it models the
credential handoff (`docs/device-login.md`) that a game has to get right.

It is not a game. It does not read the game's settings or files; the token file
below is this client's own, named explicitly by the operator.

## Running it

With the toolchain from the repo root (`nix-shell --run "…"`), or plain `cargo`
if Rust is on your PATH:

```bash
# from the repository root
cargo run -p umvc3-client -- --help
```

Three run modes:

- **Interactive shell** — the default when stdin is a terminal. Type `help` for
  roughly 30 commands covering queueing, the match handshake, inspection, and a
  set of deliberately abnormal probes (`bad-token`, `conflict`, `steal`).
- **`--auto`** — runs the whole lifecycle once and exits. This is the regression
  path.
- **`--single`** — drives one player, so a pair can come from two terminals
  (`--side a` / `--side b`). Useful when you want to watch both sides.

Without `--single` one process drives both players.

## Against a local dev server

No token file, no browser: the client signs in with a `dev:<steam_id>` ticket,
so the server must be dev-flavoured and Temporal must be up for pairing.

```bash
# server side, once
#   AUTH_DEV_MODE=true  (enables the dev ticket)
#   RANKED_QUEUE_ENABLED  (defaults true)
#   just db-up && just temporal-up && just run

cargo run -p umvc3-client -- --auto
```

The interactive equivalent, which is where the interesting commands are:

```bash
cargo run -p umvc3-client -- --interactive
```

## With a device login

Against a server that implements the device routes (`docs/device-login.md`),
log in once and reuse the saved credential:

```bash
# once: prints a URL and a code; approve it in a browser signed in with Steam
cargo run -p umvc3-client -- login \
    --base-url https://<service-origin> \
    --token-file ~/.config/umvc3-ranked/session.ini

# afterwards: no browser
cargo run -p umvc3-client -- --token-file ~/.config/umvc3-ranked/session.ini
```

`login` saves the credential to `[Settings] RankedRefreshToken` and the origin it
belongs to as `RankedBaseUrl`. Later runs exchange it for a native session and
refresh automatically as the session nears expiry, so a long run keeps working
without a browser.

Against a loopback dev server, `--dev-approve` stands in for the human's browser
(and only against a loopback server):

```bash
cargo run -p umvc3-client -- login --token-file /tmp/dev.ini --dev-approve
```

**The token file is a real credential.** It is revocable and long-lived. Keep it
private — the client writes it `0600` and refuses to read a group- or
world-readable one, or a symlink.

## Shell commands

`help` in the shell lists everything. The session-related ones, which are easy to
confuse:

| Command | Effect |
|---|---|
| `logout` | Ends the current access session only. Background reads report `session unauthorized (401)` until you `refresh`. |
| `refresh` | Exchanges the saved device credential for a fresh bearer now; resumes automatic refresh. |
| `revoke-device` | Revokes the durable credential on the server and clears it from the token file. Log in again to come back. |

`logout` is a normal end-of-session; `revoke-device` is the destructive one.

## Tests

No Postgres, no Temporal, no network:

```bash
cargo test -p umvc3-client
```

Unit tests live beside the code (`src/api.rs`, `src/ini.rs`, `src/session.rs`);
`tests/device_session.rs` drives the built binary against an in-process mock of
the device contract, with canary values asserting that no credential ever reaches
the transcript. The suite runs in CI on every push to `main` and every in-repo
PR, and the review findings behind the tests are in `docs/device-login.md`.

## Source layout

| File | Responsibility |
|---|---|
| `src/main.rs` | CLI: argument parsing, refusals before any I/O, run-mode selection, the scripted lifecycle |
| `src/repl.rs` | Interactive shell: commands, background event readers, the state ticker |
| `src/player.rs` | One player's commands over a `Session`, including the one-shot 401 replay |
| `src/session.rs` | The credential state machine: proactive refresh, cooldowns, pause, device login |
| `src/ini.rs` | The token file: atomic 0600 write, origin binding, refusal of unsafe files |
| `src/api.rs` | The wire surface: every endpoint, redaction, typed errors |
| `src/log.rs` | Line-oriented output |

## Contract documents

- `docs/device-login.md` — the device-login contract, refresh rules, open
  decisions, and known issues.
- `~/repos/login-server-handoff.md` — the game-side handoff this client is
  modelled on. It predates the device flow and still describes Steam ticket
  auth and a `/link/token` route; where it disagrees with `docs/device-login.md`,
  the latter is what this client implements.
