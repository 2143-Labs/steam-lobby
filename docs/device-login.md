# Device login (native client ↔ server contract)

How a native client (the reference client today, the UMVC3 hook tomorrow) gets a
durable credential without the game ever handling a Steam password, and without
the server needing a Steamworks publisher key.

**Status: proposed.** `umvc3-client` implements this flow in full;
`lobby-server` has no `/api/device/*` route yet, so a live run answers 404 and
the client says so rather than silently falling back to `/api/ticket`
(`umvc3-client/src/api.rs:320-324`). Everything below marked *required* is what
the client already sends or expects; everything marked *proposed* is part of the
design but not yet exercised by any caller.

## Why not Steam auth tickets

`/auth/ticket` and `/api/ticket` verify a ticket through
`ISteamUserAuth/AuthenticateUserTicket` (`lobby-server/src/routes.rs:324-327`).
That method requires a **Publisher** Web API key associated with the appid — a
plain user key is not accepted, and the key is tied to the app's Steamworks
publisher group.

UMVC3 is appid `357190` and belongs to Capcom. A third-party mod cannot obtain a
publisher key for it, so ticket auth cannot authenticate real players, whatever
`STEAM_APP_ID` is set to. The device flow needs no key at all: the server learns
the Steam ID from an ordinary Steam sign-in in the browser.

This is the reason the device flow exists, and it should be recorded wherever
the ranked service's auth is described.

## Flow

```
client                                    server                human
  │                                          │                    │
  │ POST /api/device/start                   │                    │
  ├─────────────────────────────────────────>│                    │
  │ 200 {device_code, user_code,             │                    │
  │      verification_uri_complete,          │                    │
  │      expires_in, interval}               │                    │
  │<─────────────────────────────────────────┤                    │
  │                                          │                    │
  │ print "open <url>, confirm code <code>"  │                    │
  │                                          │<───────────────────┤
  │                                          │  signs in with     │
  │                                          │  Steam, approves   │
  │                                          │                    │
  │ POST /api/device/token                   │                    │
  │   {grant_type: device_code, device_code} │                    │
  ├─────────────────────────────────────────>│                    │
  │ 400 {error: authorization_pending}       │                    │
  │<─────────────────────────────────────────┤   (repeat every    │
  │ ...                                      │    `interval` s)   │
  │ 200 {access_token, refresh_token,        │                    │
  │      expires_in}                         │                    │
  │<─────────────────────────────────────────┤                    │
  │                                          │                    │
  │ save refresh_token to the token file     │                    │
```

Later runs skip the browser entirely:

```
  │ POST /api/device/token                   │
  │   {grant_type: refresh_token,            │
  │    refresh_token}                        │
  ├─────────────────────────────────────────>│
  │ 200 {access_token, expires_in}           │
  │<─────────────────────────────────────────┤
```

The client prints the URL but never opens a browser itself: opening a browser is
the game's decision to make, not the client's.

## Endpoints the client calls (required)

| Method | Path | Body / auth | Success |
|---|---|---|---|
| POST | `/api/device/start` | `{client_name, client_version, device_label}` | `200` with the start object |
| POST | `/api/device/token` | `{grant_type:"device_code", device_code}` | `200` with `access_token`, `refresh_token`, `expires_in` |
| POST | `/api/device/token` | `{grant_type:"refresh_token", refresh_token}` | `200` with `access_token`, `expires_in`, and **no** `refresh_token` |
| POST | `/api/device/logout` | `{refresh_token}` | `204` — revokes the durable credential |
| POST | `/api/device/{user_code}/approve` | `Authorization: Bearer <browser JWT>`, no body | `204` |

`/api/device/start` returns `device_code` (secret, kept by the client),
`user_code` (short, read by a human), `verification_uri`,
`verification_uri_complete` (the URL with the code already in it), `expires_in`
and `interval`. The client accepts any `user_code` matching
`^[A-Z0-9-]{1,16}$` (`umvc3-client/src/api.rs:561-568`) because it is
interpolated into a URL path, and it takes `expires_in`/`interval` from the
response rather than assuming values.

## Error codes

`/api/device/token` answers `400` with `{"error": "<code>"}`:

| Code | Meaning | Client behaviour |
|---|---|---|
| `authorization_pending` | Nobody has approved yet | keep polling at `interval` |
| `slow_down` | Polling too fast | add 5s to the interval, capped at 60s |
| `access_denied` | The human denied it | stop, report the refusal |
| `expired_token` | The grant lapsed | stop, tell the user to log in again |
| `invalid_grant` | Not a grant the server knows | stop, tell the user to log in again |
| `unsupported_grant_type` | Wrong `grant_type` | stop, report as a protocol error |

Anything else `400` is reported as a protocol error with the code shown (bounded
to a short ASCII identifier). `404` means the server has no device login at all.
`429` and any `5xx` are treated as **transient**, not terminal: a login in
progress is not aborted by a hiccup.

`/api/device/start` may answer `503 {"error":"public_url_required"}` when the
server has no `PUBLIC_URL` to build a browser URL from.

## Refresh rules

- A refresh response **must not include `refresh_token`**. This client does not
  implement rotation; it refuses a rotated credential loudly rather than
  silently discarding it (`umvc3-client/src/api.rs:417-422`). If rotation is
  wanted later, it is a deliberate client change, not a server-side switch.
- `invalid_grant` on a refresh is **terminal**: the client clears the saved
  credential and tells the user to log in again. Do not return it for a
  transient problem.
- The client refreshes proactively: when the current time passes
  `expiry − min(300s, lifetime/2)`. It also refreshes once on a `401` and
  replays the original request **with the same `command_id`**, which relies on
  the server deduplicating an identical replay.
- After a failed refresh the client waits 5s before trying again automatically.
- `POST /api/logout` (access-only) revokes the current native bearer but leaves
  the device credential valid; the client pauses automatic refresh so the dead
  bearer's `401`s stay visible until the user asks for a `refresh`.

## Approving on a dev server

`--dev-approve` stands in for the human's browser against a **loopback** server:
it calls `/auth/test-token` for a browser JWT, then POSTs
`/api/device/{user_code}/approve` with it. The client refuses any non-loopback
base URL, but that check is a convenience, not a security boundary — the server
must gate dev approval itself (the existing `/auth/test-token` gate is
`AUTH_DEV_MODE=true` and `STEAM_BACKED_ACCOUNTS_ONLY` unset/false).

The approve route answers `403 {"error":"steam_required"}` when the approving
identity has no Steam ID.

## Where the credential is stored

`umvc3-client` writes two keys beside the game's existing settings:

```ini
[Settings]
RankedRefreshToken=<opaque>
RankedBaseUrl=https://<origin>
```

The write is atomic (0600 sibling temp file, then `rename`), a symlink or a
group/world-readable file is refused on read, and the credential is bound to
`RankedBaseUrl`'s origin — a run pointed at a different origin is refused before
any credential leaves the machine.

## Open decisions

1. **Rotation.** The current contract is a long-lived, non-rotating credential
   on disk. A stolen file stays valid for its whole lifetime. Rotating on each
   refresh, a shorter sliding TTL, and a visible `GET /api/devices` +
   `POST /api/devices/{id}/revoke` surface on the web app are the obvious
   mitigations; all cost client changes.
2. **Whose Steam ID is it?** With browser approval the server learns the Steam
   ID that *signed in to the browser*, not the Steam ID running the game.
   Someone can approve on account A and play on account B. The ticket design
   bound those two; the device design does not. Whether that matters depends on
   whether ranked identity means "the account that owns the game" or "the human
   who clicked approve".
3. **Protecting the credential on Windows.** The hook runs where mods, launchers
   and trainers already write. DPAPI (`CryptProtectData`) is the standard
   answer; the reference client cannot demonstrate it.
4. **Relation to `/api/link/*`.** `lobby-server/src/lib.rs:327-331` already
   routes a link surface (`/api/link/intent`, `/confirm`, `/native/start`,
   `/native/confirm`, `/revoke`). Nothing in this document has been reconciled
   against it — that surface has not been audited here, and how (or whether) it
   interacts with the device flow is undecided.

## Known client issues

Found in review of `61581f1`, none fixed yet:

1. **Redirects are followed.** `Api::new` sets only a timeout
   (`umvc3-client/src/api.rs:137-140`), so `reqwest` follows up to 10 hops. A
   `307`/`308` from `/api/device/token` would replay the refresh credential to
   the origin named in `Location`. The fix is
   `.redirect(reqwest::redirect::Policy::none())`.
2. **A stale read can be reported as recovery.** A long-poll begun before
   `logout`/`revoke-device` and answered afterwards prints `session recovered`
   and clears the failure state, because reads carry no generation
   (`umvc3-client/src/repl.rs:242-251`, `:307-323`). On the real 25s events hold
   this is close to every logout.
3. **`Admission::error_code` is unfiltered.** It returns the server's `error`
   string with no bound (`umvc3-client/src/api.rs:126-128`), unlike every other
   server string, and it is printed to stdout — so a server can forge a
   transcript line.
4. **`logout` narrates a pause that may not have happened.** The "automatic
   refresh paused" note is unconditional, but the session pauses only on a `204`
   (`umvc3-client/src/session.rs:214-219`).

A failed `ini::save` after a successful approval also loses the credential: the
grant is already spent and rotation is unsupported, so the user must approve a
second time.

## Test status

Covered by `umvc3-client/tests/device_session.rs` against an in-process mock of
this contract (13 subprocess tests) plus unit tests in `api.rs`, `ini.rs` and
`session.rs`. Not covered: proactive refresh before expiry (every mock uses a
3600s lifetime), the `slow_down` ladder beyond one step, the `--dev-approve`
happy path, and scripted/two-player mode.

No test can validate the real server until these routes exist.
