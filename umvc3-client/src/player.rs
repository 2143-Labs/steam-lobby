//! One simulated player: its native session, and the ranked commands it sends.
//!
//! Command bodies are built by small pure functions and sent through
//! [`Player::send`], so the interactive shell can show, replay and deliberately
//! corrupt exactly what went on the wire.

use std::fmt;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

use crate::api::{Admission, Api, detail};
use crate::log;
use crate::session::Session;

/// A read the server refused with 401 even after any refresh this session was
/// allowed to make. Typed, so the shell can tell it from a transport failure.
#[derive(Debug)]
pub struct NativeUnauthorized {
    pub what: &'static str,
}

impl fmt::Display for NativeUnauthorized {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} -> 401 Unauthorized: the native session was not accepted",
            self.what
        )
    }
}

impl std::error::Error for NativeUnauthorized {}

/// A Steam lobby id of the shape the game reports for the lobby it created.
/// Only the creating side supplies one; the joining side reports `null`.
pub const CREATED_LOBBY_ID: &str = "109775242781234567";

/// How the pairing placed this caller inside the match.
#[derive(Clone)]
pub struct ActiveMatch {
    pub match_token: String,
    pub attempt_id: String,
    /// `create` or `join`. Assigned by which side of the match this caller
    /// occupies — a client cannot choose it and must read it from the snapshot.
    pub role: String,
    pub opponent: String,
    pub phase: String,
}

impl ActiveMatch {
    /// Parse the active match out of a `/api/ranked/state` snapshot.
    pub fn from_state(snapshot: &Value) -> Option<Self> {
        let node = &snapshot["active_match"];
        let match_token = node["match_token"]
            .as_str()
            .filter(|token| !token.is_empty())?;
        Some(Self {
            match_token: match_token.to_string(),
            attempt_id: node["attempt_id"].as_str().unwrap_or_default().to_string(),
            role: node["role"].as_str().unwrap_or_default().to_string(),
            opponent: node["opponent"].as_str().unwrap_or_default().to_string(),
            phase: node["phase"].as_str().unwrap_or_default().to_string(),
        })
    }

    /// A compact one-line description for the transcript.
    pub fn summary(&self) -> String {
        format!(
            "match={} phase={} attempt={} role={}",
            self.match_token,
            self.phase,
            short(&self.attempt_id),
            self.role
        )
    }
}

/// The authoritative end-of-match result, as delivered on the event stream.
pub struct ResultSnapshot {
    pub event_type: String,
    /// Relative to this caller, lowercased: `win`, `loss` or `draw`.
    pub outcome: String,
    pub mmr_delta: Option<f64>,
}

/// Cheap to clone: the interactive shell hands a copy to each background
/// reader so they can poll independently of the command loop. Clones share one
/// [`Session`], so a refreshed bearer reaches every reader without a restart.
#[derive(Clone)]
pub struct Player {
    pub label: String,
    /// Known for a dev ticket; a browser-approved device session does not
    /// learn which Steam account approved it.
    pub steam_id: Option<u64>,
    pub session: Session,
    pub api: Api,
}

impl Player {
    /// Exchange a Steam auth session ticket for a native bearer session.
    ///
    /// A real client obtains the ticket from `ISteamUser::GetAuthSessionTicket`.
    /// `dev:<steam_id>` stands in for it against a local server started with
    /// `AUTH_DEV_MODE=true` and unrestricted account creation; that substitution
    /// is the only reason this tool can run without Steam present.
    pub async fn authenticate(base: &str, label: &str, steam_id: u64) -> Result<Self> {
        let api = Api::new(base, label)?;
        let session = Session::dev(api.clone(), steam_id).await?;
        Ok(Self::with_session(label, api, session, Some(steam_id)))
    }

    /// Resume the device credential saved in `file` — no browser prompt.
    pub async fn from_device(base: &str, label: &str, file: &Path) -> Result<Self> {
        let api = Api::new(base, label)?;
        let session = Session::device(api.clone(), file).await?;
        Ok(Self::with_session(label, api, session, None))
    }

    /// Start a new device grant, wait for its approval, and save it to `file`.
    pub async fn login(
        base: &str,
        label: &str,
        file: &Path,
        dev_approve_steam_id: Option<u64>,
    ) -> Result<Self> {
        let api = Api::new(base, label)?;
        let session = crate::session::device_login(&api, label, file, dev_approve_steam_id).await?;
        Ok(Self::with_session(label, api, session, None))
    }

    fn with_session(label: &str, api: Api, session: Session, steam_id: Option<u64>) -> Self {
        Self {
            label: label.to_string(),
            steam_id,
            session,
            api,
        }
    }

    /// The Steam ID for the transcript, or `unknown` for a device session.
    pub fn steam_id_text(&self) -> String {
        self.steam_id
            .map_or_else(|| "unknown".to_string(), |id| id.to_string())
    }

    // ── command bodies ───────────────────────────────────────────────────
    //
    // Pure builders, so a caller can log, replay or corrupt the exact body.

    pub fn queue_body(&self, mode: &str, difficulty: &str) -> Value {
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "queue",
            "mode": mode,
            "difficulty": difficulty,
        })
    }

    pub fn cancel_queue_body(&self) -> Value {
        json!({ "command_id": Uuid::new_v4().to_string(), "type": "cancel_queue" })
    }

    pub fn heartbeat_body(&self) -> Value {
        json!({ "command_id": Uuid::new_v4().to_string(), "type": "heartbeat" })
    }

    pub fn accept_body(&self, match_token: &str) -> Value {
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "accept",
            "match_token": match_token,
        })
    }

    pub fn decline_body(&self, match_token: &str) -> Value {
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "decline",
            "match_token": match_token,
        })
    }

    /// `attempt_override` exists so a caller can deliberately send a stale
    /// attempt id and watch the server refuse it.
    pub fn trying_body(&self, active: &ActiveMatch, attempt_override: Option<&str>) -> Value {
        let attempt = attempt_override.unwrap_or(&active.attempt_id);
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "trying",
            "match_token": active.match_token,
            "attempt_id": attempt,
            "role": active.role,
            "lobby_id": if active.role == "create" {
                Value::String(CREATED_LOBBY_ID.to_string())
            } else {
                Value::Null
            },
        })
    }

    pub fn connect_body(&self, active: &ActiveMatch, attempt_override: Option<&str>) -> Value {
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "connect",
            "match_token": active.match_token,
            "attempt_id": attempt_override.unwrap_or(&active.attempt_id),
            "local_reached": true,
            "peer_reached": true,
        })
    }

    pub fn ready_body(&self, active: &ActiveMatch, attempt_override: Option<&str>) -> Value {
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "ready",
            "match_token": active.match_token,
            "attempt_id": attempt_override.unwrap_or(&active.attempt_id),
        })
    }

    /// The final result. `outcome` is relative to the reporter.
    ///
    /// The match is rated only when both reports agree on the winner *and* on
    /// score/checksum/end_frame; anything else resolves as a dispute with no MMR
    /// movement, so these three are the knobs for provoking a dispute.
    pub fn report_body(
        &self,
        match_token: &str,
        outcome: &str,
        score: &str,
        checksum: &str,
        end_frame: u64,
    ) -> Value {
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "report",
            "match_token": match_token,
            "outcome": outcome,
            "score": score,
            "checksum": checksum,
            "end_frame": end_frame,
        })
    }

    /// `target` is `self` or `peer`.
    pub fn abandon_body(&self, match_token: &str, target: &str) -> Value {
        json!({
            "command_id": Uuid::new_v4().to_string(),
            "type": "abandon",
            "match_token": match_token,
            "target": target,
        })
    }

    // ── sending ──────────────────────────────────────────────────────────

    /// POST one command body verbatim. No interpretation; the only retry is a
    /// single replay of the identical body — same `command_id` — after a 401
    /// that this session could answer with a fresh bearer.
    pub async fn send(&self, body: Value) -> Result<Admission> {
        let bearer = self.session.access().await?;
        let mut admitted = self.api.command(&bearer, body.clone()).await?;
        if admitted.status == StatusCode::UNAUTHORIZED
            && self.session.refresh_after_401(&bearer).await?
        {
            let bearer = self.session.access().await?;
            admitted = self.api.command(&bearer, body.clone()).await?;
        }
        self.log_admission(&body, &admitted);
        Ok(admitted)
    }

    /// POST a body with an explicit bearer, bypassing the session entirely —
    /// the deliberate bad-token probe, which must never trigger a refresh.
    pub async fn send_with_token(&self, token: &str, body: Value) -> Result<Admission> {
        let admitted = self.api.command(token, body.clone()).await?;
        self.log_admission(&body, &admitted);
        Ok(admitted)
    }

    fn log_admission(&self, body: &Value, admitted: &Admission) {
        let kind = body.get("type").and_then(Value::as_str).unwrap_or("?");
        if admitted.ok() {
            log::line(
                &self.label,
                format!(
                    "    = {kind} {} receipt {}",
                    admitted.status.as_u16(),
                    short(admitted.body["receipt"].as_str().unwrap_or_default())
                ),
            );
        } else {
            log::line(
                &self.label,
                format!(
                    "    = {kind} {} {}",
                    admitted.status.as_u16(),
                    admitted.error_code().unwrap_or("(no error code)")
                ),
            );
        }
    }

    /// POST and require both admission and application — the deterministic path
    /// the scripted flow uses. The interactive shell deliberately does not wait,
    /// so the effect of a command is observed asynchronously.
    pub async fn send_applied(&self, body: Value) -> Result<()> {
        let kind = body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string();
        let admitted = self.send(body).await?;
        if !admitted.ok() {
            bail!(
                "{}: {kind} refused -> {}{}",
                self.label,
                admitted.status,
                admitted
                    .error_code()
                    .map(|code| format!(" {code}"))
                    .unwrap_or_default()
            );
        }
        let receipt = admitted.body["receipt"].clone();
        let settled = self
            .await_receipt(&receipt, Duration::from_secs(10))
            .await?;
        if settled != "applied" {
            bail!("{}: {kind} receipt settled as {settled}", self.label);
        }
        Ok(())
    }

    /// Poll this caller's own snapshot until its receipt settles.
    ///
    /// Receipts are caller-local, so this must be read with the asking player's
    /// token — a peer's snapshot will never show it.
    pub async fn await_receipt(&self, receipt: &Value, timeout: Duration) -> Result<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.state_quiet().await?;
            let entry = snapshot["receipts"].as_array().and_then(|receipts| {
                receipts
                    .iter()
                    .find(|entry| entry["receipt"] == *receipt)
                    .cloned()
            });
            if let Some(entry) = entry {
                let status = entry["status"].as_str().unwrap_or("pending");
                if status != "pending" {
                    if let Some(code) = entry["error_code"].as_str() {
                        bail!("{}: command rejected during apply -> {code}", self.label);
                    }
                    return Ok(status.to_string());
                }
            }
            if Instant::now() >= deadline {
                bail!(
                    "{}: receipt {} never settled",
                    self.label,
                    short_value(receipt)
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    // ── inspection ───────────────────────────────────────────────────────

    pub async fn state(&self) -> Result<Value> {
        self.state_with(true).await
    }

    /// The same read without printing the exchange — the background poller uses
    /// this so its once-per-tick requests do not bury the user's own commands.
    pub async fn state_quiet(&self) -> Result<Value> {
        self.state_with(false).await
    }

    async fn state_with(&self, verbose: bool) -> Result<Value> {
        let bearer = self.session.access().await?;
        let (mut status, mut body) = self.api.state_response(&bearer, verbose).await?;
        if status == StatusCode::UNAUTHORIZED && self.session.refresh_after_401(&bearer).await? {
            let bearer = self.session.access().await?;
            (status, body) = self.api.state_response(&bearer, verbose).await?;
        }
        checked("GET /api/ranked/state", status, body)
    }

    /// This caller's active match, if any.
    pub async fn active(&self) -> Result<Option<ActiveMatch>> {
        Ok(ActiveMatch::from_state(&self.state().await?))
    }

    /// The same lookup without printing the exchange. The waiters below poll
    /// this every 50ms, and logging each poll would bury the exchange the user
    /// actually asked to see.
    async fn active_quiet(&self) -> Result<Option<ActiveMatch>> {
        Ok(ActiveMatch::from_state(&self.state_quiet().await?))
    }

    /// This caller's current event cursor.
    pub async fn cursor(&self) -> Result<i64> {
        Ok(self.state().await?["cursor"].as_i64().unwrap_or(0))
    }

    /// One page of this caller's events, from `after`.
    pub async fn events(&self, after: i64) -> Result<Value> {
        self.events_with(after, true).await
    }

    /// The same long-poll without printing the exchange, for the poller.
    pub async fn events_quiet(&self, after: i64) -> Result<Value> {
        self.events_with(after, false).await
    }

    async fn events_with(&self, after: i64, verbose: bool) -> Result<Value> {
        let bearer = self.session.access().await?;
        let (mut status, mut body) = self.api.events_response(&bearer, after, verbose).await?;
        if status == StatusCode::UNAUTHORIZED && self.session.refresh_after_401(&bearer).await? {
            let bearer = self.session.access().await?;
            (status, body) = self.api.events_response(&bearer, after, verbose).await?;
        }
        checked("GET /api/events", status, body)
    }

    // ── waiting ──────────────────────────────────────────────────────────

    /// Poll until the server reports this player as genuinely queued.
    pub async fn await_queued(&self, mode: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            if self.state_quiet().await?["queue"]["mode"] == mode {
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("{}: never appeared in the {mode} queue", self.label);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until the matchmaker places this player into a match.
    pub async fn await_pairing(&self, timeout: Duration) -> Result<ActiveMatch> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(active) = self.active_quiet().await? {
                return Ok(active);
            }
            if Instant::now() >= deadline {
                bail!(
                    "{}: the matchmaker never paired this player — check that Temporal is running \
                     and that the server has RANKED_QUEUE_ENABLED on",
                    self.label
                );
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }

    /// Wait for the phase this player's own snapshot reports.
    pub async fn await_phase(&self, phase: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let seen = self
                .active_quiet()
                .await?
                .map(|active| active.phase)
                .unwrap_or_default();
            if seen == phase {
                log::line(&self.label, format!("    phase -> {phase}"));
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("{}: expected phase {phase}, last saw '{seen}'", self.label);
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait for the authoritative result.
    ///
    /// It arrives on the event stream rather than in the snapshot: finalizing a
    /// match clears `player_state.active_match_token`, so `active_match` — and
    /// with it any `result` field — disappears along with the match it
    /// described. `/api/events` is the only client-visible path for it.
    pub async fn await_result(
        &self,
        from_cursor: i64,
        match_token: &str,
        timeout: Duration,
    ) -> Result<ResultSnapshot> {
        let deadline = Instant::now() + timeout;
        let mut cursor = from_cursor;
        loop {
            let page = self.events(cursor).await?;
            cursor = page["next_cursor"].as_i64().unwrap_or(cursor);
            if let Some(result) = resolved_in(&page, match_token) {
                return Ok(result);
            }
            if Instant::now() >= deadline {
                bail!(
                    "{}: no resolved event for {match_token} within {timeout:?}",
                    self.label
                );
            }
        }
    }
}

/// Judge a read's status: 401 as the typed [`NativeUnauthorized`], any other
/// refusal with its status and error code, success as the body.
fn checked(what: &'static str, status: StatusCode, body: Value) -> Result<Value> {
    if status == StatusCode::UNAUTHORIZED {
        return Err(NativeUnauthorized { what }.into());
    }
    if !status.is_success() {
        bail!("{what} -> {status}{}", detail(&body));
    }
    Ok(body)
}

/// Find the terminal result for `match_token` in one event page, if present.
pub fn resolved_in(page: &Value, match_token: &str) -> Option<ResultSnapshot> {
    let event = page["events"].as_array()?.iter().find(|event| {
        event["match_token"].as_str() == Some(match_token)
            && matches!(event["type"].as_str(), Some("resolved") | Some("disputed"))
    })?;
    Some(ResultSnapshot {
        event_type: event["type"].as_str().unwrap_or_default().to_string(),
        outcome: event["payload"]["result"]["outcome"]
            .as_str()
            .unwrap_or_default()
            .to_string(),
        mmr_delta: event["payload"]["result"]["mmr_delta"].as_f64(),
    })
}

/// Abbreviate an opaque id for the transcript.
pub fn short(text: &str) -> String {
    text.chars().take(8).collect()
}

fn short_value(value: &Value) -> String {
    short(value.as_str().unwrap_or_default())
}

/// Read a `/`-separated path out of a JSON value, missing steps becoming null.
pub fn at<'a>(value: &'a Value, path: &str) -> &'a Value {
    let mut current = value;
    for step in path.trim_start_matches('/').split('/') {
        current = &current[step];
    }
    current
}

/// A field as a string, or "" when absent.
pub fn text(value: &Value, path: &str) -> String {
    at(value, path).as_str().unwrap_or_default().to_string()
}

/// Context helper used by the shell when a command needs an active match.
pub fn require_match(active: Option<ActiveMatch>) -> Result<ActiveMatch> {
    active.context("this player has no active match")
}
