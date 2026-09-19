//! One simulated player: its native session, and the ranked commands it sends.
//!
//! Nothing here is UMVC3-specific beyond the mode name. This is the sequence a
//! real client performs, so it doubles as executable documentation of the
//! ranked flow.

use std::time::{Duration, Instant};

use anyhow::{Result, bail};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::api::Api;
use crate::log;

/// A Steam lobby id of the shape the game reports for the lobby it created.
/// Only the creating side supplies one; the joining side reports `null`.
const CREATED_LOBBY_ID: &str = "109775242781234567";

/// How the pairing placed this caller inside the match.
pub struct ActiveMatch {
    pub match_token: String,
    pub attempt_id: String,
    /// `create` or `join`. Assigned by which side of the match this caller
    /// occupies — a client cannot choose it and must read it from the snapshot.
    pub role: String,
    pub opponent: String,
}

/// The authoritative end-of-match result, as delivered on the event stream.
pub struct ResultSnapshot {
    pub event_type: String,
    /// Relative to this caller, lowercased: `win`, `loss` or `draw`.
    pub outcome: String,
    pub mmr_delta: Option<f64>,
}

pub struct Player {
    pub label: String,
    pub steam_id: u64,
    pub token: String,
}

impl Player {
    /// Exchange a Steam auth session ticket for a native bearer session.
    ///
    /// A real client obtains the ticket from `ISteamUser::GetAuthSessionTicket`.
    /// `dev:<steam_id>` stands in for it against a local server started with
    /// `AUTH_DEV_MODE=true` and unrestricted account creation; that substitution
    /// is the only reason this tool can run without Steam present.
    pub async fn authenticate(api: &Api, label: &str, steam_id: u64) -> Result<Self> {
        let ticket = format!("dev:{steam_id}");
        log::line(label, format!("POST /api/ticket  (ticket {ticket})"));
        let token = api.native_ticket(&ticket).await?;
        log::line(label, format!("session minted for steam_id {steam_id}"));
        Ok(Self {
            label: label.to_string(),
            steam_id,
            token,
        })
    }

    pub async fn state(&self, api: &Api) -> Result<Value> {
        api.state(&self.token).await
    }

    /// Send one ranked command and require that it is both admitted and applied.
    ///
    /// Admission is asynchronous by design: the server durably records the
    /// command and a background ticker applies it. `202` therefore means
    /// "durably accepted", not "done" — the receipt is polled to settlement, and
    /// a command that is refused during apply reports its own error code here.
    pub async fn send(&self, api: &Api, command: Value) -> Result<Value> {
        let kind = command
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("?")
            .to_string();
        let admitted = api.command(&self.token, command).await?;
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
        log::line(
            &self.label,
            format!(
                "{kind:<12} admitted ({}) receipt {}",
                admitted.status.as_u16(),
                short(&receipt)
            ),
        );
        let settled = self
            .await_receipt(api, &receipt, Duration::from_secs(10))
            .await?;
        if settled != "applied" {
            bail!("{}: {kind} receipt settled as {settled}", self.label);
        }
        Ok(admitted.body)
    }

    /// Poll this caller's own snapshot until its receipt settles.
    ///
    /// Receipts are caller-local, so this must be read with the asking player's
    /// token — a peer's snapshot will never show it.
    async fn await_receipt(&self, api: &Api, receipt: &Value, timeout: Duration) -> Result<String> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.state(api).await?;
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
                bail!("{}: receipt {} never settled", self.label, short(receipt));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Join the queue for `mode`. Enabling ranked never queues a player on its
    /// own; this call is the explicit player action.
    pub async fn queue(&self, api: &Api, mode: &str, difficulty: &str) -> Result<()> {
        self.send(
            api,
            json!({
                "command_id": Uuid::new_v4().to_string(),
                "type": "queue",
                "mode": mode,
                "difficulty": difficulty,
            }),
        )
        .await?;
        Ok(())
    }

    /// Poll until the server reports this player as genuinely queued.
    pub async fn await_queued(&self, api: &Api, mode: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.state(api).await?;
            if snapshot["queue"]["mode"] == mode {
                log::line(&self.label, format!("queued for {mode}"));
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!("{}: never appeared in the {mode} queue", self.label);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Poll until the matchmaker places this player into a match.
    pub async fn await_pairing(&self, api: &Api, timeout: Duration) -> Result<ActiveMatch> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.state(api).await?;
            let match_token = snapshot["active_match"]["match_token"]
                .as_str()
                .filter(|token| !token.is_empty());
            if let Some(match_token) = match_token {
                return Ok(ActiveMatch {
                    match_token: match_token.to_string(),
                    attempt_id: string_at(&snapshot, "/active_match/attempt_id"),
                    role: string_at(&snapshot, "/active_match/role"),
                    opponent: string_at(&snapshot, "/active_match/opponent"),
                });
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

    pub async fn accept(&self, api: &Api, match_token: &str) -> Result<()> {
        self.send(
            api,
            json!({
                "command_id": Uuid::new_v4().to_string(),
                "type": "accept",
                "match_token": match_token,
            }),
        )
        .await?;
        Ok(())
    }

    /// Report the lobby rendezvous. The creator names the lobby it opened; the
    /// joiner reports `null`.
    pub async fn trying(&self, api: &Api, active: &ActiveMatch) -> Result<()> {
        self.send(
            api,
            json!({
                "command_id": Uuid::new_v4().to_string(),
                "type": "trying",
                "match_token": active.match_token,
                "attempt_id": active.attempt_id,
                "role": active.role,
                "lobby_id": if active.role == "create" {
                    Value::String(CREATED_LOBBY_ID.to_string())
                } else {
                    Value::Null
                },
            }),
        )
        .await?;
        Ok(())
    }

    /// Best-faith report that the local and peer connection handshakes landed.
    pub async fn connect(&self, api: &Api, active: &ActiveMatch) -> Result<()> {
        self.send(
            api,
            json!({
                "command_id": Uuid::new_v4().to_string(),
                "type": "connect",
                "match_token": active.match_token,
                "attempt_id": active.attempt_id,
                "local_reached": true,
                "peer_reached": true,
            }),
        )
        .await?;
        Ok(())
    }

    pub async fn ready(&self, api: &Api, active: &ActiveMatch) -> Result<()> {
        self.send(
            api,
            json!({
                "command_id": Uuid::new_v4().to_string(),
                "type": "ready",
                "match_token": active.match_token,
                "attempt_id": active.attempt_id,
            }),
        )
        .await?;
        Ok(())
    }

    /// Submit this client's final result. `outcome` is relative to the reporter.
    ///
    /// The match is rated only when both reports agree on the winner *and* on
    /// score/checksum/end_frame; anything else resolves as a dispute with no MMR
    /// movement, which is why the metadata is sent verbatim by both sides.
    pub async fn report(&self, api: &Api, match_token: &str, outcome: &str) -> Result<()> {
        self.send(
            api,
            json!({
                "command_id": Uuid::new_v4().to_string(),
                "type": "report",
                "match_token": match_token,
                "outcome": outcome,
                "score": "2-0",
                "checksum": "deadbeef",
                "end_frame": 900,
            }),
        )
        .await?;
        Ok(())
    }

    /// Wait for the phase this player's own snapshot reports.
    ///
    /// Checking each stage is what turns a stall into a precise failure at the
    /// stage that stalled, instead of a timeout at the end of the flow.
    pub async fn await_phase(&self, api: &Api, phase: &str, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.state(api).await?;
            let seen = string_at(&snapshot, "/active_match/phase");
            if seen == phase {
                log::line(&self.label, format!("phase -> {phase}"));
                return Ok(());
            }
            if Instant::now() >= deadline {
                bail!(
                    "{}: expected phase {phase}, last saw '{}'",
                    self.label,
                    seen
                );
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// This caller's current event cursor.
    pub async fn cursor(&self, api: &Api) -> Result<i64> {
        Ok(self.state(api).await?["cursor"].as_i64().unwrap_or(0))
    }

    /// Wait for the authoritative result.
    ///
    /// It arrives on the event stream rather than in the snapshot: finalizing a
    /// match clears `player_state.active_match_token`, so `active_match` — and
    /// with it any `result` field — disappears along with the match it
    /// described. `/api/events` is the only client-visible path for it.
    pub async fn await_result(
        &self,
        api: &Api,
        from_cursor: i64,
        match_token: &str,
        timeout: Duration,
    ) -> Result<ResultSnapshot> {
        let deadline = Instant::now() + timeout;
        let mut cursor = from_cursor;
        loop {
            // The server holds this request for up to 25s while the page would
            // be empty, so the loop needs no sleep of its own.
            let page = api.events(&self.token, cursor).await?;
            cursor = page["next_cursor"].as_i64().unwrap_or(cursor);
            let event = page["events"].as_array().and_then(|events| {
                events
                    .iter()
                    .find(|event| {
                        event["match_token"].as_str() == Some(match_token)
                            && matches!(event["type"].as_str(), Some("resolved") | Some("disputed"))
                    })
                    .cloned()
            });
            if let Some(event) = event {
                return Ok(ResultSnapshot {
                    event_type: event["type"].as_str().unwrap_or_default().to_string(),
                    outcome: string_at(&event, "/payload/result/outcome"),
                    mmr_delta: event["payload"]["result"]["mmr_delta"].as_f64(),
                });
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

/// Read a dotted path out of a JSON value, collapsing anything missing to "".
fn string_at(value: &Value, path: &str) -> String {
    let mut current = value;
    for step in path.trim_start_matches('/').split('/') {
        current = &current[step];
    }
    current.as_str().unwrap_or_default().to_string()
}

/// Abbreviate an opaque id for the transcript.
fn short(value: &Value) -> String {
    let text = value.as_str().unwrap_or_default();
    text.chars().take(8).collect()
}
