//! The ranked (native) HTTP surface, in one place.
//!
//! Every request body here is a raw JSON literal rather than a type shared with
//! the server. That is deliberate: this file is the wire contract the game
//! client has to reproduce, so a mismatch must surface as a failure *here*
//! rather than being hidden behind a type both sides happen to share.

use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

/// One `/api/events` long-poll is held by the server for 25s, so the request
/// must be allowed to outlive that.
const EVENTS_TIMEOUT: Duration = Duration::from_secs(40);

/// Default timeout for the ordinary request/response calls.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

pub struct Api {
    http: reqwest::Client,
    base: String,
}

/// The admission outcome of one command. A rejection is a normal result, not a
/// transport error, so it is returned rather than raised.
pub struct Admission {
    pub status: reqwest::StatusCode,
    pub body: Value,
}

impl Admission {
    /// The server's machine-readable code, e.g. `ranked_queue_disabled`.
    pub fn error_code(&self) -> Option<&str> {
        self.body.get("error").and_then(Value::as_str)
    }

    pub fn ok(&self) -> bool {
        self.status.is_success()
    }
}

impl Api {
    pub fn new(base: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("build the HTTP client")?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_string(),
        })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// `GET /modes` — what this server will let us queue for.
    pub async fn modes(&self) -> Result<Value> {
        let response = self
            .http
            .get(self.url("/modes"))
            .send()
            .await
            .context("GET /modes")?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            bail!("GET /modes -> {status}{}", detail(&body));
        }
        Ok(body)
    }

    /// `POST /api/ticket` — exchange a Steam auth session ticket for a native bearer.
    ///
    /// A real client obtains `ticket` from `ISteamUser::GetAuthSessionTicket`.
    /// Against a local server started with `AUTH_DEV_MODE=true` and unrestricted
    /// accounts, the literal `dev:<steam_id>` is accepted instead — that is the
    /// only reason this client can run without Steam present.
    pub async fn native_ticket(&self, ticket: &str) -> Result<String> {
        let response = self
            .http
            .post(self.url("/api/ticket"))
            .json(&json!({ "ticket": ticket }))
            .send()
            .await
            .context("POST /api/ticket")?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            bail!(
                "POST /api/ticket -> {status}{}\n\
                 hint: the server needs AUTH_DEV_MODE=true and STEAM_BACKED_ACCOUNTS_ONLY unset/false \
                 to accept a dev: ticket, and a real Steam auth ticket otherwise",
                detail(&body)
            );
        }
        body["access_token"]
            .as_str()
            .map(str::to_owned)
            .context("POST /api/ticket: response carried no access_token")
    }

    /// `GET /api/ranked/state` — this caller's queue, active match and receipts.
    pub async fn state(&self, token: &str) -> Result<Value> {
        let response = self
            .http
            .get(self.url("/api/ranked/state"))
            .bearer_auth(token)
            .send()
            .await
            .context("GET /api/ranked/state")?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            bail!("GET /api/ranked/state -> {status}{}", detail(&body));
        }
        Ok(body)
    }

    /// `POST /api/command` — durably admit one ranked command.
    ///
    /// `202` is a fresh admission, `200` an identical replay of a known
    /// `command_id`, and `409` the same id with a different payload.
    pub async fn command(&self, token: &str, body: Value) -> Result<Admission> {
        let response = self
            .http
            .post(self.url("/api/command"))
            .bearer_auth(token)
            .json(&body)
            .send()
            .await
            .context("POST /api/command")?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        Ok(Admission { status, body })
    }

    /// `GET /api/events?after=` — one recipient-local page.
    ///
    /// Long-polls for up to 25s, which is why this override exists: the caller
    /// is expected to repeat immediately when a page comes back empty.
    pub async fn events(&self, token: &str, after: i64) -> Result<Value> {
        let response = self
            .http
            .get(self.url(&format!("/api/events?after={after}")))
            .bearer_auth(token)
            .timeout(EVENTS_TIMEOUT)
            .send()
            .await
            .context("GET /api/events")?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);
        if !status.is_success() {
            bail!("GET /api/events -> {status}{}", detail(&body));
        }
        Ok(body)
    }
}

/// Render an error body compactly: `{"error":"native_session_required"}`.
fn detail(body: &Value) -> String {
    match body.get("error").and_then(Value::as_str) {
        Some(code) => format!(" — {{\"error\":\"{code}\"}}"),
        None => String::new(),
    }
}
