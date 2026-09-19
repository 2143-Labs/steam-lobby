//! The ranked (native) HTTP surface, in one place.
//!
//! Every request body here is a raw JSON literal rather than a type shared with
//! the server. That is deliberate: this file is the wire contract a real client
//! has to reproduce, so a mismatch has to surface as a failure *here* rather
//! than being hidden behind a type both sides happen to share.
//!
//! Every exchange is printed verbatim, which is the point of the tool — but
//! credentials are never printed. Bearer tokens are redacted from both the
//! request header and any response body that carries one.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};

use crate::log;

/// One `/api/events` long-poll is held by the server for 25s, so the request
/// must be allowed to outlive that.
const EVENTS_TIMEOUT: Duration = Duration::from_secs(40);

/// Default timeout for ordinary request/response calls.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Object keys whose values are credentials, matched case-insensitively at any
/// depth. A `ticket` is exempted only when it is the synthetic `dev:` form.
const SECRET_KEYS: [&str; 7] = [
    "access_token",
    "refresh_token",
    "session_token",
    "token",
    "secret",
    "client_secret",
    "password",
];

static SHOW_PACKETS: AtomicBool = AtomicBool::new(true);

pub fn show_packets(on: bool) {
    SHOW_PACKETS.store(on, Ordering::Relaxed);
}

pub fn packets_shown() -> bool {
    SHOW_PACKETS.load(Ordering::Relaxed)
}

#[derive(Clone)]
pub struct Api {
    http: reqwest::Client,
    base: String,
    /// Transcript tag — the player this handle acts as.
    tag: String,
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
    pub fn new(base: &str, tag: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .context("build the HTTP client")?;
        Ok(Self {
            http,
            base: base.trim_end_matches('/').to_string(),
            tag: tag.to_string(),
        })
    }

    pub fn base(&self) -> &str {
        &self.base
    }

    /// One exchange, logged. `body` is the exact JSON that goes on the wire.
    ///
    /// `log_exchange` is false for background polling: the shell prints what it
    /// observes from those polls, and logging every 2s heartbeat on top of that
    /// would bury the exchanges the user actually asked for.
    async fn exec(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
        body: Option<&Value>,
        timeout: Option<Duration>,
        log_exchange: bool,
    ) -> Result<(reqwest::StatusCode, Value)> {
        let url = format!("{}{path}", self.base);
        let mut request = self.http.request(method.clone(), &url);
        if let Some(token) = token {
            request = request.bearer_auth(token);
        }
        if let Some(body) = body {
            request = request.json(body);
        }
        if let Some(timeout) = timeout {
            request = request.timeout(timeout);
        }

        let verbose = log_exchange && packets_shown();
        if verbose {
            log::line(&self.tag, format!("--> {method} {path}"));
            if token.is_some() {
                log::line(&self.tag, "      Authorization: Bearer <redacted>");
            }
            if let Some(body) = body {
                log::line(&self.tag, format!("      {}", compact(&redact(body))));
            }
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("{method} {path}"))?;
        let status = response.status();
        let body: Value = response.json().await.unwrap_or(Value::Null);

        if verbose {
            log::line(&self.tag, format!("<-- {}", status.as_u16()));
            log::line(&self.tag, format!("      {}", compact(&redact(&body))));
        }
        Ok((status, body))
    }

    /// `GET /modes` — what this server will let us queue for.
    pub async fn modes(&self) -> Result<Value> {
        let (status, body) = self
            .exec(reqwest::Method::GET, "/modes", None, None, None, true)
            .await?;
        if !status.is_success() {
            bail!("GET /modes -> {status}{}", detail(&body));
        }
        Ok(body)
    }

    /// `POST /api/ticket` — exchange a Steam auth session ticket for a native bearer.
    ///
    /// A real client obtains `ticket` from `ISteamUser::GetAuthSessionTicket`.
    /// Against a local server started with `AUTH_DEV_MODE=true` and unrestricted
    /// accounts, the literal `dev:<steam_id>` is accepted in its place — that is
    /// the only reason this tool can run without Steam present.
    pub async fn native_ticket(&self, ticket: &str) -> Result<String> {
        let body = json!({ "ticket": ticket });
        let (status, response) = self
            .exec(
                reqwest::Method::POST,
                "/api/ticket",
                None,
                Some(&body),
                None,
                true,
            )
            .await?;
        if !status.is_success() {
            bail!(
                "POST /api/ticket -> {status}{}\n\
                 hint: the server needs AUTH_DEV_MODE=true and STEAM_BACKED_ACCOUNTS_ONLY \
                 unset/false to accept a dev: ticket, and a real Steam auth ticket otherwise",
                detail(&response)
            );
        }
        response["access_token"]
            .as_str()
            .map(str::to_owned)
            .context("POST /api/ticket: response carried no access_token")
    }

    /// `GET /api/ranked/state` — this caller's queue, active match and receipts.
    pub async fn state(&self, token: &str) -> Result<Value> {
        self.state_with(token, true).await
    }

    /// The same read, without printing the exchange. Used by the poller.
    pub async fn state_quiet(&self, token: &str) -> Result<Value> {
        self.state_with(token, false).await
    }

    async fn state_with(&self, token: &str, log_exchange: bool) -> Result<Value> {
        let (status, body) = self
            .exec(
                reqwest::Method::GET,
                "/api/ranked/state",
                Some(token),
                None,
                None,
                log_exchange,
            )
            .await?;
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
        let (status, body) = self
            .exec(
                reqwest::Method::POST,
                "/api/command",
                Some(token),
                Some(&body),
                None,
                true,
            )
            .await?;
        Ok(Admission { status, body })
    }

    /// `POST /api/logout` — revoke this session server-side.
    pub async fn logout(&self, token: &str) -> Result<reqwest::StatusCode> {
        let (status, _) = self
            .exec(
                reqwest::Method::POST,
                "/api/logout",
                Some(token),
                None,
                None,
                true,
            )
            .await?;
        Ok(status)
    }

    /// `GET /api/events?after=` — one recipient-local page.
    ///
    /// Long-polls for up to 25s, which is why the timeout override exists: the
    /// caller is expected to repeat immediately when a page comes back empty.
    pub async fn events(&self, token: &str, after: i64) -> Result<Value> {
        self.events_with(token, after, true).await
    }

    /// The same long-poll, without printing the exchange. Used by the poller,
    /// which reports the events it receives instead of the requests it makes.
    pub async fn events_quiet(&self, token: &str, after: i64) -> Result<Value> {
        self.events_with(token, after, false).await
    }

    async fn events_with(&self, token: &str, after: i64, log_exchange: bool) -> Result<Value> {
        let (status, body) = self
            .exec(
                reqwest::Method::GET,
                &format!("/api/events?after={after}"),
                Some(token),
                None,
                Some(EVENTS_TIMEOUT),
                log_exchange,
            )
            .await?;
        if !status.is_success() {
            bail!("GET /api/events -> {status}{}", detail(&body));
        }
        Ok(body)
    }
}

/// The exact bytes of a value, on one line.
pub fn compact(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

/// Replace every credential in a value with a placeholder, at any depth.
///
/// The `dev:` ticket form is allowed through: it is synthetic (`dev:<steam_id>`)
/// and seeing it is the point of the local flow. Any other ticket value is
/// treated as the real Steam credential it would be.
pub fn redact(value: &Value) -> Value {
    match value {
        Value::Object(map) => Value::Object(
            map.iter()
                .map(|(key, value)| {
                    let lower = key.to_ascii_lowercase();
                    if SECRET_KEYS.contains(&lower.as_str()) {
                        (key.clone(), json!("<redacted>"))
                    } else if lower == "ticket" {
                        let is_dev = value
                            .as_str()
                            .is_some_and(|ticket| ticket.starts_with("dev:"));
                        if is_dev {
                            (key.clone(), value.clone())
                        } else {
                            (key.clone(), json!("<redacted>"))
                        }
                    } else {
                        (key.clone(), redact(value))
                    }
                })
                .collect(),
        ),
        Value::Array(items) => Value::Array(items.iter().map(redact).collect()),
        other => other.clone(),
    }
}

/// Render an error body compactly: `{"error":"native_session_required"}`.
fn detail(body: &Value) -> String {
    match body.get("error").and_then(Value::as_str) {
        Some(code) => format!(" — {{\"error\":\"{code}\"}}"),
        None => String::new(),
    }
}
