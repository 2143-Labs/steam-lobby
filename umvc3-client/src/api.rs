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

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};

use crate::log;

/// One `/api/events` long-poll is held by the server for 25s, so the request
/// must be allowed to outlive that.
const EVENTS_TIMEOUT: Duration = Duration::from_secs(40);

/// Default timeout for ordinary request/response calls.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);

/// Object keys whose values are credentials, matched at any depth after
/// lowercasing and dropping `_`/`-`, so `accessToken`, `access-token` and
/// `access_token` are one key. A `ticket` is exempted only in its `dev:` form.
const SECRET_KEYS: [&str; 10] = [
    "accesstoken",
    "refreshtoken",
    "sessiontoken",
    "idtoken",
    "devicecode",
    "token",
    "secret",
    "clientsecret",
    "password",
    "authorization",
];

/// What a device-login response is refused with when it is not JSON of the
/// expected shape. The body itself is never echoed: it may carry a credential.
const MALFORMED: &str = "malformed response (body withheld)";

/// A device grant as started: shown to the human (`user_code`, the URL) and
/// polled with the secret `device_code`.
pub struct DeviceStart {
    pub device_code: String,
    pub user_code: String,
    pub verification_uri_complete: String,
    pub expires_in: u64,
    pub interval: u64,
}

/// One poll of a pending device grant.
pub enum DevicePoll {
    Pending,
    SlowDown,
    Denied,
    Expired,
    Invalid,
    Issued {
        access_token: String,
        refresh_token: String,
        expires_in: u64,
    },
}

/// A native bearer and how long the server says it lives.
pub struct IssuedAccess {
    pub access_token: String,
    pub expires_in: u64,
}

/// Exchanging a saved device credential for a fresh native bearer.
pub enum RefreshOutcome {
    Issued(IssuedAccess),
    /// The grant is revoked or expired; no retry can succeed.
    InvalidGrant,
}

/// A failure worth retrying later: rate limiting, a 5xx, or no response at all.
/// Callers find it with `error.downcast_ref::<Transient>()`.
#[derive(Debug)]
pub struct Transient(pub String);

impl fmt::Display for Transient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for Transient {}

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
    ///
    /// The status is returned rather than judged so the caller can tell a 401
    /// (refresh and retry once) from any other refusal. `verbose` false is the
    /// background poller, which prints what it observes instead.
    pub(crate) async fn state_response(
        &self,
        token: &str,
        verbose: bool,
    ) -> Result<(StatusCode, Value)> {
        self.exec(
            reqwest::Method::GET,
            "/api/ranked/state",
            Some(token),
            None,
            None,
            verbose,
        )
        .await
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
    pub(crate) async fn events_response(
        &self,
        token: &str,
        after: i64,
        verbose: bool,
    ) -> Result<(StatusCode, Value)> {
        self.exec(
            reqwest::Method::GET,
            &format!("/api/events?after={after}"),
            Some(token),
            None,
            Some(EVENTS_TIMEOUT),
            verbose,
        )
        .await
    }

    // ── device login ─────────────────────────────────────────────────────
    //
    // A proposed contract: the server does not implement these routes yet, so a
    // 404 means "not available here", never "fall back to /api/ticket".

    /// `POST /api/device/start` — begin a browser-approved device grant.
    pub async fn device_start(&self, device_label: &str) -> Result<DeviceStart> {
        let body = json!({
            "client_name": "umvc3-client",
            "client_version": env!("CARGO_PKG_VERSION"),
            "device_label": device_label,
        });
        let (status, response) = self
            .exec(
                reqwest::Method::POST,
                "/api/device/start",
                None,
                Some(&body),
                None,
                true,
            )
            .await?;
        match status {
            StatusCode::OK => {}
            StatusCode::NOT_FOUND => {
                bail!("POST /api/device/start -> 404: device login is not available on this server")
            }
            StatusCode::SERVICE_UNAVAILABLE
                if error_id(&response) == Some("public_url_required") =>
            {
                bail!(
                    "POST /api/device/start -> 503 public_url_required: the server has no public \
                     URL to send a browser to"
                )
            }
            StatusCode::TOO_MANY_REQUESTS => {
                bail!("POST /api/device/start -> 429: rate limited; wait and try again")
            }
            other => bail!("POST /api/device/start -> {other}{}", detail(&response)),
        }
        let malformed = || format!("POST /api/device/start: {MALFORMED}");
        let user_code = nonempty(&response, "user_code").with_context(malformed)?;
        if !valid_user_code(&user_code) {
            bail!(malformed());
        }
        let verification_uri = nonempty(&response, "verification_uri").with_context(malformed)?;
        let complete = nonempty(&response, "verification_uri_complete").with_context(malformed)?;
        for uri in [&verification_uri, &complete] {
            let parsed = reqwest::Url::parse(uri).ok().with_context(malformed)?;
            if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
                bail!(malformed());
            }
        }
        Ok(DeviceStart {
            device_code: nonempty(&response, "device_code").with_context(malformed)?,
            user_code,
            verification_uri_complete: complete,
            expires_in: positive(&response, "expires_in").with_context(malformed)?,
            interval: positive(&response, "interval").with_context(malformed)?,
        })
    }

    /// `POST /api/device/token` (device_code grant) — one poll. Not logged: it
    /// repeats every few seconds for as long as the human takes to approve.
    pub async fn device_poll(&self, device_code: &str) -> Result<DevicePoll> {
        let body = json!({ "grant_type": "device_code", "device_code": device_code });
        let (status, response) = self.token_exchange(&body, false).await?;
        if status == StatusCode::OK {
            let access = issued_access(&response)?;
            let refresh_token = nonempty(&response, "refresh_token")
                .with_context(|| format!("POST /api/device/token: {MALFORMED}"))?;
            return Ok(DevicePoll::Issued {
                access_token: access.access_token,
                refresh_token,
                expires_in: access.expires_in,
            });
        }
        if status == StatusCode::BAD_REQUEST {
            return Ok(match error_id(&response) {
                Some("authorization_pending") => DevicePoll::Pending,
                Some("slow_down") => DevicePoll::SlowDown,
                Some("access_denied") => DevicePoll::Denied,
                Some("expired_token") => DevicePoll::Expired,
                Some("invalid_grant") => DevicePoll::Invalid,
                _ => bail!("POST /api/device/token -> 400{}", detail(&response)),
            });
        }
        Err(token_failure(status, &response))
    }

    /// `POST /api/device/token` (refresh_token grant) — a fresh native bearer
    /// for a saved device credential, without a browser.
    pub async fn device_refresh(&self, refresh: &str) -> Result<RefreshOutcome> {
        let body = json!({ "grant_type": "refresh_token", "refresh_token": refresh });
        let (status, response) = self.token_exchange(&body, true).await?;
        if status == StatusCode::OK {
            if !response["refresh_token"].is_null() {
                bail!(
                    "POST /api/device/token: the server rotated the refresh credential, which \
                     this client does not support"
                );
            }
            return Ok(RefreshOutcome::Issued(issued_access(&response)?));
        }
        if status == StatusCode::BAD_REQUEST && error_id(&response) == Some("invalid_grant") {
            return Ok(RefreshOutcome::InvalidGrant);
        }
        Err(token_failure(status, &response))
    }

    /// `POST /api/device/logout` — revoke the durable device credential.
    pub async fn device_revoke(&self, refresh: &str) -> Result<()> {
        let body = json!({ "refresh_token": refresh });
        let (status, response) = self
            .exec(
                reqwest::Method::POST,
                "/api/device/logout",
                None,
                Some(&body),
                None,
                true,
            )
            .await?;
        match status {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::NOT_FOUND => {
                bail!(
                    "POST /api/device/logout -> 404: device login is not available on this server"
                )
            }
            other => bail!("POST /api/device/logout -> {other}{}", detail(&response)),
        }
    }

    /// Approve a pending grant as the local test user `steam_id`, standing in
    /// for the human's browser. Loopback servers only: the browser JWT from
    /// `/auth/test-token` is only ever requested from this machine.
    pub async fn dev_approve(&self, user_code: &str, steam_id: u64) -> Result<()> {
        if !is_loopback(&self.base) {
            bail!("--dev-approve only talks to a loopback server (localhost, 127.0.0.1, ::1)");
        }
        if !valid_user_code(user_code) {
            bail!("refusing to approve a malformed user code");
        }
        let body = json!({ "steam_id": steam_id });
        let (status, response) = self
            .exec(
                reqwest::Method::POST,
                "/auth/test-token",
                None,
                Some(&body),
                None,
                true,
            )
            .await?;
        if !status.is_success() {
            bail!(
                "POST /auth/test-token -> {status}{}\n\
                 hint: the server needs AUTH_DEV_MODE=true and STEAM_BACKED_ACCOUNTS_ONLY unset/false",
                detail(&response)
            );
        }
        let browser = nonempty(&response, "token")
            .with_context(|| format!("POST /auth/test-token: {MALFORMED}"))?;
        let path = format!("/api/device/{user_code}/approve");
        let (status, response) = self
            .exec(
                reqwest::Method::POST,
                &path,
                Some(&browser),
                None,
                None,
                true,
            )
            .await?;
        match status {
            StatusCode::NO_CONTENT => Ok(()),
            StatusCode::FORBIDDEN if error_id(&response) == Some("steam_required") => bail!(
                "POST {path} -> 403 steam_required: this server only lets a Steam-backed \
                 identity approve a device"
            ),
            other => bail!("POST {path} -> {other}{}", detail(&response)),
        }
    }

    /// The token endpoint, with a failure to get any response marked transient.
    async fn token_exchange(&self, body: &Value, verbose: bool) -> Result<(StatusCode, Value)> {
        self.exec(
            reqwest::Method::POST,
            "/api/device/token",
            None,
            Some(body),
            None,
            verbose,
        )
        .await
        .map_err(|error| Transient(format!("{error:#}")).into())
    }
}

/// A non-success token response the caller did not map itself.
fn token_failure(status: StatusCode, response: &Value) -> anyhow::Error {
    if status == StatusCode::NOT_FOUND {
        return anyhow::anyhow!(
            "POST /api/device/token -> 404: device login is not available on this server"
        );
    }
    let message = format!("POST /api/device/token -> {status}{}", detail(response));
    if status == StatusCode::TOO_MANY_REQUESTS || status.is_server_error() {
        Transient(message).into()
    } else {
        anyhow::anyhow!(message)
    }
}

/// Validate a 200 token response's native bearer.
fn issued_access(response: &Value) -> Result<IssuedAccess> {
    let malformed = || format!("POST /api/device/token: {MALFORMED}");
    match &response["token_type"] {
        Value::Null => {}
        Value::String(kind) if kind.eq_ignore_ascii_case("bearer") => {}
        _ => bail!(malformed()),
    }
    Ok(IssuedAccess {
        access_token: nonempty(response, "access_token").with_context(malformed)?,
        expires_in: positive(response, "expires_in").with_context(malformed)?,
    })
}

fn nonempty(body: &Value, key: &str) -> Option<String> {
    body[key]
        .as_str()
        .filter(|value| !value.is_empty())
        .map(str::to_owned)
}

fn positive(body: &Value, key: &str) -> Option<u64> {
    body[key].as_u64().filter(|value| *value > 0)
}

/// A code the human reads and types, and the client puts in a URL path — so it
/// is held to a short, path-safe alphabet.
pub fn valid_user_code(code: &str) -> bool {
    (1..=16).contains(&code.len())
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-')
}

/// The canonical `scheme://host[:port]` a device credential is bound to.
///
/// No path, userinfo, query or fragment, and plain HTTP only on loopback: a
/// refresh credential must never travel in cleartext across a network. The
/// input is not echoed back, since a malformed origin may embed a secret.
pub fn device_origin(raw: &str) -> Result<String> {
    let url = reqwest::Url::parse(raw.trim())
        .ok()
        .context("base URL is not a valid URL")?;
    if !matches!(url.scheme(), "http" | "https") {
        bail!("base URL must be http:// or https://");
    }
    if !url.username().is_empty() || url.password().is_some() {
        bail!("base URL must not carry credentials");
    }
    if url.query().is_some() || url.fragment().is_some() || url.path() != "/" {
        bail!("base URL must be an origin: no path, query or fragment");
    }
    let host = url.host_str().context("base URL has no host")?;
    if url.scheme() == "http" && !loopback_host(host) {
        bail!("plain http:// is only allowed for a loopback server; use https://");
    }
    Ok(url.origin().ascii_serialization())
}

/// Whether `base` names this machine.
pub fn is_loopback(base: &str) -> bool {
    reqwest::Url::parse(base)
        .ok()
        .and_then(|url| url.host_str().map(loopback_host))
        .unwrap_or(false)
}

fn loopback_host(host: &str) -> bool {
    matches!(host, "localhost" | "127.0.0.1" | "[::1]")
}

/// A server error identifier, only if it is a short ASCII code — never an
/// arbitrary server-provided string.
fn error_id(body: &Value) -> Option<&str> {
    body.get("error").and_then(Value::as_str).filter(|code| {
        (1..=64).contains(&code.len())
            && code
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'.' || b == b'-')
    })
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
                    let normal: String = key
                        .chars()
                        .filter(|c| *c != '_' && *c != '-')
                        .collect::<String>()
                        .to_ascii_lowercase();
                    let lower = normal.as_str();
                    if SECRET_KEYS.contains(&lower) {
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
/// Only a short ASCII identifier is shown, never an arbitrary server string.
pub(crate) fn detail(body: &Value) -> String {
    match error_id(body) {
        Some(code) => format!(" — {{\"error\":\"{code}\"}}"),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, Method, Uri, header};
    use axum::response::{IntoResponse, Response};

    /// One request the stub answered, as the server would have seen it.
    #[derive(Clone)]
    struct Hit {
        method: String,
        path: String,
        /// The bearer with the `Bearer ` prefix stripped.
        bearer: Option<String>,
        body: Value,
    }

    struct StubState {
        /// A canned reply per exact path, `fallback` for anything else.
        replies: HashMap<String, (StatusCode, Value)>,
        fallback: (StatusCode, Value),
        hits: Arc<Mutex<Vec<Hit>>>,
    }

    /// A loopback server that answers from a fixed script and records what it
    /// saw, so a test can prove a refusal happened before anything was sent.
    struct Stub {
        base: String,
        hits: Arc<Mutex<Vec<Hit>>>,
    }

    impl Stub {
        async fn start(
            replies: Vec<(&str, StatusCode, Value)>,
            fallback: (StatusCode, Value),
        ) -> Self {
            let hits = Arc::new(Mutex::new(Vec::new()));
            let state = Arc::new(StubState {
                replies: replies
                    .into_iter()
                    .map(|(path, status, body)| (path.to_string(), (status, body)))
                    .collect(),
                fallback,
                hits: hits.clone(),
            });
            let app = Router::new().fallback(answer).with_state(state);
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind a loopback port");
            let addr = listener.local_addr().expect("read the bound address");
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            Self {
                base: format!("http://{addr}"),
                hits,
            }
        }

        /// Every path gets the same status and body.
        async fn one(status: StatusCode, body: Value) -> Self {
            Self::start(Vec::new(), (status, body)).await
        }

        fn api(&self) -> Api {
            Api::new(&self.base, "T1").expect("build an Api")
        }

        fn hits(&self) -> Vec<Hit> {
            self.hits.lock().expect("read the hits").clone()
        }
    }

    async fn answer(
        State(state): State<Arc<StubState>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> Response {
        state.hits.lock().expect("record the request").push(Hit {
            method: method.to_string(),
            path: uri.path().to_string(),
            bearer: headers
                .get(header::AUTHORIZATION)
                .and_then(|value| value.to_str().ok())
                .map(|value| value.trim_start_matches("Bearer ").to_string()),
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        });
        let (status, body) = state
            .replies
            .get(uri.path())
            .cloned()
            .unwrap_or_else(|| state.fallback.clone());
        if status == StatusCode::NO_CONTENT {
            return status.into_response();
        }
        let bytes = serde_json::to_vec(&body).unwrap_or_default();
        (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
    }

    /// A port nothing is listening on: bound, then released.
    fn closed_port() -> u16 {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let port = listener.local_addr().expect("read the address").port();
        drop(listener);
        port
    }

    fn shape(poll: &DevicePoll) -> &'static str {
        match poll {
            DevicePoll::Pending => "pending",
            DevicePoll::SlowDown => "slow_down",
            DevicePoll::Denied => "denied",
            DevicePoll::Expired => "expired",
            DevicePoll::Invalid => "invalid",
            DevicePoll::Issued { .. } => "issued",
        }
    }

    fn refusal(error: &anyhow::Error) -> String {
        format!("{error:#}")
    }

    /// `Result::expect_err` needs `Debug` on the success type, and the device
    /// types are deliberately opaque, so a failure is unwrapped by hand.
    trait UnwrapErr<T> {
        fn failed(self, what: &str) -> anyhow::Error;
    }

    impl<T> UnwrapErr<T> for Result<T> {
        fn failed(self, what: &str) -> anyhow::Error {
            match self {
                Ok(_) => panic!("{what}: expected an error"),
                Err(error) => error,
            }
        }
    }

    #[test]
    fn valid_user_code_holds_the_path_safe_alphabet() {
        let cases: &[(&str, bool)] = &[
            ("A", true),
            ("0", true),
            ("-", true),
            ("WDJB-MJHT", true),
            ("ABCDEFGHIJKLMNOP", true),
            ("ABCDEFGHIJKLMNOPQ", false),
            ("", false),
            ("wdjb-mjht", false),
            ("WDJB_MJHT", false),
            ("WDJB MJHT", false),
            ("WDJB/MJHT", false),
            ("WDJB.MJHT", false),
            ("WDJB-MJHT\n", false),
            ("Ä", false),
        ];
        for (code, expected) in cases {
            assert_eq!(valid_user_code(code), *expected, "{code:?}");
        }
    }

    #[test]
    fn device_origin_normalizes_and_refuses_the_rest() {
        let cases: &[(&str, Result<&str, &str>)] = &[
            ("http://127.0.0.1:8080", Ok("http://127.0.0.1:8080")),
            ("http://localhost:8080/", Ok("http://localhost:8080")),
            ("http://[::1]:8080", Ok("http://[::1]:8080")),
            (
                "https://ranked.example.com",
                Ok("https://ranked.example.com"),
            ),
            (
                "HTTPS://RANKED.EXAMPLE.COM:443/",
                Ok("https://ranked.example.com"),
            ),
            (
                "  https://ranked.example.com  ",
                Ok("https://ranked.example.com"),
            ),
            ("https://ranked.example.com:8443/base", Err("origin")),
            ("https://ranked.example.com/?x=1", Err("origin")),
            ("https://ranked.example.com/#frag", Err("origin")),
            ("http://user@127.0.0.1:8080", Err("credentials")),
            ("http://10.0.0.5:8080", Err("loopback")),
            ("http://0.0.0.0:8080", Err("loopback")),
            ("ftp://127.0.0.1", Err("http://")),
            ("ranked.example.com", Err("valid URL")),
            ("http://", Err("valid URL")),
        ];
        for (raw, expected) in cases {
            match (device_origin(raw), expected) {
                (Ok(origin), Ok(want)) => assert_eq!(&origin, want, "{raw:?}"),
                (Err(error), Err(want)) => {
                    assert!(refusal(&error).contains(want), "{raw:?}: {error:#}");
                }
                (got, want) => panic!("{raw:?}: got {got:?}, wanted {want:?}"),
            }
        }
    }

    #[test]
    fn a_rejected_origin_is_never_echoed() {
        let error = device_origin("https://operator:CANARY-PASSWORD@ranked.example.com")
            .failed("credentials are refused");

        assert!(refusal(&error).contains("credentials"), "{error:#}");
        assert!(
            !refusal(&error).contains("CANARY-PASSWORD"),
            "a malformed origin may embed a secret: {error:#}"
        );
        assert!(!refusal(&error).contains("operator"), "{error:#}");
    }

    #[test]
    fn redact_hides_every_secret_and_keeps_what_the_human_reads() {
        let payload = json!({
            "access_token": "S-ACCESS",
            "AccessToken": "S-CAMEL",
            "refresh-token": "S-REFRESH",
            "session_token": "S-SESSION",
            "id_token": "S-ID",
            "device_code": "S-DEVICE",
            "token": "S-TOKEN",
            "secret": "S-SECRET",
            "client_secret": "S-CLIENT",
            "password": "S-PASSWORD",
            "Authorization": "S-AUTH",
            "ticket": "S-TICKET",
            "nested": { "deep": { "refreshToken": "S-NESTED" } },
            "grants": [{ "device_code": "S-LIST" }],
            "access_token_hint": "kept because the key is not a secret name",
            "user_code": "WDJB-MJHT",
            "match_token": "MATCH-CANARY",
            "expires_in": 900,
            "interval": 5,
        });

        let rendered = compact(&redact(&payload));

        for secret in [
            "S-ACCESS",
            "S-CAMEL",
            "S-REFRESH",
            "S-SESSION",
            "S-ID",
            "S-DEVICE",
            "S-TOKEN",
            "S-SECRET",
            "S-CLIENT",
            "S-PASSWORD",
            "S-AUTH",
            "S-TICKET",
            "S-NESTED",
            "S-LIST",
        ] {
            assert!(!rendered.contains(secret), "{secret} leaked: {rendered}");
        }
        for kept in ["WDJB-MJHT", "MATCH-CANARY", "900", "5", "not a secret name"] {
            assert!(rendered.contains(kept), "{kept} went missing: {rendered}");
        }
    }

    #[test]
    fn redact_lets_only_a_dev_ticket_through() {
        let kept = json!({
            "ticket": "dev:76561198000000101",
            "event": { "payload": { "ticket": "dev:76561198000000102" } },
        });
        let rendered = compact(&redact(&kept));
        assert!(
            rendered.contains("dev:76561198000000101")
                && rendered.contains("dev:76561198000000102"),
            "the synthetic dev ticket is the point of the local flow: {rendered}"
        );

        for payload in [
            json!({ "ticket": "CANARY-REAL-TICKET" }),
            json!({ "event": { "payload": { "ticket": "CANARY-REAL-TICKET" } } }),
            json!({ "ticket": { "inner": "CANARY-REAL-TICKET" } }),
            json!({ "ticket": " dev:CANARY-REAL-TICKET" }),
        ] {
            let rendered = compact(&redact(&payload));
            assert!(
                !rendered.contains("CANARY-REAL-TICKET"),
                "{payload} -> {rendered}"
            );
        }
    }

    #[test]
    fn detail_shows_a_short_code_and_nothing_else() {
        let long = "x".repeat(65);
        let cases: Vec<(Value, bool)> = vec![
            (json!({ "error": "native_session_required" }), true),
            (json!({ "error": "queue.disabled-x" }), true),
            (json!({ "error": "boom CANARY-LEAK" }), false),
            (json!({ "error": "internal: CANARY-LEAK" }), false),
            (json!({ "error": long }), false),
            (json!({ "error": 42 }), false),
            (json!({ "error": { "nested": "CANARY-LEAK" } }), false),
            (json!({ "message": "CANARY-LEAK" }), false),
        ];
        for (body, shown) in cases {
            let rendered = detail(&body);
            assert!(!rendered.contains("CANARY-LEAK"), "{body} -> {rendered}");
            if shown {
                assert!(
                    rendered.contains(body["error"].as_str().expect("an id")),
                    "{body} -> {rendered}"
                );
            } else {
                assert!(rendered.is_empty(), "{body} -> {rendered}");
            }
        }
    }

    #[tokio::test]
    async fn device_start_reads_a_well_formed_grant() {
        let stub = Stub::one(
            StatusCode::OK,
            json!({
                "device_code": "DEVICE-CODE",
                "user_code": "WDJB-MJHT",
                "verification_uri": "https://ranked.example.com/device",
                "verification_uri_complete": "https://ranked.example.com/device?code=WDJB-MJHT",
                "expires_in": 600,
                "interval": 5,
            }),
        )
        .await;

        let start = stub.api().device_start("P1").await.expect("start");

        assert_eq!(start.device_code, "DEVICE-CODE");
        assert_eq!(start.user_code, "WDJB-MJHT");
        assert_eq!(
            start.verification_uri_complete,
            "https://ranked.example.com/device?code=WDJB-MJHT"
        );
        assert_eq!(start.expires_in, 600);
        assert_eq!(start.interval, 5);
        let hits = stub.hits();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].method, "POST");
        assert_eq!(hits[0].path, "/api/device/start");
        assert_eq!(hits[0].bearer, None, "starting a grant takes no credential");
        assert_eq!(hits[0].body["device_label"], "P1");
    }

    #[tokio::test]
    async fn device_start_withholds_a_body_that_is_not_a_usable_grant() {
        let good = json!({
            "device_code": "S-DEVICE",
            "user_code": "WDJB-MJHT",
            "verification_uri": "https://ranked.example.com/device",
            "verification_uri_complete": "https://ranked.example.com/device?code=WDJB-MJHT",
            "expires_in": 600,
            "interval": 5,
        });
        let cases: Vec<(&str, Value)> = vec![
            (
                "lowercase user code",
                json!({ "device_code": "S-DEVICE", "user_code": "wdjb", "verification_uri": "https://h.example.com/", "verification_uri_complete": "https://h.example.com/?c=1", "expires_in": 600, "interval": 5 }),
            ),
            (
                "no device code",
                json!({ "user_code": "WDJB-MJHT", "verification_uri": "https://h.example.com/", "verification_uri_complete": "https://h.example.com/?c=1", "expires_in": 600, "interval": 5 }),
            ),
            (
                "zero expires_in",
                json!({ "device_code": "S-DEVICE", "user_code": "WDJB-MJHT", "verification_uri": "https://h.example.com/", "verification_uri_complete": "https://h.example.com/?c=1", "expires_in": 0, "interval": 5 }),
            ),
            (
                "zero interval",
                json!({ "device_code": "S-DEVICE", "user_code": "WDJB-MJHT", "verification_uri": "https://h.example.com/", "verification_uri_complete": "https://h.example.com/?c=1", "expires_in": 600, "interval": 0 }),
            ),
            (
                "javascript verification url",
                json!({ "device_code": "S-DEVICE", "user_code": "WDJB-MJHT", "verification_uri": "javascript:alert(1)", "verification_uri_complete": "https://h.example.com/?c=1", "expires_in": 600, "interval": 5 }),
            ),
            (
                "non-http verification url",
                json!({ "device_code": "S-DEVICE", "user_code": "WDJB-MJHT", "verification_uri": "ftp://h.example.com/", "verification_uri_complete": "https://h.example.com/?c=1", "expires_in": 600, "interval": 5 }),
            ),
            ("empty object", json!({})),
            ("a bare string", json!("S-DEVICE")),
            ("a JSON number where the grant belongs", json!(7)),
        ];
        for (name, body) in cases {
            let stub = Stub::one(StatusCode::OK, body).await;
            let error = stub
                .api()
                .device_start("P1")
                .await
                .failed("this body is not a grant");
            assert!(
                refusal(&error).contains("malformed response (body withheld)"),
                "{name}: {error:#}"
            );
            assert!(
                !refusal(&error).contains("S-DEVICE"),
                "{name}: the body must be withheld: {error:#}"
            );
        }

        // The same body with a usable code is the control: it is accepted.
        let stub = Stub::one(StatusCode::OK, good).await;
        assert!(stub.api().device_start("P1").await.is_ok());
    }

    #[tokio::test]
    async fn device_start_maps_its_own_refusals() {
        let cases: Vec<(StatusCode, Value, &str)> = vec![
            (
                StatusCode::NOT_FOUND,
                json!({}),
                "not available on this server",
            ),
            (
                StatusCode::SERVICE_UNAVAILABLE,
                json!({ "error": "public_url_required" }),
                "public_url_required",
            ),
            (StatusCode::TOO_MANY_REQUESTS, json!({}), "rate limited"),
            (
                StatusCode::UNAUTHORIZED,
                json!({ "error": "steam_required" }),
                "steam_required",
            ),
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                json!({ "error": "internal: CANARY-LEAK" }),
                "500",
            ),
        ];
        for (status, body, expected) in cases {
            let stub = Stub::one(status, body).await;
            let error = stub
                .api()
                .device_start("P1")
                .await
                .failed("a refused start");
            assert!(refusal(&error).contains(expected), "{status}: {error:#}");
            assert!(
                !refusal(&error).contains("CANARY-LEAK"),
                "{status}: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn device_poll_maps_the_pending_codes() {
        let cases: &[(&str, &str)] = &[
            ("authorization_pending", "pending"),
            ("slow_down", "slow_down"),
            ("access_denied", "denied"),
            ("expired_token", "expired"),
            ("invalid_grant", "invalid"),
        ];
        for (code, expected) in cases {
            let stub = Stub::one(StatusCode::BAD_REQUEST, json!({ "error": code })).await;
            let poll = stub
                .api()
                .device_poll("S-DEVICE")
                .await
                .expect("a mapped 400");
            assert_eq!(shape(&poll), *expected, "{code}");
        }

        // A 400 the client has no rule for is a refusal, not a pending grant.
        let stub = Stub::one(StatusCode::BAD_REQUEST, json!({ "error": "surprise" })).await;
        let error = stub
            .api()
            .device_poll("S-DEVICE")
            .await
            .failed("unknown code");
        assert!(refusal(&error).contains("surprise"), "{error:#}");
    }

    #[tokio::test]
    async fn device_poll_reads_the_issued_grant() {
        let stub = Stub::one(
            StatusCode::OK,
            json!({
                "access_token": "ACCESS-1",
                "refresh_token": "REFRESH-1",
                "expires_in": 3600,
                "token_type": "bEaReR",
            }),
        )
        .await;

        match stub
            .api()
            .device_poll("S-DEVICE")
            .await
            .expect("an issued grant")
        {
            DevicePoll::Issued {
                access_token,
                refresh_token,
                expires_in,
            } => {
                assert_eq!(access_token, "ACCESS-1");
                assert_eq!(refresh_token, "REFRESH-1");
                assert_eq!(expires_in, 3600);
            }
            other => panic!("expected an issued grant, got {}", shape(&other)),
        }
        let hits = stub.hits();
        assert_eq!(hits[0].method, "POST");
        assert_eq!(hits[0].path, "/api/device/token");
        assert_eq!(hits[0].body["grant_type"], "device_code");
        assert_eq!(hits[0].body["device_code"], "S-DEVICE");

        // A bearer without the durable credential cannot be saved.
        let stub = Stub::one(
            StatusCode::OK,
            json!({ "access_token": "ACCESS-1", "expires_in": 3600 }),
        )
        .await;
        let error = stub
            .api()
            .device_poll("S-DEVICE")
            .await
            .failed("no refresh credential");
        assert!(refusal(&error).contains("malformed"), "{error:#}");

        // A token that is not a bearer is not usable either.
        let stub = Stub::one(
            StatusCode::OK,
            json!({
                "access_token": "ACCESS-1",
                "refresh_token": "REFRESH-1",
                "expires_in": 3600,
                "token_type": "MAC",
            }),
        )
        .await;
        let error = stub
            .api()
            .device_poll("S-DEVICE")
            .await
            .failed("not a bearer");
        assert!(refusal(&error).contains("malformed"), "{error:#}");
    }

    #[tokio::test]
    async fn device_poll_marks_a_retryable_failure_transient() {
        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let stub = Stub::one(status, json!({ "error": "unavailable" })).await;
            let error = stub
                .api()
                .device_poll("S-DEVICE")
                .await
                .failed("still waiting");
            assert!(
                error.downcast_ref::<Transient>().is_some(),
                "{status} must keep the poll loop going: {error:#}"
            );
        }

        // No response at all is transient too: a hiccup must not abort a login
        // the human is in the middle of approving.
        let api = Api::new(&format!("http://127.0.0.1:{}", closed_port()), "T1").expect("api");
        let error = api
            .device_poll("S-DEVICE")
            .await
            .failed("no server is listening");
        assert!(
            error.downcast_ref::<Transient>().is_some(),
            "a transport failure is retryable: {error:#}"
        );

        // 404 means the server does not have this feature at all.
        let stub = Stub::one(StatusCode::NOT_FOUND, json!({})).await;
        let error = stub
            .api()
            .device_poll("S-DEVICE")
            .await
            .failed("not available");
        let rendered = refusal(&error);
        assert!(
            rendered.contains("not available on this server"),
            "{rendered}"
        );
        assert!(error.downcast_ref::<Transient>().is_none(), "{rendered}");
    }

    #[tokio::test]
    async fn device_refresh_reads_a_bearer_and_refuses_a_rotated_credential() {
        let stub = Stub::one(
            StatusCode::OK,
            json!({ "access_token": "ACCESS-2", "expires_in": 1800, "token_type": "Bearer" }),
        )
        .await;
        match stub
            .api()
            .device_refresh("S-REFRESH")
            .await
            .expect("a fresh bearer")
        {
            RefreshOutcome::Issued(issued) => {
                assert_eq!(issued.access_token, "ACCESS-2");
                assert_eq!(issued.expires_in, 1800);
            }
            RefreshOutcome::InvalidGrant => panic!("a 200 is not a revoked grant"),
        }
        let hits = stub.hits();
        assert_eq!(hits[0].path, "/api/device/token");
        assert_eq!(hits[0].body["grant_type"], "refresh_token");
        assert_eq!(hits[0].body["refresh_token"], "S-REFRESH");

        // A rotated credential would invalidate the saved one, so it is
        // refused rather than silently dropped.
        let stub = Stub::one(
            StatusCode::OK,
            json!({
                "access_token": "ACCESS-2",
                "refresh_token": "S-ROTATED",
                "expires_in": 1800,
            }),
        )
        .await;
        let error = stub
            .api()
            .device_refresh("S-REFRESH")
            .await
            .failed("rotation is not supported");
        assert!(refusal(&error).contains("rotated"), "{error:#}");
        assert!(
            !refusal(&error).contains("S-ROTATED"),
            "the replacement credential is not echoed: {error:#}"
        );
    }

    #[tokio::test]
    async fn device_refresh_separates_a_dead_grant_from_a_retryable_failure() {
        let stub = Stub::one(StatusCode::BAD_REQUEST, json!({ "error": "invalid_grant" })).await;
        assert!(
            matches!(
                stub.api()
                    .device_refresh("S-REFRESH")
                    .await
                    .expect("mapped"),
                RefreshOutcome::InvalidGrant
            ),
            "invalid_grant is terminal, not an error to retry"
        );

        for (status, body) in [
            (
                StatusCode::BAD_REQUEST,
                json!({ "error": "unsupported_grant_type" }),
            ),
            (StatusCode::UNAUTHORIZED, json!({})),
        ] {
            let stub = Stub::one(status, body).await;
            let error = stub
                .api()
                .device_refresh("S-REFRESH")
                .await
                .failed("a refusal");
            assert!(
                error.downcast_ref::<Transient>().is_none(),
                "{status} is not retryable: {error:#}"
            );
        }

        for status in [
            StatusCode::TOO_MANY_REQUESTS,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
        ] {
            let stub = Stub::one(status, json!({ "error": "unavailable" })).await;
            let error = stub
                .api()
                .device_refresh("S-REFRESH")
                .await
                .failed("retry later");
            assert!(
                error.downcast_ref::<Transient>().is_some(),
                "{status} must be retryable: {error:#}"
            );
        }
    }

    #[tokio::test]
    async fn device_revoke_accepts_only_a_204() {
        let stub = Stub::one(StatusCode::NO_CONTENT, json!({})).await;
        stub.api()
            .device_revoke("S-REFRESH")
            .await
            .expect("204 is revoked");
        let hits = stub.hits();
        assert_eq!(hits[0].method, "POST");
        assert_eq!(hits[0].path, "/api/device/logout");
        assert_eq!(hits[0].body["refresh_token"], "S-REFRESH");

        let stub = Stub::one(StatusCode::NOT_FOUND, json!({})).await;
        let error = stub.api().device_revoke("S-REFRESH").await.failed("404");
        assert!(refusal(&error).contains("not available"), "{error:#}");

        let stub = Stub::one(
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({ "error": "internal: CANARY-LEAK" }),
        )
        .await;
        let error = stub.api().device_revoke("S-REFRESH").await.failed("500");
        let rendered = refusal(&error);
        assert!(rendered.contains("500"), "{rendered}");
        assert!(!rendered.contains("CANARY-LEAK"), "{rendered}");
    }

    #[test]
    fn is_loopback_names_only_this_machine() {
        let cases: &[(&str, bool)] = &[
            ("http://localhost:8080", true),
            ("http://127.0.0.1:8080", true),
            ("http://[::1]:8080", true),
            ("http://127.0.0.2:80", false),
            ("https://ranked.example.com", false),
            ("http://10.0.0.5:8080", false),
            ("not a url", false),
        ];
        for (base, expected) in cases {
            assert_eq!(is_loopback(base), *expected, "{base}");
        }
    }

    #[tokio::test]
    async fn dev_approve_only_talks_to_loopback() {
        let api = Api::new("https://ranked.example.com", "P1").expect("api");

        let error = api
            .dev_approve("WDJB-MJHT", 7)
            .await
            .failed("a remote server is refused");
        assert!(refusal(&error).contains("loopback"), "{error:#}");
        assert!(
            error.downcast_ref::<Transient>().is_none(),
            "a refusal is not something to retry"
        );
    }

    #[tokio::test]
    async fn dev_approve_uses_the_browser_token_on_the_grant_path() {
        let stub = Stub::start(
            vec![(
                "/auth/test-token",
                StatusCode::OK,
                json!({ "token": "S-BROWSER" }),
            )],
            (StatusCode::FORBIDDEN, json!({ "error": "steam_required" })),
        )
        .await;

        let error = stub
            .api()
            .dev_approve("WDJB-MJHT", 76_561_198_000_000_101)
            .await
            .failed("a Steam-backed identity is required");
        let rendered = refusal(&error);
        assert!(rendered.contains("steam_required"), "{rendered}");
        assert!(
            !rendered.contains("S-BROWSER"),
            "the browser token is never echoed: {rendered}"
        );

        let hits = stub.hits();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].method, "POST");
        assert_eq!(hits[0].path, "/auth/test-token");
        assert_eq!(hits[0].body["steam_id"], json!(76_561_198_000_000_101u64));
        assert_eq!(hits[1].path, "/api/device/WDJB-MJHT/approve");
        assert_eq!(hits[1].bearer.as_deref(), Some("S-BROWSER"));
    }

    #[tokio::test]
    async fn dev_approve_refuses_a_malformed_code_before_asking_anything() {
        let stub = Stub::one(StatusCode::OK, json!({ "token": "S-BROWSER" })).await;

        let error = stub
            .api()
            .dev_approve("wdjb mjht", 7)
            .await
            .failed("not a code");

        assert!(refusal(&error).contains("malformed user code"), "{error:#}");
        assert!(stub.hits().is_empty(), "nothing was sent");
    }
}
