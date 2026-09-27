//! Device login and the saved session, driven through the real binary against
//! an in-process mock of the device wire contract.
//!
//! Everything here is offline: the mock binds a loopback port, the client runs
//! as a subprocess, and the assertions are about what the human sees and what
//! actually goes on the wire — never about the client's internals. The point of
//! the feature is that a credential travels only to the server that issued it,
//! so most of these tests are negative: a canary token must not appear, a
//! foreign origin must be sent nothing, and a refused login must leave no file.
//!
//! The device endpoints do not exist on any server yet, so the literals in
//! [`decide`] are the contract the future backend has to match. `--interactive`
//! is passed wherever stdin is piped: without it the client would treat a pipe
//! as scripted mode and wait for a matchmaker this mock does not have.

use std::collections::{HashSet, VecDeque};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, mpsc};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Bytes;
use axum::extract::State;
use axum::http::{HeaderMap, Method, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

// ── canaries ─────────────────────────────────────────────────────────────
//
// Never real credentials. Each exists to be searched for in a transcript or a
// file, so a leak fails a test instead of shipping.

/// The secret half of the grant: must never be printed.
const DEVICE_CODE: &str = "CANARY-DEVICE-CODE";
/// The half the human reads and types.
const USER_CODE: &str = "WDJB-MJHT";
/// What the mock issues when a login completes.
const LOGIN_REFRESH: &str = "CANARY-REFRESH-FROM-LOGIN";
/// What a seeded session file holds.
const SEED_REFRESH: &str = "CANARY-REFRESH-SEED";
/// The browser stand-in's bearer, used by `--dev-approve`.
const BROWSER_TOKEN: &str = "CANARY-BROWSER-TOKEN";
/// A secret the mock echoes back inside a state snapshot.
const ECHOED_ACCESS: &str = "CANARY-ECHOED-ACCESS";
/// A non-secret the transcript is expected to show.
const MATCH_TOKEN: &str = "MATCH-CANARY";

/// How long any one condition may take before a test is called stuck.
const PATIENCE: Duration = Duration::from_secs(30);

// ── the mock server ──────────────────────────────────────────────────────

/// What the refresh grant answers with.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Refresh {
    Issue,
    InvalidGrant,
    Unavailable,
}

/// One request the mock answered, as it saw it.
#[derive(Clone)]
struct Hit {
    path: String,
    bearer: Option<String>,
    body: Value,
    status: StatusCode,
}

struct Fixture {
    /// Set once the listener is bound, so the grant can echo its own origin.
    base: OnceLock<String>,
    /// Refresh grants asked for.
    grants: AtomicUsize,
    /// Bearers minted, in order: `ACCESS-1`, `ACCESS-2`, …
    issued: AtomicUsize,
    /// Bearers the read endpoints refuse with 401.
    revoked: Mutex<HashSet<String>>,
    /// Answers for the next device-code polls, before an issued grant.
    poll_script: Mutex<VecDeque<&'static str>>,
    /// When each device-code poll arrived.
    poll_times: Mutex<Vec<Instant>>,
    refresh: Mutex<Refresh>,
    /// Whether `/api/device/start` exists on this server at all.
    device_login: AtomicBool,
    /// How many `/api/command` calls are refused however good the bearer is.
    command_401: AtomicUsize,
    /// How `/api/device/{code}/approve` answers.
    approve: Mutex<StatusCode>,
    hits: Mutex<Vec<Hit>>,
}

impl Fixture {
    fn valid(&self, bearer: &str) -> bool {
        bearer.starts_with("ACCESS-")
            && !self
                .revoked
                .lock()
                .expect("read the revoked set")
                .contains(bearer)
    }

    fn origin(&self) -> String {
        self.base.get().cloned().unwrap_or_default()
    }
}

/// The client's view of this machine, with a secret echoed where a real server
/// would put a session summary.
fn state_body() -> Value {
    json!({
        "cursor": 0,
        "queue": {},
        "active_match": null,
        "receipts": [],
        "holder": {
            "access_token": ECHOED_ACCESS,
            "match_token": MATCH_TOKEN,
            "user_code": USER_CODE,
        },
    })
}

/// Mint a bearer. `refresh` is returned only by the device-code grant: a
/// refresh grant that rotates the credential is refused by the client.
fn mint(fixture: &Fixture, refresh: Option<&str>) -> (StatusCode, Value) {
    let ordinal = fixture.issued.fetch_add(1, Ordering::SeqCst) + 1;
    let mut body = json!({
        "access_token": format!("ACCESS-{ordinal}"),
        "expires_in": 3600,
        "token_type": "Bearer",
    });
    if let Some(refresh) = refresh {
        body["refresh_token"] = json!(refresh);
    }
    (StatusCode::OK, body)
}

/// Decide one response, recording anything the test needs later. `delay` is the
/// long-poll the events route owes its caller.
fn decide(
    fixture: &Fixture,
    method: &Method,
    path: &str,
    bearer: Option<&str>,
    request: &Value,
) -> (StatusCode, Value, Option<Duration>) {
    match (method.as_str(), path) {
        ("GET", "/modes") => (
            StatusCode::OK,
            json!({ "modes": [{ "name": "umvc3_1v1", "queue_enabled": true }] }),
            None,
        ),
        ("POST", "/api/device/start") => {
            if !fixture.device_login.load(Ordering::SeqCst) {
                return (StatusCode::NOT_FOUND, json!({}), None);
            }
            let origin = fixture.origin();
            (
                StatusCode::OK,
                json!({
                    "device_code": DEVICE_CODE,
                    "user_code": USER_CODE,
                    "verification_uri": format!("{origin}/device"),
                    "verification_uri_complete": format!("{origin}/device?code={USER_CODE}"),
                    "expires_in": 30,
                    "interval": 1,
                }),
                None,
            )
        }
        ("POST", "/api/device/token") => match request["grant_type"].as_str() {
            Some("device_code") => {
                fixture
                    .poll_times
                    .lock()
                    .expect("record the poll")
                    .push(Instant::now());
                let scripted = fixture
                    .poll_script
                    .lock()
                    .expect("read the poll script")
                    .pop_front();
                match scripted {
                    Some(error) => (StatusCode::BAD_REQUEST, json!({ "error": error }), None),
                    None => {
                        let (status, body) = mint(fixture, Some(LOGIN_REFRESH));
                        (status, body, None)
                    }
                }
            }
            Some("refresh_token") => {
                fixture.grants.fetch_add(1, Ordering::SeqCst);
                match *fixture.refresh.lock().expect("read the refresh reply") {
                    Refresh::Issue => {
                        let (status, body) = mint(fixture, None);
                        (status, body, None)
                    }
                    Refresh::InvalidGrant => (
                        StatusCode::BAD_REQUEST,
                        json!({ "error": "invalid_grant" }),
                        None,
                    ),
                    Refresh::Unavailable => (
                        StatusCode::SERVICE_UNAVAILABLE,
                        json!({ "error": "unavailable" }),
                        None,
                    ),
                }
            }
            _ => (
                StatusCode::BAD_REQUEST,
                json!({ "error": "unsupported_grant_type" }),
                None,
            ),
        },
        ("POST", "/api/device/logout") => (StatusCode::NO_CONTENT, Value::Null, None),
        ("POST", "/api/logout") => {
            if let Some(bearer) = bearer {
                fixture
                    .revoked
                    .lock()
                    .expect("revoke the bearer")
                    .insert(bearer.to_string());
            }
            (StatusCode::NO_CONTENT, Value::Null, None)
        }
        ("POST", "/auth/test-token") => (StatusCode::OK, json!({ "token": BROWSER_TOKEN }), None),
        ("POST", other) if other.starts_with("/api/device/") && other.ends_with("/approve") => (
            *fixture.approve.lock().expect("read the approve reply"),
            json!({ "error": "steam_required" }),
            None,
        ),
        ("GET", "/api/ranked/state") => match bearer {
            Some(bearer) if fixture.valid(bearer) => (StatusCode::OK, state_body(), None),
            _ => (
                StatusCode::UNAUTHORIZED,
                json!({ "error": "native_session_required" }),
                None,
            ),
        },
        ("GET", "/api/events") => match bearer {
            Some(bearer) if fixture.valid(bearer) => (
                StatusCode::OK,
                json!({ "events": [], "next_cursor": 0 }),
                Some(Duration::from_millis(500)),
            ),
            _ => (
                StatusCode::UNAUTHORIZED,
                json!({ "error": "native_session_required" }),
                None,
            ),
        },
        ("POST", "/api/command") => {
            if fixture.command_401.load(Ordering::SeqCst) > 0 {
                fixture.command_401.fetch_sub(1, Ordering::SeqCst);
                return (
                    StatusCode::UNAUTHORIZED,
                    json!({ "error": "native_session_required" }),
                    None,
                );
            }
            match bearer {
                Some(bearer) if fixture.valid(bearer) => (
                    StatusCode::ACCEPTED,
                    json!({ "receipt": "rcpt-1", "status": "pending" }),
                    None,
                ),
                _ => (
                    StatusCode::UNAUTHORIZED,
                    json!({ "error": "native_session_required" }),
                    None,
                ),
            }
        }
        _ => (StatusCode::NOT_FOUND, json!({}), None),
    }
}

fn render(status: StatusCode, body: Value) -> Response {
    if status == StatusCode::NO_CONTENT {
        return status.into_response();
    }
    let bytes = serde_json::to_vec(&body).unwrap_or_default();
    (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
}

async fn answer(
    State(fixture): State<Arc<Fixture>>,
    method: Method,
    uri: Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.trim_start_matches("Bearer ").to_string());
    let path = uri.path().to_string();
    let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
    let (status, body, delay) = decide(&fixture, &method, &path, bearer.as_deref(), &request);
    fixture.hits.lock().expect("record the request").push(Hit {
        path,
        bearer,
        body: request,
        status,
    });
    if let Some(delay) = delay {
        tokio::time::sleep(delay).await;
    }
    render(status, body)
}

/// A loopback mock of the device contract, on its own thread and port, so no
/// test can see another's requests.
struct Mock {
    fixture: Arc<Fixture>,
    base: String,
}

impl Mock {
    fn start() -> Self {
        let fixture = Arc::new(Fixture {
            base: OnceLock::new(),
            grants: AtomicUsize::new(0),
            issued: AtomicUsize::new(0),
            revoked: Mutex::new(HashSet::new()),
            poll_script: Mutex::new(VecDeque::new()),
            poll_times: Mutex::new(Vec::new()),
            refresh: Mutex::new(Refresh::Issue),
            device_login: AtomicBool::new(true),
            command_401: AtomicUsize::new(0),
            approve: Mutex::new(StatusCode::FORBIDDEN),
            hits: Mutex::new(Vec::new()),
        });
        let served = Arc::clone(&fixture);
        let (ready, waiting) = mpsc::channel();
        std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("build a runtime for the mock");
            runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("bind a loopback port");
                let addr = listener.local_addr().expect("read the bound address");
                served.base.set(format!("http://{addr}")).ok();
                ready.send(()).expect("hand back control");
                let app = Router::new().fallback(answer).with_state(served);
                let _ = axum::serve(listener, app).await;
            });
        });
        waiting.recv().expect("the mock came up");
        let base = fixture.origin();
        Self { fixture, base }
    }

    fn base(&self) -> &str {
        &self.base
    }

    /// Answer the next device-code polls with these error ids, in order.
    fn script(&self, ids: &[&'static str]) {
        self.fixture
            .poll_script
            .lock()
            .expect("write the poll script")
            .extend(ids.iter().copied());
    }

    fn without_device_login(&self) {
        self.fixture.device_login.store(false, Ordering::SeqCst);
    }

    /// Refuse the next `/api/command` whatever bearer it carries.
    fn refuse_next_command(&self) {
        self.fixture.command_401.store(1, Ordering::SeqCst);
    }

    fn set_refresh(&self, next: Refresh) {
        *self.fixture.refresh.lock().expect("write the reply") = next;
    }

    fn revoke(&self, bearer: &str) {
        self.fixture
            .revoked
            .lock()
            .expect("revoke the bearer")
            .insert(bearer.to_string());
    }

    fn grants(&self) -> usize {
        self.fixture.grants.load(Ordering::SeqCst)
    }

    fn polls(&self) -> Vec<Instant> {
        self.fixture
            .poll_times
            .lock()
            .expect("read the polls")
            .clone()
    }

    fn hits(&self) -> Vec<Hit> {
        self.fixture.hits.lock().expect("read the hits").clone()
    }

    fn total(&self) -> usize {
        self.fixture.hits.lock().expect("count the hits").len()
    }

    fn count(&self, path: &str) -> usize {
        self.hits().iter().filter(|hit| hit.path == path).count()
    }

    fn status_count(&self, status: StatusCode) -> usize {
        self.hits()
            .iter()
            .filter(|hit| hit.status == status)
            .count()
    }

    fn count_with_bearer(&self, bearer: &str) -> usize {
        self.hits()
            .iter()
            .filter(|hit| hit.bearer.as_deref() == Some(bearer))
            .count()
    }

    fn status(&self, path: &str, bearer: &str) -> Option<StatusCode> {
        self.hits()
            .iter()
            .find(|hit| hit.path == path && hit.bearer.as_deref() == Some(bearer))
            .map(|hit| hit.status)
    }

    fn body(&self, path: &str, key: &str) -> Option<Value> {
        self.hits()
            .iter()
            .find(|hit| hit.path == path)
            .map(|hit| hit.body[key].clone())
    }

    fn bearers(&self) -> Vec<String> {
        self.hits()
            .iter()
            .filter_map(|hit| hit.bearer.clone())
            .collect()
    }
}

/// A unique directory in the system temp dir, removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("umvc3-device-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create the scratch directory");
        Self(dir)
    }

    fn ini(&self) -> PathBuf {
        self.0.join("client.ini")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Write a session file the way `login` leaves one: 0600, `[Settings]`.
fn seed(path: &Path, base: &str, refresh: &str) {
    let text = format!("[Settings]\nRankedRefreshToken={refresh}\nRankedBaseUrl={base}\n");
    std::fs::write(path, text).expect("write the session file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("make the session file private");
    }
}

/// The client binary, running with piped stdio, and its transcript.
struct Run {
    child: Child,
    stdin: Option<ChildStdin>,
    lines: Arc<Mutex<Vec<String>>>,
    errors: Arc<Mutex<String>>,
    readers: Vec<std::thread::JoinHandle<()>>,
    argv: Vec<String>,
}

impl Run {
    /// Run the client with nothing on stdin: for anything that is not the shell.
    fn once(argv: &[String]) -> Self {
        Self::spawn(argv, Stdio::null())
    }

    /// Run the interactive shell with stdin piped in.
    fn shell(argv: &[String]) -> Self {
        Self::spawn(argv, Stdio::piped())
    }

    fn spawn(argv: &[String], stdin: Stdio) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_umvc3-client"))
            .args(argv)
            .stdin(stdin)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap_or_else(|error| panic!("spawn the client with {argv:?}: {error}"));
        let stdin = child.stdin.take();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let errors = Arc::new(Mutex::new(String::new()));
        let mut readers = Vec::new();

        let stdout = child.stdout.take().expect("capture stdout");
        let sink = Arc::clone(&lines);
        readers.push(std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                match line {
                    Ok(line) => sink.lock().expect("stdout").push(line),
                    Err(_) => break,
                }
            }
        }));
        let stderr = child.stderr.take().expect("capture stderr");
        let sink = Arc::clone(&errors);
        readers.push(std::thread::spawn(move || {
            for line in BufReader::new(stderr).lines() {
                match line {
                    Ok(line) => {
                        sink.lock().expect("stderr").push_str(&line);
                        sink.lock().expect("stderr").push('\n');
                    }
                    Err(_) => break,
                }
            }
        }));
        Self {
            child,
            stdin,
            lines,
            errors,
            readers,
            argv: argv.to_vec(),
        }
    }

    /// Type one command into the shell.
    fn send(&mut self, line: &str) {
        if let Some(stdin) = self.stdin.as_mut() {
            let _ = writeln!(stdin, "{line}");
            let _ = stdin.flush();
        }
    }

    fn transcript(&self) -> String {
        let lines = self.lines.lock().expect("stdout");
        let errors = self.errors.lock().expect("stderr");
        format!("{}{}", lines.join("\n"), errors.as_str())
    }

    fn count(&self, needle: &str) -> usize {
        self.lines
            .lock()
            .expect("stdout")
            .iter()
            .filter(|line| line.contains(needle))
            .count()
    }

    /// Wait until the transcript has said something, or fail with it.
    fn expect_line(&mut self, needle: &str) -> usize {
        self.expect_lines(needle, 1)
    }

    /// Wait until the transcript has said something exactly `want` times.
    fn expect_lines(&mut self, needle: &str, want: usize) -> usize {
        let what = format!("{want}x {needle:?} in the transcript");
        until(&what, || self.count(needle) >= want);
        self.count(needle)
    }

    /// Close stdin and wait for the client to exit on its own.
    fn finish(mut self) -> (ExitStatus, String) {
        drop(self.stdin.take());
        let deadline = Instant::now() + PATIENCE;
        let status = loop {
            match self.child.try_wait().expect("poll the client") {
                Some(status) => break status,
                None if Instant::now() > deadline => {
                    let _ = self.child.kill();
                    let _ = self.child.wait();
                    panic!(
                        "the client did not exit after being run with {:?}; transcript:\n{}",
                        self.argv,
                        self.transcript()
                    );
                }
                None => std::thread::sleep(Duration::from_millis(20)),
            }
        };
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
        (status, self.transcript())
    }
}

impl Drop for Run {
    fn drop(&mut self) {
        // A panicking test must not leave the client running.
        let _ = self.child.kill();
    }
}

/// Poll `condition` until it holds, so no test waits on a wall clock.
fn until(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + PATIENCE;
    loop {
        if condition() {
            return;
        }
        if Instant::now() > deadline {
            panic!("timed out waiting for {what}");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn argv(parts: &[&str]) -> Vec<String> {
    parts.iter().map(|part| (*part).to_string()).collect()
}

/// `umvc3-client login --token-file <file> --base-url <base>`.
fn login_argv(file: &Path, base: &str) -> Vec<String> {
    argv(&[
        "login",
        "--token-file",
        &file.display().to_string(),
        "--base-url",
        base,
    ])
}

/// The shell, driving one player off a saved session.
fn shell_argv(file: &Path) -> Vec<String> {
    argv(&[
        "--token-file",
        &file.display().to_string(),
        "--single",
        "--interactive",
        "--tick",
        "1",
    ])
}

fn absent(transcript: &str, needle: &str, why: &str) {
    assert!(
        !transcript.contains(needle),
        "{why}, but {needle} appears in:\n{transcript}"
    );
}

fn ini_text(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()))
}

#[cfg(unix)]
fn mode(path: &Path) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    std::fs::metadata(path).expect("stat").permissions().mode() & 0o777
}

// ── tests ────────────────────────────────────────────────────────────────

#[test]
fn login_polls_through_pending_and_slow_down_then_saves_the_credential() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    mock.script(&["authorization_pending", "slow_down"]);

    let (status, transcript) = Run::once(&login_argv(&file, mock.base())).finish();

    assert!(status.success(), "login failed:\n{transcript}");
    // The human is shown what to type and where; the secret half is not shown.
    assert!(transcript.contains(USER_CODE), "{transcript}");
    assert!(
        transcript.contains(&format!("{}/device?code={USER_CODE}", mock.base())),
        "{transcript}"
    );
    absent(&transcript, DEVICE_CODE, "the device code is a secret");
    absent(
        &transcript,
        LOGIN_REFRESH,
        "the saved credential is a secret",
    );

    let saved = ini_text(&file);
    assert!(
        saved.contains(&format!("RankedRefreshToken={LOGIN_REFRESH}")),
        "{saved}"
    );
    assert!(
        saved.contains(&format!("RankedBaseUrl={}", mock.base())),
        "{saved}"
    );
    #[cfg(unix)]
    assert_eq!(mode(&file), 0o600, "the credential file is private");

    // One poll per scripted answer, then the issued grant.
    let polls = mock.polls();
    assert_eq!(polls.len(), 3, "the grant was polled exactly three times");
    let pending_gap = polls[1] - polls[0];
    let slow_down_gap = polls[2] - polls[1];
    assert!(
        pending_gap >= Duration::from_millis(900) && pending_gap < Duration::from_secs(4),
        "a pending answer leaves the server's interval alone: {pending_gap:?}"
    );
    assert!(
        slow_down_gap >= Duration::from_secs(4),
        "slow_down has to push the next poll out by five more seconds: {slow_down_gap:?}"
    );

    // Device login never falls back to a dev ticket, and needs no other route.
    assert_eq!(mock.count("/api/ticket"), 0, "no dev-ticket fallback");
    assert_eq!(mock.count("/api/device/start"), 1);
}

#[test]
fn a_command_refused_with_401_is_replayed_verbatim_with_a_fresh_bearer() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    seed(&file, mock.base(), SEED_REFRESH);
    mock.refuse_next_command();

    let mut run = Run::shell(&shell_argv(&file));
    run.expect_line("type 'help' for the full command list");
    run.send("heartbeat");
    run.expect_line("= heartbeat 202");

    let commands: Vec<Hit> = mock
        .hits()
        .into_iter()
        .filter(|hit| hit.path == "/api/command")
        .collect();
    assert_eq!(commands.len(), 2, "one refusal and exactly one replay");
    assert_eq!(commands[0].status, StatusCode::UNAUTHORIZED);
    assert_eq!(commands[1].status, StatusCode::ACCEPTED);
    assert_eq!(
        commands[0].body, commands[1].body,
        "the replay is the same request, command_id included"
    );
    assert!(
        commands[0].body["command_id"]
            .as_str()
            .is_some_and(|id| !id.is_empty()),
        "a command carries an id to be replayed under"
    );
    assert_eq!(commands[0].bearer.as_deref(), Some("ACCESS-1"));
    assert_eq!(
        commands[1].bearer.as_deref(),
        Some("ACCESS-2"),
        "the replay carries the refreshed bearer"
    );
    assert_eq!(mock.grants(), 2);

    run.send("quit");
    let (status, transcript) = run.finish();
    assert!(status.success(), "{transcript}");
}

#[test]
fn a_refresh_that_fails_transiently_is_not_a_dead_session() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    seed(&file, mock.base(), SEED_REFRESH);

    let mut run = Run::shell(&shell_argv(&file));
    run.expect_line("type 'help' for the full command list");
    // The bearer dies while the token endpoint is unavailable: the run has to
    // say which of the two failed, and stop hammering the server meanwhile.
    mock.set_refresh(Refresh::Unavailable);
    mock.revoke("ACCESS-1");

    run.expect_line("background read failed (refresh)");
    until("three refused reads", || {
        mock.status_count(StatusCode::UNAUTHORIZED) >= 3
    });
    assert_eq!(
        run.count("session unauthorized (401)"),
        0,
        "a failed refresh is not an unauthorized session:\n{}",
        run.transcript()
    );
    assert_eq!(mock.grants(), 2, "the cooldown keeps retries off the wire");

    run.send("quit");
    let (status, transcript) = run.finish();
    assert!(status.success(), "{transcript}");
}

#[test]
fn a_saved_credential_resumes_the_session_without_a_browser() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    seed(&file, mock.base(), SEED_REFRESH);

    let mut run = Run::shell(&shell_argv(&file));
    run.expect_line("type 'help' for the full command list");
    until("a read carried a refreshed bearer", || {
        !mock.bearers().is_empty()
    });

    let transcript = run.transcript();
    assert_eq!(
        mock.count("/api/device/start"),
        0,
        "a later run must not start a new grant:\n{transcript}"
    );
    assert_eq!(mock.count("/api/ticket"), 0, "no dev-ticket fallback");
    assert_eq!(mock.grants(), 1, "the saved credential was exchanged once");
    absent(
        &transcript,
        SEED_REFRESH,
        "the saved credential is a secret",
    );
    absent(
        &transcript,
        "session unauthorized",
        "the session kept working",
    );
    // A snapshot the server echoed is printed, with its credential redacted and
    // the identifiers the human asked for left readable.
    assert!(
        transcript.contains(MATCH_TOKEN),
        "the state read was printed:\n{transcript}"
    );
    absent(
        &transcript,
        ECHOED_ACCESS,
        "a credential echoed by the server is redacted",
    );

    run.send("quit");
    let (status, transcript) = run.finish();
    assert!(status.success(), "{transcript}");
}

#[test]
fn a_second_login_is_refused_without_asking_the_server() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    seed(&file, mock.base(), SEED_REFRESH);
    let before = ini_text(&file);

    let (status, transcript) = Run::once(&login_argv(&file, mock.base())).finish();

    assert!(!status.success(), "a second login must fail:\n{transcript}");
    assert!(transcript.contains("already linked"), "{transcript}");
    assert_eq!(mock.total(), 0, "the refusal is settled before any request");
    assert_eq!(ini_text(&file), before, "the saved session is untouched");
    #[cfg(unix)]
    assert_eq!(mode(&file), 0o600);
}

#[test]
fn an_origin_that_does_not_match_the_saved_one_is_refused_before_any_request() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    // The same server reached under another name: if the guard were missing,
    // the credential would travel to an origin that never issued it.
    let alias = mock.base().replace("127.0.0.1", "localhost");
    seed(&file, &alias, SEED_REFRESH);

    // Not a login: a login with a saved session stops at "already linked"
    // before the origin is ever compared.
    let run = Run::once(&argv(&[
        "--token-file",
        &file.display().to_string(),
        "--base-url",
        mock.base(),
        "--single",
        // If this guard regressed, the run has to fail fast rather than sit in
        // scripted mode waiting for a matchmaker that is not there.
        "--stage-timeout",
        "1",
        "--pair-timeout",
        "1",
    ]));
    let (status, transcript) = run.finish();

    assert!(!status.success(), "{transcript}");
    assert!(
        transcript.contains("refusing to send its credential"),
        "{transcript}"
    );
    assert!(
        transcript.contains(&alias) && transcript.contains(mock.base()),
        "both origins are named so the mismatch is fixable:\n{transcript}"
    );
    assert_eq!(
        mock.total(),
        0,
        "the credential must not be offered to a server it is not bound to"
    );
    absent(
        &transcript,
        SEED_REFRESH,
        "the saved credential is a secret",
    );
}

#[test]
fn an_unattended_run_without_a_saved_session_says_how_to_get_one() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();

    // No terminal on stdin and no --interactive: nobody can approve a browser
    // prompt, so the run has to stop with something actionable.
    let run = Run::once(&argv(&[
        "--token-file",
        &file.display().to_string(),
        "--single",
        // As above: a regression should fail in a second, not in a minute.
        "--stage-timeout",
        "1",
        "--pair-timeout",
        "1",
    ]));
    let (status, transcript) = run.finish();

    assert!(!status.success(), "{transcript}");
    assert!(
        transcript.contains(&format!("run login --token-file {} first", file.display())),
        "{transcript}"
    );
    assert_eq!(mock.total(), 0);
    assert!(!file.exists(), "nothing was written");
}

#[test]
fn a_401_on_a_startup_read_is_answered_by_one_refresh() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    seed(&file, mock.base(), SEED_REFRESH);
    // Whatever bearer the saved credential mints first is already dead.
    mock.revoke("ACCESS-1");

    let mut run = Run::shell(&shell_argv(&file));
    run.expect_line("type 'help' for the full command list");

    let transcript = run.transcript();
    assert_eq!(
        mock.grants(),
        2,
        "one exchange to resume and one to answer the 401:\n{transcript}"
    );
    assert_eq!(
        mock.count_with_bearer("ACCESS-1"),
        1,
        "the dead bearer is used once and then replaced"
    );
    assert_eq!(
        mock.status("/api/ranked/state", "ACCESS-1"),
        Some(StatusCode::UNAUTHORIZED)
    );
    assert!(
        mock.bearers().iter().any(|bearer| bearer == "ACCESS-2"),
        "the retry carried the fresh bearer"
    );
    absent(
        &transcript,
        "session unauthorized",
        "a 401 this session can answer stays silent",
    );

    run.send("quit");
    let (status, transcript) = run.finish();
    assert!(status.success(), "{transcript}");
}

#[test]
fn logout_leaves_the_401_visible_until_an_explicit_refresh() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    seed(&file, mock.base(), SEED_REFRESH);

    let mut run = Run::shell(&shell_argv(&file));
    run.expect_line("type 'help' for the full command list");
    until("the session resumed", || mock.grants() == 1);

    run.send("logout");
    run.expect_line("= logout");
    run.expect_line("session unauthorized (401)");
    // Several refused reads later, the transcript still says it once.
    until("three refused reads", || {
        mock.status_count(StatusCode::UNAUTHORIZED) >= 3
    });
    assert_eq!(
        run.count("session unauthorized (401)"),
        1,
        "a dead session reads as one line, not one per poll:\n{}",
        run.transcript()
    );
    assert_eq!(
        mock.grants(),
        1,
        "a paused session must not refresh behind the user's back"
    );

    run.send("refresh");
    run.expect_line("= refreshed; automatic refresh resumed");
    run.expect_lines("session recovered", 1);
    assert_eq!(mock.grants(), 2, "an explicit refresh is one exchange");

    run.send("revoke-device");
    run.expect_line("device revoked on the server and cleared from its token file");
    assert_eq!(mock.count("/api/device/logout"), 1);
    assert_eq!(
        mock.body("/api/device/logout", "refresh_token"),
        Some(json!(SEED_REFRESH))
    );
    let saved = ini_text(&file);
    assert!(saved.contains("RankedRefreshToken=\n"), "{saved}");
    assert!(
        saved.contains(&format!("RankedBaseUrl={}", mock.base())),
        "{saved}"
    );
    #[cfg(unix)]
    assert_eq!(mode(&file), 0o600, "clearing keeps the file private");
    absent(
        &run.transcript(),
        SEED_REFRESH,
        "the credential is never printed",
    );

    run.send("quit");
    let (status, transcript) = run.finish();
    assert!(status.success(), "{transcript}");
}

#[test]
fn a_refresh_the_server_refuses_as_invalid_grant_clears_the_file() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    seed(&file, mock.base(), SEED_REFRESH);

    let mut run = Run::shell(&shell_argv(&file));
    run.expect_line("type 'help' for the full command list");
    until("the session resumed", || mock.grants() >= 1);
    // The grant is revoked server-side from now on.
    mock.set_refresh(Refresh::InvalidGrant);

    run.send("refresh");

    run.expect_line("device grant revoked or expired");
    run.expect_line(&format!("run login --token-file {}", file.display()));
    assert_eq!(
        mock.grants(),
        2,
        "a dead grant is tried once, not repeatedly"
    );
    let saved = ini_text(&file);
    assert!(saved.contains("RankedRefreshToken=\n"), "{saved}");
    assert!(
        saved.contains(&format!("RankedBaseUrl={}", mock.base())),
        "{saved}"
    );
    absent(
        &run.transcript(),
        SEED_REFRESH,
        "the credential is never printed",
    );

    run.send("quit");
    let (status, transcript) = run.finish();
    assert!(status.success(), "{transcript}");
}

#[test]
fn a_refused_login_saves_nothing() {
    let cases: &[(&str, &[&'static str], &str)] = &[
        ("access_denied", &["access_denied"], "denied in the browser"),
        ("expired_token", &["expired_token"], "expired"),
        ("invalid_grant", &["invalid_grant"], "no longer recognises"),
    ];
    for (name, script, expected) in cases {
        let scratch = Scratch::new();
        let file = scratch.ini();
        let mock = Mock::start();
        mock.script(script);

        let (status, transcript) = Run::once(&login_argv(&file, mock.base())).finish();

        assert!(!status.success(), "{name} must not log in:\n{transcript}");
        assert!(transcript.contains(expected), "{name}: {transcript}");
        assert!(!file.exists(), "{name}: a refused login wrote a file");
        assert_eq!(
            mock.count("/api/ticket"),
            0,
            "{name}: no dev-ticket fallback"
        );
    }
}

#[test]
fn a_server_without_device_login_says_so_and_never_falls_back_to_a_ticket() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    mock.without_device_login();

    let (status, transcript) = Run::once(&login_argv(&file, mock.base())).finish();

    assert!(!status.success(), "{transcript}");
    assert!(
        transcript.contains("device login is not available on this server"),
        "{transcript}"
    );
    assert_eq!(
        mock.count("/api/ticket"),
        0,
        "a 404 must not mean 'use a dev ticket'"
    );
    assert!(!file.exists());
}

#[test]
fn dev_approve_reports_a_steam_required_refusal_without_leaking_the_browser_token() {
    let scratch = Scratch::new();
    let file = scratch.ini();
    let mock = Mock::start();
    let mut options = login_argv(&file, mock.base());
    options.push("--dev-approve".to_string());
    options.extend(argv(&["--steam-id", "76561198000000101"]));

    let (status, transcript) = Run::once(&options).finish();

    assert!(!status.success(), "{transcript}");
    assert!(transcript.contains("steam_required"), "{transcript}");
    absent(
        &transcript,
        BROWSER_TOKEN,
        "a browser credential is never printed",
    );
    assert!(!file.exists(), "nothing was saved");
    assert_eq!(
        mock.body("/auth/test-token", "steam_id"),
        Some(json!(76_561_198_000_000_101u64))
    );
    let approve = mock
        .hits()
        .into_iter()
        .find(|hit| hit.path.ends_with("/approve"))
        .expect("the grant was approved as the local user");
    assert_eq!(approve.path, format!("/api/device/{USER_CODE}/approve"));
    assert_eq!(approve.bearer.as_deref(), Some(BROWSER_TOKEN));
}
