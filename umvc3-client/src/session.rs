//! A native session that outlives any one bearer.
//!
//! A dev session is one `dev:<steam_id>` ticket exchange and never refreshes. A
//! device session holds the durable credential a browser approval produced and
//! trades it for fresh native bearers as they near expiry, so a long run — and
//! every clone a background reader holds — keeps working without a browser.
//!
//! Every clone shares one state behind an async mutex, which is what makes a
//! refresh happen once however many readers notice the expiry together.

use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use reqwest::StatusCode;
use tokio::sync::Mutex;

use crate::api::{self, Api, DevicePoll, IssuedAccess, RefreshOutcome, Transient};
use crate::{ini, log};

/// Refresh this long before expiry at most; half the lifetime when shorter.
const REFRESH_LEAD_CAP: Duration = Duration::from_secs(300);

/// After a failed refresh, automatic attempts wait this long.
const REFRESH_COOLDOWN: Duration = Duration::from_secs(5);

/// How far one `slow_down` pushes the poll interval out, and its ceiling.
const SLOW_DOWN_STEP: u64 = 5;
const MAX_POLL_INTERVAL: u64 = 60;

/// Marks an error as a failed refresh rather than a failed request, so the
/// shell can say which one happened without parsing a message.
#[derive(Debug)]
pub struct RefreshFailed;

impl fmt::Display for RefreshFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("refreshing the device session failed")
    }
}

#[derive(Clone)]
pub struct Session {
    inner: Arc<Mutex<SessionState>>,
    api: Api,
}

struct SessionState {
    bearer: String,
    /// When the bearer stops working, measured from after the response.
    expires_at: Option<Instant>,
    /// When to start refreshing ahead of `expires_at`.
    refresh_at: Option<Instant>,
    /// The durable device credential; `None` for a dev session or once revoked.
    refresh: Option<String>,
    /// Where the credential is saved; `None` for a dev session.
    file: Option<PathBuf>,
    /// Set by an access-only logout: automatic refresh stays off until an
    /// explicit refresh, so the revoked bearer's 401s stay observable.
    paused: bool,
    next_refresh_attempt: Option<Instant>,
}

impl SessionState {
    fn install(&mut self, access_token: String, expires_in: u64) {
        let now = Instant::now();
        let lifetime = Duration::from_secs(expires_in);
        self.bearer = access_token;
        self.expires_at = Some(now + lifetime);
        self.refresh_at = Some(now + lifetime - REFRESH_LEAD_CAP.min(lifetime / 2));
        self.next_refresh_attempt = None;
    }

    fn relogin_hint(&self) -> String {
        match &self.file {
            Some(file) => format!("run login --token-file {}", file.display()),
            None => "run login --token-file <PATH>".to_string(),
        }
    }
}

impl Session {
    fn from_state(api: Api, state: SessionState) -> Self {
        Self {
            inner: Arc::new(Mutex::new(state)),
            api,
        }
    }

    /// Exchange the synthetic `dev:<steam_id>` ticket for a native bearer.
    pub async fn dev(api: Api, steam_id: u64) -> Result<Self> {
        let bearer = api.native_ticket(&format!("dev:{steam_id}")).await?;
        let state = SessionState {
            bearer,
            expires_at: None,
            refresh_at: None,
            refresh: None,
            file: None,
            paused: false,
            next_refresh_attempt: None,
        };
        Ok(Self::from_state(api, state))
    }

    /// Resume the device credential saved in `file` — no browser involved.
    pub async fn device(api: Api, file: &Path) -> Result<Self> {
        let stored = ini::load(file)?.with_context(|| {
            format!(
                "no saved device session in {}; run login --token-file {} first",
                file.display(),
                file.display()
            )
        })?;
        let bound = api::device_origin(&stored.base)
            .with_context(|| format!("{}: RankedBaseUrl", file.display()))?;
        let target = api::device_origin(api.base())?;
        if bound != target {
            bail!(
                "{} is bound to a different server; refusing to send its credential to it",
                file.display()
            );
        }
        let state = SessionState {
            bearer: String::new(),
            expires_at: None,
            refresh_at: None,
            refresh: Some(stored.refresh),
            file: Some(file.to_path_buf()),
            paused: false,
            next_refresh_attempt: None,
        };
        let session = Self::from_state(api, state);
        {
            let mut state = session.inner.lock().await;
            session.refresh_locked(&mut state).await?;
        }
        Ok(session)
    }

    /// The bearer to send now, refreshed first when it is close to expiry.
    pub async fn access(&self) -> Result<String> {
        let mut state = self.inner.lock().await;
        if state.paused || state.refresh.is_none() {
            return Ok(state.bearer.clone());
        }
        let now = Instant::now();
        if state.refresh_at.is_some_and(|at| now < at) {
            return Ok(state.bearer.clone());
        }
        let still_valid = state.expires_at.is_some_and(|at| now < at);
        if state.next_refresh_attempt.is_some_and(|at| now < at) {
            if still_valid {
                return Ok(state.bearer.clone());
            }
            return Err(anyhow!(
                "the native bearer expired and the last refresh failed moments ago"
            )
            .context(RefreshFailed));
        }
        match self.refresh_locked(&mut state).await {
            Ok(()) => Ok(state.bearer.clone()),
            // A failed early refresh is not yet a failure: the bearer in hand
            // still works, and the cooldown spaces out the next attempt.
            Err(_) if still_valid && state.refresh.is_some() => Ok(state.bearer.clone()),
            Err(error) => Err(error),
        }
    }

    /// Called after a 401 on a request made with `observed`. True when the
    /// caller should retry once: a fresh bearer is now installed.
    pub async fn refresh_after_401(&self, observed: &str) -> Result<bool> {
        let mut state = self.inner.lock().await;
        if state.bearer != observed {
            // Another caller refreshed while this request was in flight.
            return Ok(true);
        }
        if state.paused || state.refresh.is_none() {
            return Ok(false);
        }
        if state
            .next_refresh_attempt
            .is_some_and(|at| Instant::now() < at)
        {
            return Err(anyhow!(
                "the native bearer was refused and the last refresh failed moments ago"
            )
            .context(RefreshFailed));
        }
        self.refresh_locked(&mut state).await?;
        Ok(true)
    }

    /// Force one refresh now, ignoring the cooldown. Resumes a paused session
    /// only when it succeeds.
    pub async fn refresh_now(&self) -> Result<()> {
        let mut state = self.inner.lock().await;
        if state.refresh.is_none() {
            if state.file.is_none() {
                bail!("no refresh credential; use a device session");
            }
            bail!("no refresh credential; {}", state.relogin_hint());
        }
        self.refresh_locked(&mut state).await?;
        state.paused = false;
        Ok(())
    }

    /// `POST /api/logout` with the current bearer: ends this access session
    /// only. The saved device credential survives; automatic refresh pauses so
    /// the dead bearer's 401s can be observed until an explicit refresh.
    pub async fn logout_access(&self) -> Result<StatusCode> {
        let mut state = self.inner.lock().await;
        let status = self.api.logout(&state.bearer).await?;
        if status == StatusCode::NO_CONTENT {
            state.paused = true;
        }
        Ok(status)
    }

    /// Revoke the durable device credential on the server, then clear it from
    /// the ini. A revoked credential is dropped from memory even when clearing
    /// the file fails, so it is never sent again.
    pub async fn revoke_device(&self) -> Result<()> {
        let mut state = self.inner.lock().await;
        let Some(refresh) = state.refresh.clone() else {
            bail!("no device credential");
        };
        self.api.device_revoke(&refresh).await?;
        state.paused = true;
        state.refresh = None;
        if let Some(file) = state.file.clone() {
            ini::clear(&file).with_context(|| {
                format!(
                    "the device was revoked on the server, but clearing {} failed",
                    file.display()
                )
            })?;
        }
        Ok(())
    }

    /// One device refresh with the lock held, so concurrent callers queue
    /// behind it rather than refreshing again. Never calls another method
    /// that takes the lock.
    async fn refresh_locked(&self, state: &mut SessionState) -> Result<()> {
        let result = self.exchange(state).await;
        if result.is_err() && state.refresh.is_some() {
            state.next_refresh_attempt = Some(Instant::now() + REFRESH_COOLDOWN);
        }
        result.context(RefreshFailed)
    }

    async fn exchange(&self, state: &mut SessionState) -> Result<()> {
        let refresh = state.refresh.clone().context("no refresh credential")?;
        match self.api.device_refresh(&refresh).await? {
            RefreshOutcome::Issued(IssuedAccess {
                access_token,
                expires_in,
            }) => {
                state.install(access_token, expires_in);
                Ok(())
            }
            RefreshOutcome::InvalidGrant => {
                // Terminal: no retry can succeed, so forget it everywhere.
                state.refresh = None;
                state.next_refresh_attempt = None;
                if let Some(file) = state.file.clone() {
                    ini::clear(&file).with_context(|| {
                        format!(
                            "the device grant is revoked or expired, and clearing {} failed",
                            file.display()
                        )
                    })?;
                }
                bail!("device grant revoked or expired; {}", state.relogin_hint())
            }
        }
    }
}

/// Start a new device grant, show the human where to approve it, wait for the
/// approval, and save the resulting credential to `file`.
///
/// `dev_approve_steam_id` stands in for the browser against a loopback dev
/// server: it approves the grant as that local test user.
pub async fn device_login(
    api: &Api,
    label: &str,
    file: &Path,
    dev_approve_steam_id: Option<u64>,
) -> Result<Session> {
    let origin = api::device_origin(api.base())?;
    let start = api.device_start(label).await?;
    let host = reqwest::Url::parse(&start.verification_uri_complete)
        .ok()
        .and_then(|url| url.host_str().map(str::to_owned))
        .unwrap_or_default();
    log::line(
        label,
        format!(
            "approve this device: open {} and confirm code {}",
            start.verification_uri_complete, start.user_code
        ),
    );
    log::line(
        label,
        format!(
            "    check the address is on {host} before signing in; the code expires in {}s",
            start.expires_in
        ),
    );
    if let Some(steam_id) = dev_approve_steam_id {
        log::line(
            label,
            format!("    --dev-approve: approving as local test user {steam_id}"),
        );
        api.dev_approve(&start.user_code, steam_id).await?;
    }

    let deadline = Instant::now() + Duration::from_secs(start.expires_in);
    let mut interval = start.interval;
    let mut busy = false;
    loop {
        let wait = Duration::from_secs(interval);
        if Instant::now() + wait > deadline {
            bail!("the device code expired before it was approved; run login again");
        }
        tokio::time::sleep(wait).await;
        match api.device_poll(&start.device_code).await {
            Ok(DevicePoll::Pending) => busy = false,
            Ok(DevicePoll::SlowDown) => {
                interval = interval.max((interval + SLOW_DOWN_STEP).min(MAX_POLL_INTERVAL));
            }
            Ok(DevicePoll::Denied) => bail!("the device login was denied in the browser"),
            Ok(DevicePoll::Expired) => {
                bail!("the device code expired before it was approved; run login again")
            }
            Ok(DevicePoll::Invalid) => {
                bail!("the server no longer recognises this device code; run login again")
            }
            Ok(DevicePoll::Issued {
                access_token,
                refresh_token,
                expires_in,
            }) => {
                ini::save(file, &origin, &refresh_token).with_context(|| {
                    format!(
                        "the device was approved, but saving its credential to {} failed",
                        file.display()
                    )
                })?;
                log::line(
                    label,
                    format!(
                        "    approved; device credential saved to {}",
                        file.display()
                    ),
                );
                let mut state = SessionState {
                    bearer: String::new(),
                    expires_at: None,
                    refresh_at: None,
                    refresh: Some(refresh_token),
                    file: Some(file.to_path_buf()),
                    paused: false,
                    next_refresh_attempt: None,
                };
                state.install(access_token, expires_in);
                return Ok(Session::from_state(api.clone(), state));
            }
            Err(error) if error.downcast_ref::<Transient>().is_some() => {
                if !busy {
                    busy = true;
                    log::line(label, format!("    {error:#}; still waiting"));
                }
            }
            Err(error) => return Err(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{Method, Uri, header};
    use axum::response::{IntoResponse, Response};
    use serde_json::{Value, json};

    /// Every refresh grant mints this prefix plus an ordinal.
    const ACCESS: &str = "ACCESS";
    /// A canary, never a real credential.
    const REFRESH: &str = "CANARY-REFRESH-SEED";

    /// What the token endpoint answers with next.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Reply {
        Issue,
        /// A retryable failure: 503.
        Unavailable,
        /// A terminal one: 400 invalid_grant.
        InvalidGrant,
    }

    struct Token {
        reply: Mutex<Reply>,
        /// Refresh grants asked for, answered or not, so "exchanged once" is
        /// checkable even when the exchange fails.
        grants: AtomicUsize,
        /// Bearers actually minted, which is what names them.
        issued: AtomicUsize,
        /// `METHOD /path` for every request, so "nothing was sent" is too.
        hits: Mutex<Vec<String>>,
    }

    fn reply(status: StatusCode, body: Value) -> Response {
        if status == StatusCode::NO_CONTENT {
            return status.into_response();
        }
        let bytes = serde_json::to_vec(&body).unwrap_or_default();
        (status, [(header::CONTENT_TYPE, "application/json")], bytes).into_response()
    }

    async fn answer(
        State(state): State<Arc<Token>>,
        method: Method,
        uri: Uri,
        body: Bytes,
    ) -> Response {
        state
            .hits
            .lock()
            .expect("record the request")
            .push(format!("{method} {}", uri.path()));
        match (method.as_str(), uri.path()) {
            ("POST", "/api/logout") | ("POST", "/api/device/logout") => {
                StatusCode::NO_CONTENT.into_response()
            }
            ("POST", "/api/device/token") => {
                let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
                if request["grant_type"] != json!("refresh_token") {
                    return reply(
                        StatusCode::BAD_REQUEST,
                        json!({ "error": "unsupported_grant_type" }),
                    );
                }
                state.grants.fetch_add(1, Ordering::SeqCst);
                match *state.reply.lock().expect("read the reply") {
                    Reply::Unavailable => reply(
                        StatusCode::SERVICE_UNAVAILABLE,
                        json!({ "error": "unavailable" }),
                    ),
                    Reply::InvalidGrant => {
                        reply(StatusCode::BAD_REQUEST, json!({ "error": "invalid_grant" }))
                    }
                    Reply::Issue => {
                        let ordinal = state.issued.fetch_add(1, Ordering::SeqCst) + 1;
                        reply(
                            StatusCode::OK,
                            json!({
                                "access_token": format!("{ACCESS}-{ordinal}"),
                                "expires_in": 3600,
                                "token_type": "Bearer",
                            }),
                        )
                    }
                }
            }
            _ => reply(StatusCode::NOT_FOUND, json!({})),
        }
    }

    /// A loopback token endpoint plus a scratch directory holding the ini the
    /// session is resumed from.
    struct Mock {
        state: Arc<Token>,
        base: String,
        dir: PathBuf,
    }

    impl Mock {
        async fn start() -> Self {
            let state = Arc::new(Token {
                reply: Mutex::new(Reply::Issue),
                grants: AtomicUsize::new(0),
                issued: AtomicUsize::new(0),
                hits: Mutex::new(Vec::new()),
            });
            let app = Router::new().fallback(answer).with_state(state.clone());
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                .await
                .expect("bind a loopback port");
            let addr = listener.local_addr().expect("read the bound address");
            tokio::spawn(async move {
                let _ = axum::serve(listener, app).await;
            });
            let dir = std::env::temp_dir().join(format!("umvc3-session-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).expect("create the scratch directory");
            Self {
                state,
                base: format!("http://{addr}"),
                dir,
            }
        }

        fn api(&self) -> Api {
            Api::new(&self.base, "P1").expect("build an Api")
        }

        /// The path of the ini a session is saved in, whether or not it exists.
        fn ini(&self) -> PathBuf {
            self.dir.join("client.ini")
        }

        /// Save a device credential for this server, as `login` would.
        fn stored(&self) -> PathBuf {
            let file = self.ini();
            ini::save(&file, &self.base, REFRESH).expect("save the fixture");
            file
        }

        fn set_reply(&self, next: Reply) {
            *self.state.reply.lock().expect("write the reply") = next;
        }

        fn grants(&self) -> usize {
            self.state.grants.load(Ordering::SeqCst)
        }

        fn hits(&self) -> Vec<String> {
            self.state.hits.lock().expect("read the hits").clone()
        }
    }

    impl Drop for Mock {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    /// `Result::expect_err` needs `Debug` on the success type, and `Session` is
    /// deliberately opaque.
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn refreshes_once_for_every_caller_that_missed_the_bearer() {
        let mock = Mock::start().await;
        let session = Session::device(mock.api(), &mock.stored())
            .await
            .expect("resume the saved device session");
        assert_eq!(mock.grants(), 1, "resuming exchanges the credential once");
        let observed = session.access().await.expect("the bearer");

        let mut callers = Vec::new();
        for _ in 0..8 {
            let session = session.clone();
            let observed = observed.clone();
            callers.push(tokio::spawn(async move {
                session.refresh_after_401(&observed).await
            }));
        }
        for caller in callers {
            assert!(
                caller.await.expect("join").expect("a retryable 401"),
                "every caller holding the dead bearer must be told to retry"
            );
        }

        assert_eq!(
            mock.grants(),
            2,
            "however many callers notice the 401, the credential is exchanged once"
        );
        let refreshed = session.access().await.expect("the bearer");
        assert_ne!(refreshed, observed, "the retry must carry a fresh bearer");
        assert_eq!(refreshed, "ACCESS-2");
    }

    #[tokio::test]
    async fn a_failed_refresh_cools_down_and_an_explicit_refresh_bypasses_it() {
        let mock = Mock::start().await;
        let session = Session::device(mock.api(), &mock.stored())
            .await
            .expect("resume the saved device session");
        let observed = session.access().await.expect("the bearer");
        mock.set_reply(Reply::Unavailable);

        let error = session
            .refresh_after_401(&observed)
            .await
            .failed("a 503 is not a fresh bearer");
        assert!(
            error.downcast_ref::<RefreshFailed>().is_some(),
            "the shell has to tell a failed refresh from a failed read: {error:#}"
        );
        assert_eq!(mock.grants(), 2);
        assert_eq!(
            session.access().await.expect("the bearer in hand"),
            observed,
            "a failed early refresh is not yet a failure"
        );

        // The cooldown keeps the next automatic attempt off the wire.
        let error = session
            .refresh_after_401(&observed)
            .await
            .failed("within the cooldown");
        assert!(error.downcast_ref::<RefreshFailed>().is_some(), "{error:#}");
        assert_eq!(mock.grants(), 2, "the cooldown must not hammer the server");

        // An explicit refresh ignores the cooldown.
        mock.set_reply(Reply::Issue);
        session.refresh_now().await.expect("an explicit refresh");
        assert_eq!(mock.grants(), 3);
        assert_eq!(session.access().await.expect("the bearer"), "ACCESS-2");
    }

    #[tokio::test]
    async fn an_invalid_grant_is_terminal_and_clears_the_saved_credential() {
        let mock = Mock::start().await;
        let file = mock.stored();
        let session = Session::device(mock.api(), &file)
            .await
            .expect("resume the saved device session");
        let observed = session.access().await.expect("the bearer");
        mock.set_reply(Reply::InvalidGrant);

        let error = session
            .refresh_after_401(&observed)
            .await
            .failed("a revoked grant");
        let rendered = format!("{error:#}");
        assert!(
            rendered.contains("device grant revoked or expired"),
            "{rendered}"
        );
        assert!(rendered.contains("run login --token-file"), "{rendered}");
        assert!(
            error.downcast_ref::<RefreshFailed>().is_some(),
            "{rendered}"
        );
        assert_eq!(mock.grants(), 2);

        // The credential is gone from the file; its bound origin stays.
        let saved = std::fs::read_to_string(&file).expect("read the ini");
        assert!(saved.contains("RankedRefreshToken=\n"), "{saved}");
        assert!(
            saved.contains(&format!("RankedBaseUrl={}", mock.base)),
            "{saved}"
        );

        // And it is not offered again, however many readers notice.
        let before = mock.hits().len();
        let error = session
            .refresh_now()
            .await
            .failed("there is no credential left");
        assert!(
            format!("{error:#}").contains("no refresh credential"),
            "{error:#}"
        );
        assert_eq!(
            mock.hits().len(),
            before,
            "a dead grant is never sent again"
        );
    }

    #[tokio::test]
    async fn a_credential_is_never_sent_to_a_server_it_is_not_bound_to() {
        let mock = Mock::start().await;
        // The same server, spelled the way the saved file may have it.
        let alias = mock.base.replace("127.0.0.1", "localhost");
        let file = mock.ini();
        ini::save(&file, &alias, REFRESH).expect("save the fixture");

        let error = Session::device(mock.api(), &file)
            .await
            .failed("a foreign origin");

        assert!(
            format!("{error:#}").contains("refusing to send its credential"),
            "{error:#}"
        );
        assert!(
            mock.hits().is_empty(),
            "the credential must not leave the machine: {:?}",
            mock.hits()
        );
    }

    #[tokio::test]
    async fn a_missing_saved_session_says_how_to_make_one() {
        let mock = Mock::start().await;
        let file = mock.ini();

        let error = Session::device(mock.api(), &file)
            .await
            .failed("no saved session");

        let rendered = format!("{error:#}");
        assert!(rendered.contains("run login --token-file"), "{rendered}");
        assert!(rendered.contains(&file.display().to_string()), "{rendered}");
        assert!(mock.hits().is_empty(), "{:?}", mock.hits());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_revoked_credential_is_forgotten_even_when_the_file_cannot_be_cleared() {
        let mock = Mock::start().await;
        let file = mock.stored();
        let session = Session::device(mock.api(), &file)
            .await
            .expect("resume the saved device session");
        // The file turns into a symlink after the session was loaded, so the
        // clear fails once the server has already revoked the grant.
        let target = mock.dir.join("elsewhere.ini");
        std::fs::rename(&file, &target).expect("move the ini aside");
        std::os::unix::fs::symlink(&target, &file).expect("symlink the ini");

        let error = session
            .revoke_device()
            .await
            .failed("the file cannot be cleared");

        assert!(format!("{error:#}").contains("clearing"), "{error:#}");
        assert!(
            mock.hits().contains(&"POST /api/device/logout".to_string()),
            "the grant was revoked on the server: {:?}",
            mock.hits()
        );

        let before = mock.hits().len();
        let error = session
            .refresh_now()
            .await
            .failed("the credential is gone from memory");
        assert!(
            format!("{error:#}").contains("no refresh credential"),
            "{error:#}"
        );
        assert_eq!(
            mock.hits().len(),
            before,
            "a revoked credential is never sent again"
        );
    }

    #[tokio::test]
    async fn logout_pauses_automatic_refresh_until_an_explicit_one() {
        let mock = Mock::start().await;
        let session = Session::device(mock.api(), &mock.stored())
            .await
            .expect("resume the saved device session");
        let observed = session.access().await.expect("the bearer");

        assert_eq!(
            session.logout_access().await.expect("logout"),
            StatusCode::NO_CONTENT
        );
        assert!(
            !session
                .refresh_after_401(&observed)
                .await
                .expect("a paused session is not an error"),
            "a paused session must not refresh behind the user's back"
        );
        assert_eq!(
            mock.grants(),
            1,
            "the pause kept the credential off the wire"
        );
        assert_eq!(
            session.access().await.expect("the dead bearer"),
            observed,
            "reads keep using the revoked bearer so its 401 stays visible"
        );

        session.refresh_now().await.expect("an explicit refresh");
        assert_eq!(mock.grants(), 2);
        assert_eq!(session.access().await.expect("the bearer"), "ACCESS-2");
    }
}
