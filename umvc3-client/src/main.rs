//! Reference native client for the UMVC3 ranked service.
//!
//! Two ways to run it:
//!
//! * **Interactive** (the default on a terminal) — a shell where every step is
//!   a command, including deliberately wrong ones, and every request and
//!   response is printed verbatim. This is the choose-your-own-adventure mode
//!   for probing how the server behaves.
//! * **Scripted** (`--auto`, or whenever stdin is not a terminal) — drives the
//!   whole lifecycle and asserts that a rated match moves MMR exactly once. This
//!   is the regression path, and it is what a pipe or CI gets.
//!
//! It talks to the ranked service the way the game client has to: plain HTTP
//! against `/api/ticket`, `/api/command`, `/api/ranked/state` and `/api/events`,
//! with no WebSocket anywhere.

mod api;
mod ini;
mod log;
mod player;
mod repl;
mod session;

use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::api::Api;
use crate::ini::StoredDevice;
use crate::player::{ActiveMatch, Player, ResultSnapshot};

const DEFAULT_BASE_URL: &str = "http://localhost:8080";

const USAGE: &str = "\
umvc3-client — reference native client for the UMVC3 ranked service

USAGE:
    umvc3-client [OPTIONS]
    umvc3-client login --token-file <PATH> [--base-url <URL>] [--dev-approve]

With a terminal on stdin this starts an interactive shell (type 'help' for the
command list). Piped or with --auto it runs the whole lifecycle once, which is
the regression path.

OPTIONS:
    --base-url <URL>        Ranked service origin     [default: http://localhost:8080,
                            or the RankedBaseUrl saved in a --token-file]
    --mode <MODE>           Game mode to queue for    [default: umvc3_1v1]
    --difficulty <LEVEL>    easy | normal | hard      [default: normal]
    --steam-id <ID>         First player's Steam ID   [default: 76561198000000101]
    --peer-steam-id <ID>    Second player's Steam ID  [default: 76561198000000102]
    --single                Drive one player (a peer comes from a second run)
    --side <a|b>            Which side --single is    [default: a]
    --auto                  Skip the shell; run the scripted flow once
    --interactive           Force the shell even without a terminal on stdin
    --token-file <PATH>     First player signs in with a device session saved here
    --peer-token-file <PATH>
                            Second player's device session file (not with --single)
    --dev-approve           Approve a new device grant as --steam-id / --peer-steam-id
                            instead of a browser; loopback servers only
    --tick <SECS>           Shell state poll interval [default: 2]
    --pair-timeout <SECS>   Pairing deadline          [default: 60]
    --stage-timeout <SECS>  Per-stage deadline        [default: 30]
    --result-timeout <SECS> Result deadline           [default: 30]
    -h, --help              Print this help

Without a token file a player signs in with a dev:<steam_id> ticket, so the
server must be local and dev-flavoured: AUTH_DEV_MODE=true, and the queue
enabled (RANKED_QUEUE_ENABLED=true, the default when unset). Temporal must be
running — pairing is one of its schedules.

DEVICE LOGIN:
    umvc3-client login --base-url https://<service-origin> --token-file <private.ini>
    umvc3-client --token-file <private.ini> --single --interactive

'login' prints a code and a URL; approve the code in a browser signed in with
Steam, and the device credential is saved to [Settings] RankedRefreshToken and
RankedBaseUrl in the file. Later runs exchange it for native sessions without a
browser. It is a revocable long-lived credential: keep the file private (0600;
a group- or world-readable file is refused) and never share it. In the shell,
'logout' ends only the current access session, 'refresh' resumes it, and
'revoke-device' revokes the durable grant and clears the file. --dev-approve
stands in for the browser against a loopback dev server only. This models the
game's credential handoff; the game itself does not read this file.";

/// Which simulated player this process drives in `--single` mode.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    A,
    B,
}

impl Side {
    /// The result this side reports. The two sides must disagree, because the
    /// server only rates a match when the reports agree on a winner.
    fn reported_outcome(self) -> &'static str {
        match self {
            Side::A => "win",
            Side::B => "loss",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Side::A => "P1",
            Side::B => "P2",
        }
    }
}

struct Args {
    /// `None` unless `--base-url` was given, so a saved origin can apply.
    base_url: Option<String>,
    mode: String,
    difficulty: String,
    steam_id: u64,
    peer_steam_id: u64,
    single: bool,
    side: Side,
    auto: bool,
    interactive: bool,
    token_file: Option<PathBuf>,
    peer_token_file: Option<PathBuf>,
    dev_approve: bool,
    tick: Duration,
    pair_timeout: Duration,
    stage_timeout: Duration,
    result_timeout: Duration,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            base_url: None,
            mode: "umvc3_1v1".into(),
            difficulty: "normal".into(),
            steam_id: 76_561_198_000_000_101,
            peer_steam_id: 76_561_198_000_000_102,
            single: false,
            side: Side::A,
            auto: false,
            interactive: false,
            token_file: None,
            peer_token_file: None,
            dev_approve: false,
            tick: Duration::from_secs(2),
            pair_timeout: Duration::from_secs(60),
            stage_timeout: Duration::from_secs(30),
            result_timeout: Duration::from_secs(30),
        }
    }
}

enum Parsed {
    Help,
    Run(Box<Args>),
    Login(Box<Args>),
}

#[tokio::main]
async fn main() -> ExitCode {
    log::start();
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            log::note(format!("FAILED: {error:#}"));
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<()> {
    let (args, login) = match Args::parse()? {
        Parsed::Help => {
            println!("{USAGE}");
            return Ok(());
        }
        Parsed::Run(args) => (*args, false),
        Parsed::Login(args) => (*args, true),
    };
    args.validate(login)?;

    // Everything local — the token files and the origin they are bound to — is
    // settled before any request, so a missing session fails even offline and
    // no credential is ever sent to the wrong server.
    let stored = [
        args.token_file.as_deref().map_or(Ok(None), ini::load)?,
        args.peer_token_file
            .as_deref()
            .map_or(Ok(None), ini::load)?,
    ];
    if login && stored[0].is_some() {
        bail!("already linked; use the saved session or revoke-device first");
    }
    let base = args.resolve_base(&stored)?;
    if args.dev_approve && !api::is_loopback(&base) {
        bail!("--dev-approve only talks to a loopback server (localhost, 127.0.0.1, ::1)");
    }

    if login {
        let file = args
            .token_file
            .as_deref()
            .context("login requires --token-file")?;
        log::note(format!("device login at {base}"));
        Player::login(&base, "P1", file, args.dev_approve.then_some(args.steam_id)).await?;
        log::note(format!(
            "device linked; start a session with --token-file {}",
            file.display()
        ));
        return Ok(());
    }

    // Interactive when a human is watching; scripted otherwise, so piping the
    // binary or running it from CI keeps the old deterministic behaviour.
    let interactive = if args.interactive {
        true
    } else if args.auto {
        false
    } else {
        std::io::stdin().is_terminal()
    };
    // Nobody is watching an unattended run to approve a browser prompt.
    for (file, saved) in [
        (&args.token_file, &stored[0]),
        (&args.peer_token_file, &stored[1]),
    ] {
        if let Some(file) = file
            && saved.is_none()
            && !interactive
            && !args.dev_approve
        {
            bail!(
                "no saved device session in {0}; run login --token-file {0} first",
                file.display()
            );
        }
    }

    let service = Api::new(&base, "---")?;
    log::note(format!("ranked service at {}", service.base()));
    verify_mode(&service, &args.mode).await?;

    let mut players = Vec::new();
    let first = args.token_file.as_deref();
    players.push(
        args.player(
            &base,
            args.first_label(),
            args.steam_id,
            first,
            stored[0].is_some(),
        )
        .await?,
    );
    if !args.single {
        let peer = args.peer_token_file.as_deref();
        players.push(
            args.player(&base, "P2", args.peer_steam_id, peer, stored[1].is_some())
                .await?,
        );
    }

    if interactive {
        let mut shell = repl::Shell::new(
            players,
            args.mode.clone(),
            args.difficulty.clone(),
            args.tick,
        )
        .await?;
        return shell.loop_forever().await;
    }

    log::note("scripted mode (pass a terminal on stdin, or omit --auto, for the shell)");
    if args.single {
        return drive_single(&args, &players[0]).await;
    }
    drive_pair(&args, &players[0], &players[1]).await
}

impl Args {
    fn first_label(&self) -> &'static str {
        if self.single { self.side.label() } else { "P1" }
    }

    fn parse() -> Result<Parsed> {
        let mut args = Args::default();
        let mut argv = std::env::args().skip(1).peekable();
        // `login` is a subcommand only in first position; anywhere else it is
        // an unrecognised argument like any other.
        let login = argv.next_if(|first| first == "login").is_some();
        while let Some(flag) = argv.next() {
            match flag.as_str() {
                "-h" | "--help" => return Ok(Parsed::Help),
                "--single" => args.single = true,
                "--auto" => args.auto = true,
                "--interactive" => args.interactive = true,
                "--dev-approve" => args.dev_approve = true,
                "--base-url" => args.base_url = Some(value(&mut argv, &flag)?),
                "--token-file" => args.token_file = Some(path(&mut argv, &flag)?),
                "--peer-token-file" => args.peer_token_file = Some(path(&mut argv, &flag)?),
                "--mode" => args.mode = value(&mut argv, &flag)?,
                "--difficulty" => args.difficulty = value(&mut argv, &flag)?,
                "--steam-id" => args.steam_id = number(&mut argv, &flag)?,
                "--peer-steam-id" => args.peer_steam_id = number(&mut argv, &flag)?,
                "--tick" => args.tick = Duration::from_secs(number(&mut argv, &flag)?.max(1)),
                "--pair-timeout" => args.pair_timeout = secs(&mut argv, &flag)?,
                "--stage-timeout" => args.stage_timeout = secs(&mut argv, &flag)?,
                "--result-timeout" => args.result_timeout = secs(&mut argv, &flag)?,
                "--side" => {
                    args.side = match value(&mut argv, &flag)?.as_str() {
                        "a" | "A" | "1" => Side::A,
                        "b" | "B" | "2" => Side::B,
                        other => bail!("--side must be a or b, got '{other}'"),
                    }
                }
                other => bail!("unrecognised argument '{other}' (try --help)"),
            }
        }
        let args = Box::new(args);
        Ok(if login {
            Parsed::Login(args)
        } else {
            Parsed::Run(args)
        })
    }

    /// Flag combinations that cannot mean anything, refused before any I/O.
    fn validate(&self, login: bool) -> Result<()> {
        if login {
            if self.token_file.is_none() {
                bail!("login requires --token-file <PATH>");
            }
            for (set, flag) in [
                (self.single, "--single"),
                (self.peer_token_file.is_some(), "--peer-token-file"),
                (self.auto, "--auto"),
                (self.interactive, "--interactive"),
            ] {
                if set {
                    bail!("login does not take {flag}");
                }
            }
        }
        if let Some(peer) = &self.peer_token_file {
            if self.single {
                bail!("--peer-token-file needs a second player; drop --single");
            }
            if self
                .token_file
                .as_deref()
                .is_some_and(|own| same_path(own, peer))
            {
                bail!("--peer-token-file must name a different file from --token-file");
            }
        }
        if self.dev_approve && self.token_file.is_none() && self.peer_token_file.is_none() {
            bail!("--dev-approve needs --token-file or --peer-token-file");
        }
        Ok(())
    }

    /// The one origin this process talks to: an explicit `--base-url`, else the
    /// origin a saved device credential is bound to, else the default. Without
    /// a token file the dev-ticket path keeps using the URL as given.
    fn resolve_base(&self, stored: &[Option<StoredDevice>; 2]) -> Result<String> {
        if self.token_file.is_none() && self.peer_token_file.is_none() {
            return Ok(self
                .base_url
                .clone()
                .unwrap_or_else(|| DEFAULT_BASE_URL.to_string()));
        }
        let mut bound: Option<String> = None;
        for (saved, file) in stored.iter().zip([&self.token_file, &self.peer_token_file]) {
            let (Some(saved), Some(file)) = (saved, file) else {
                continue;
            };
            let origin = api::device_origin(&saved.base)
                .with_context(|| format!("{}: RankedBaseUrl", file.display()))?;
            if let Some(other) = &bound
                && *other != origin
            {
                bail!(
                    "--token-file and --peer-token-file are bound to different servers \
                     ({other} vs {origin})"
                );
            }
            bound = Some(origin);
        }
        let explicit = self
            .base_url
            .as_deref()
            .map(api::device_origin)
            .transpose()
            .context("--base-url")?;
        match (explicit, bound) {
            (Some(explicit), Some(bound)) if explicit != bound => bail!(
                "--base-url {explicit} does not match {bound}, the server the saved device \
                 session is bound to; refusing to send its credential"
            ),
            (Some(origin), _) | (None, Some(origin)) => Ok(origin),
            (None, None) => api::device_origin(DEFAULT_BASE_URL),
        }
    }

    /// One player: a dev ticket without a token file; with one, its saved
    /// device session, or a new device grant when none is saved yet.
    async fn player(
        &self,
        base: &str,
        label: &str,
        steam_id: u64,
        file: Option<&Path>,
        saved: bool,
    ) -> Result<Player> {
        match file {
            None => Player::authenticate(base, label, steam_id).await,
            Some(file) if saved => Player::from_device(base, label, file).await,
            Some(file) => {
                Player::login(base, label, file, self.dev_approve.then_some(steam_id)).await
            }
        }
    }
}

/// Whether two paths name one file, even spelled differently.
fn same_path(a: &Path, b: &Path) -> bool {
    let resolve = |p: &Path| p.canonicalize().or_else(|_| std::path::absolute(p)).ok();
    a == b || matches!((resolve(a), resolve(b)), (Some(a), Some(b)) if a == b)
}

fn path(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<PathBuf> {
    let raw = value(argv, flag)?;
    if raw.is_empty() {
        bail!("{flag} needs a non-empty path");
    }
    Ok(PathBuf::from(raw))
}

fn value(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<String> {
    argv.next()
        .with_context(|| format!("{flag} requires a value"))
}

fn number(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<u64> {
    let raw = value(argv, flag)?;
    raw.parse()
        .with_context(|| format!("{flag} expects a number, got '{raw}'"))
}

fn secs(argv: &mut impl Iterator<Item = String>, flag: &str) -> Result<Duration> {
    Ok(Duration::from_secs(number(argv, flag)?))
}

/// Fail fast, with an actionable message, when the server will not let us queue
/// for this mode — otherwise the first symptom is a rejected command.
async fn verify_mode(service: &Api, mode: &str) -> Result<()> {
    let body = service.modes().await?;
    let modes = body["modes"].as_array().cloned().unwrap_or_default();
    let Some(entry) = modes
        .iter()
        .find(|entry| entry["name"].as_str() == Some(mode))
    else {
        let advertised: Vec<&str> = modes
            .iter()
            .filter_map(|entry| entry["name"].as_str())
            .collect();
        bail!(
            "this server does not advertise {mode} (it advertises: {})",
            if advertised.is_empty() {
                "nothing".to_string()
            } else {
                advertised.join(", ")
            }
        );
    };
    if entry["queue_enabled"].as_bool() == Some(false) {
        bail!(
            "the server advertises {mode} but reports queue_enabled=false, so every queue command \
             will be refused with ranked_queue_disabled. Restart it with RANKED_QUEUE_ENABLED=true \
             (or unset) to run this client."
        );
    }
    log::note(format!("{mode} advertised and its queue is enabled"));
    Ok(())
}

/// Two players in one process: queue both, wait for the real matchmaker, drive
/// both sides through the lifecycle, then report bilaterally.
async fn drive_pair(args: &Args, p1: &Player, p2: &Player) -> Result<()> {
    log::note("queueing both players");
    for (player, difficulty) in [(p1, &args.difficulty), (p2, &args.difficulty)] {
        let body = player.queue_body(&args.mode, difficulty);
        player.send_applied(body).await?;
    }
    for player in [p1, p2] {
        player.await_queued(&args.mode, args.stage_timeout).await?;
    }

    log::note(format!(
        "waiting for the matchmaker (up to {}s; its schedule fires every ~2s)",
        args.pair_timeout.as_secs()
    ));
    let a1 = p1.await_pairing(args.pair_timeout).await?;
    let a2 = p2.await_pairing(args.pair_timeout).await?;
    check_pairing(p1, &a1, p2, &a2)?;
    log::note(format!("paired on {}", a1.match_token));
    log::line(&p1.label, format!("    {}", a1.summary()));
    log::line(&p2.label, format!("    {}", a2.summary()));

    for (player, active) in [(p1, &a1), (p2, &a2)] {
        let body = player.accept_body(&active.match_token);
        player.send_applied(body).await?;
    }
    for player in [p1, p2] {
        player
            .await_phase("AwaitingTrying", args.stage_timeout)
            .await?;
    }

    for (player, active) in [(p1, &a1), (p2, &a2)] {
        let body = player.trying_body(active, None);
        player.send_applied(body).await?;
    }
    for player in [p1, p2] {
        player
            .await_phase("AwaitingConnect", args.stage_timeout)
            .await?;
    }

    for (player, active) in [(p1, &a1), (p2, &a2)] {
        let body = player.connect_body(active, None);
        player.send_applied(body).await?;
    }
    for player in [p1, p2] {
        player
            .await_phase("AwaitingReady", args.stage_timeout)
            .await?;
    }

    for (player, active) in [(p1, &a1), (p2, &a2)] {
        let body = player.ready_body(active, None);
        player.send_applied(body).await?;
    }
    for player in [p1, p2] {
        player.await_phase("Playing", args.stage_timeout).await?;
    }

    log::note("match is live; both sides reporting a 2-0 win for P1");
    let cursor1 = p1.cursor().await?;
    let cursor2 = p2.cursor().await?;
    for (player, outcome) in [(p1, "win"), (p2, "loss")] {
        let body = player.report_body(&a1.match_token, outcome, "2-0", "deadbeef", 900);
        player.send_applied(body).await?;
    }

    let r1 = p1
        .await_result(cursor1, &a1.match_token, args.result_timeout)
        .await?;
    let r2 = p2
        .await_result(cursor2, &a1.match_token, args.result_timeout)
        .await?;
    summarise(p1, &r1, p2, &r2)
}

/// One player, driven from one run. The peer is a second run of this binary
/// with `--single --side b`.
async fn drive_single(args: &Args, me: &Player) -> Result<()> {
    let peer_side = if args.side == Side::A { "b" } else { "a" };
    log::note(format!(
        "single-player mode: start `umvc3-client --single --side {peer_side}` in another terminal"
    ));

    let body = me.queue_body(&args.mode, &args.difficulty);
    me.send_applied(body).await?;
    me.await_queued(&args.mode, args.stage_timeout).await?;

    log::note(format!(
        "waiting for the matchmaker (up to {}s)",
        args.pair_timeout.as_secs()
    ));
    let active = me.await_pairing(args.pair_timeout).await?;
    log::note(format!("paired on {}", active.match_token));
    log::line(&me.label, format!("    {}", active.summary()));

    let body = me.accept_body(&active.match_token);
    me.send_applied(body).await?;
    me.await_phase("AwaitingTrying", args.stage_timeout).await?;
    let body = me.trying_body(&active, None);
    me.send_applied(body).await?;
    me.await_phase("AwaitingConnect", args.stage_timeout)
        .await?;
    let body = me.connect_body(&active, None);
    me.send_applied(body).await?;
    me.await_phase("AwaitingReady", args.stage_timeout).await?;
    let body = me.ready_body(&active, None);
    me.send_applied(body).await?;
    me.await_phase("Playing", args.stage_timeout).await?;

    let outcome = args.side.reported_outcome();
    log::note(format!("reporting this side's result as '{outcome}'"));
    let cursor = me.cursor().await?;
    let body = me.report_body(&active.match_token, outcome, "2-0", "deadbeef", 900);
    me.send_applied(body).await?;

    let result = me
        .await_result(cursor, &active.match_token, args.result_timeout)
        .await?;
    log::line(
        &me.label,
        format!(
            "{}  outcome={}  mmr_delta={}",
            result.event_type,
            result.outcome,
            render_delta(result.mmr_delta)
        ),
    );
    if result.outcome != outcome {
        bail!(
            "reported '{outcome}' but the server resolved '{}'",
            result.outcome
        );
    }
    log::note("this side's ranked match completed");
    Ok(())
}

/// Both sides must observe one match, name each other, and hold opposite roles.
fn check_pairing(p1: &Player, a1: &ActiveMatch, p2: &Player, a2: &ActiveMatch) -> Result<()> {
    if a1.match_token != a2.match_token {
        bail!(
            "{} and {} disagree on the match: {} vs {}",
            p1.label,
            p2.label,
            a1.match_token,
            a2.match_token
        );
    }
    if a1.opponent == a2.opponent {
        bail!(
            "both players report the same opponent ({}) — each must name the other",
            a1.opponent
        );
    }
    if a1.role == a2.role {
        bail!(
            "both players were assigned role '{}' — exactly one must create the lobby",
            a1.role
        );
    }
    Ok(())
}

/// Print and check the outcome: a rated win/loss moves MMR once, in opposite
/// directions, which is the invariant the whole lifecycle exists to produce.
fn summarise(p1: &Player, r1: &ResultSnapshot, p2: &Player, r2: &ResultSnapshot) -> Result<()> {
    for (player, result) in [(p1, r1), (p2, r2)] {
        log::line(
            &player.label,
            format!(
                "steam_id={}  {}  outcome={}  mmr_delta={}",
                player.steam_id_text(),
                result.event_type,
                result.outcome,
                render_delta(result.mmr_delta)
            ),
        );
    }
    if r1.outcome != "win" || r2.outcome != "loss" {
        bail!(
            "expected P1 to resolve as win and P2 as loss, got '{}'/'{}'",
            r1.outcome,
            r2.outcome
        );
    }
    let (Some(delta1), Some(delta2)) = (r1.mmr_delta, r2.mmr_delta) else {
        bail!("a rated win/loss must carry an MMR delta for both players");
    };
    if !(delta1 > 0.0 && delta2 < 0.0) {
        bail!(
            "expected the winner's delta positive and the loser's negative, got {delta1:+.3} / {delta2:+.3}"
        );
    }
    log::note(
        "MMR moved exactly once, in opposite directions — the ranked lifecycle completed end to end",
    );
    Ok(())
}

fn render_delta(delta: Option<f64>) -> String {
    match delta {
        Some(value) => format!("{value:+.2}"),
        None => "none".to_string(),
    }
}
