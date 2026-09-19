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
mod log;
mod player;
mod repl;

use std::io::IsTerminal;
use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::api::Api;
use crate::player::{ActiveMatch, Player, ResultSnapshot};

const USAGE: &str = "\
umvc3-client — reference native client for the UMVC3 ranked service

USAGE:
    umvc3-client [OPTIONS]

With a terminal on stdin this starts an interactive shell (type 'help' for the
command list). Piped or with --auto it runs the whole lifecycle once, which is
the regression path.

OPTIONS:
    --base-url <URL>        Ranked service origin     [default: http://localhost:8080]
    --mode <MODE>           Game mode to queue for    [default: umvc3_1v1]
    --difficulty <LEVEL>    easy | normal | hard      [default: normal]
    --steam-id <ID>         First player's Steam ID   [default: 76561198000000101]
    --peer-steam-id <ID>    Second player's Steam ID  [default: 76561198000000102]
    --single                Drive one player (a peer comes from a second run)
    --side <a|b>            Which side --single is    [default: a]
    --auto                  Skip the shell; run the scripted flow once
    --interactive           Force the shell even without a terminal on stdin
    --tick <SECS>           Shell state poll interval [default: 2]
    --pair-timeout <SECS>   Pairing deadline          [default: 60]
    --stage-timeout <SECS>  Per-stage deadline        [default: 30]
    --result-timeout <SECS> Result deadline           [default: 30]
    -h, --help              Print this help

The server must be local and dev-flavoured: AUTH_DEV_MODE=true so a dev: ticket
is accepted, and the queue enabled (RANKED_QUEUE_ENABLED=true, the default when
unset). Temporal must be running — pairing is one of its schedules.";

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
    base_url: String,
    mode: String,
    difficulty: String,
    steam_id: u64,
    peer_steam_id: u64,
    single: bool,
    side: Side,
    auto: bool,
    interactive: bool,
    tick: Duration,
    pair_timeout: Duration,
    stage_timeout: Duration,
    result_timeout: Duration,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            base_url: "http://localhost:8080".into(),
            mode: "umvc3_1v1".into(),
            difficulty: "normal".into(),
            steam_id: 76_561_198_000_000_101,
            peer_steam_id: 76_561_198_000_000_102,
            single: false,
            side: Side::A,
            auto: false,
            interactive: false,
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
    let args = match Args::parse()? {
        Parsed::Help => {
            println!("{USAGE}");
            return Ok(());
        }
        Parsed::Run(args) => *args,
    };

    let service = Api::new(&args.base_url, "---")?;
    log::note(format!("ranked service at {}", service.base()));
    verify_mode(&service, &args.mode).await?;

    let mut players = Vec::new();
    players.push(Player::authenticate(&args.base_url, args.first_label(), args.steam_id).await?);
    if !args.single {
        players.push(Player::authenticate(&args.base_url, "P2", args.peer_steam_id).await?);
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
        let mut argv = std::env::args().skip(1);
        while let Some(flag) = argv.next() {
            match flag.as_str() {
                "-h" | "--help" => return Ok(Parsed::Help),
                "--single" => args.single = true,
                "--auto" => args.auto = true,
                "--interactive" => args.interactive = true,
                "--base-url" => args.base_url = value(&mut argv, &flag)?,
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
        Ok(Parsed::Run(Box::new(args)))
    }
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
                player.steam_id,
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
