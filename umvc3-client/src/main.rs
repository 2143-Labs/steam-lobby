//! Reference native client for the UMVC3 ranked service.
//!
//! Drives the real HTTP lifecycle — ticket exchange, queue, matchmaker pairing,
//! accept/trying/connect/ready, bilateral report, result — so the game client's
//! work can be developed and regression-tested without the game client.
//!
//! By default two simulated players run in one process, because pairing needs
//! two queued players and that is the smallest thing that exercises it.

mod api;
mod log;
mod player;

use std::process::ExitCode;
use std::time::Duration;

use anyhow::{Context, Result, bail};

use crate::api::Api;
use crate::player::{ActiveMatch, Player, ResultSnapshot};

const USAGE: &str = "\
umvc3-client — reference native client for the UMVC3 ranked service

USAGE:
    umvc3-client [OPTIONS]

Drives two simulated players through a complete ranked match by default. Pass
--single to drive just one player, for a two-terminal run.

OPTIONS:
    --base-url <URL>        Ranked service origin     [default: http://localhost:8080]
    --mode <MODE>           Game mode to queue for    [default: umvc3_1v1]
    --difficulty <LEVEL>    easy | normal | hard      [default: normal]
    --steam-id <ID>         First player's Steam ID   [default: 76561198000000101]
    --peer-steam-id <ID>    Second player's Steam ID  [default: 76561198000000102]
    --single                Drive only this player (run two terminals)
    --side <a|b>            Which side --single is    [default: a]
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

    let api = Api::new(&args.base_url)?;
    log::note(format!("ranked service at {}", api.base()));
    verify_mode(&api, &args.mode).await?;

    let me = Player::authenticate(&api, args.side_label(), args.steam_id).await?;
    if args.single {
        return drive_single(&api, &args, me).await;
    }
    let peer = Player::authenticate(&api, "P2", args.peer_steam_id).await?;
    drive_pair(&api, &args, me, peer).await
}

impl Args {
    /// In `--single` mode the label follows the side; otherwise P1 is the local
    /// player and P2 is the peer created below.
    fn side_label(&self) -> &'static str {
        if self.single { self.side.label() } else { "P1" }
    }

    fn parse() -> Result<Parsed> {
        let mut args = Args::default();
        let mut argv = std::env::args().skip(1);
        while let Some(flag) = argv.next() {
            match flag.as_str() {
                "-h" | "--help" => return Ok(Parsed::Help),
                "--single" => args.single = true,
                "--base-url" => args.base_url = value(&mut argv, &flag)?,
                "--mode" => args.mode = value(&mut argv, &flag)?,
                "--difficulty" => args.difficulty = value(&mut argv, &flag)?,
                "--steam-id" => args.steam_id = number(&mut argv, &flag)?,
                "--peer-steam-id" => args.peer_steam_id = number(&mut argv, &flag)?,
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
async fn verify_mode(api: &Api, mode: &str) -> Result<()> {
    let body = api.modes().await?;
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
async fn drive_pair(api: &Api, args: &Args, p1: Player, p2: Player) -> Result<()> {
    log::note("both native sessions are live; queueing both players");
    p1.queue(api, &args.mode, &args.difficulty).await?;
    p2.queue(api, &args.mode, &args.difficulty).await?;
    p1.await_queued(api, &args.mode, args.stage_timeout).await?;
    p2.await_queued(api, &args.mode, args.stage_timeout).await?;

    log::note(format!(
        "waiting for the matchmaker (up to {}s; its schedule fires every ~2s)",
        args.pair_timeout.as_secs()
    ));
    let a1 = p1.await_pairing(api, args.pair_timeout).await?;
    let a2 = p2.await_pairing(api, args.pair_timeout).await?;
    check_pairing(&p1, &a1, &p2, &a2)?;
    log::note(format!(
        "paired on match {} (attempt {}…)",
        a1.match_token,
        truncate(&a1.attempt_id)
    ));
    log::line(&p1.label, format!("assigned role '{}'", a1.role));
    log::line(&p2.label, format!("assigned role '{}'", a2.role));

    p1.accept(api, &a1.match_token).await?;
    p2.accept(api, &a2.match_token).await?;
    p1.await_phase(api, "AwaitingTrying", args.stage_timeout)
        .await?;
    p2.await_phase(api, "AwaitingTrying", args.stage_timeout)
        .await?;

    p1.trying(api, &a1).await?;
    p2.trying(api, &a2).await?;
    p1.await_phase(api, "AwaitingConnect", args.stage_timeout)
        .await?;
    p2.await_phase(api, "AwaitingConnect", args.stage_timeout)
        .await?;

    p1.connect(api, &a1).await?;
    p2.connect(api, &a2).await?;
    p1.await_phase(api, "AwaitingReady", args.stage_timeout)
        .await?;
    p2.await_phase(api, "AwaitingReady", args.stage_timeout)
        .await?;

    p1.ready(api, &a1).await?;
    p2.ready(api, &a2).await?;
    p1.await_phase(api, "Playing", args.stage_timeout).await?;
    p2.await_phase(api, "Playing", args.stage_timeout).await?;
    log::note("match is live; both sides reporting a 2-0 win for P1");

    // Read the cursor BEFORE reporting, so the result event cannot be missed
    // even if it is emitted before the first poll.
    let cursor1 = p1.cursor(api).await?;
    let cursor2 = p2.cursor(api).await?;

    p1.report(api, &a1.match_token, "win").await?;
    p2.report(api, &a2.match_token, "loss").await?;

    let r1 = p1
        .await_result(api, cursor1, &a1.match_token, args.result_timeout)
        .await?;
    let r2 = p2
        .await_result(api, cursor2, &a2.match_token, args.result_timeout)
        .await?;
    summarise(&p1, &r1, &p2, &r2)
}

/// One player, driven from one terminal. The peer is a second run of this same
/// binary with `--single --side b`.
async fn drive_single(api: &Api, args: &Args, me: Player) -> Result<()> {
    let peer_side = if args.side == Side::A { "b" } else { "a" };
    log::note(format!(
        "single-player mode: start `umvc3-client --single --side {peer_side}` in another terminal"
    ));

    me.queue(api, &args.mode, &args.difficulty).await?;
    me.await_queued(api, &args.mode, args.stage_timeout).await?;

    log::note(format!(
        "waiting for the matchmaker (up to {}s)",
        args.pair_timeout.as_secs()
    ));
    let active = me.await_pairing(api, args.pair_timeout).await?;
    log::note(format!("paired on match {}", active.match_token));
    log::line(&me.label, format!("assigned role '{}'", active.role));

    me.accept(api, &active.match_token).await?;
    me.await_phase(api, "AwaitingTrying", args.stage_timeout)
        .await?;
    me.trying(api, &active).await?;
    me.await_phase(api, "AwaitingConnect", args.stage_timeout)
        .await?;
    me.connect(api, &active).await?;
    me.await_phase(api, "AwaitingReady", args.stage_timeout)
        .await?;
    me.ready(api, &active).await?;
    me.await_phase(api, "Playing", args.stage_timeout).await?;

    let outcome = args.side.reported_outcome();
    log::note(format!("reporting this side's result as '{outcome}'"));
    let cursor = me.cursor(api).await?;
    me.report(api, &active.match_token, outcome).await?;

    let result = me
        .await_result(api, cursor, &active.match_token, args.result_timeout)
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

fn truncate(text: &str) -> String {
    text.chars().take(8).collect()
}
