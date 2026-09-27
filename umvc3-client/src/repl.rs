//! The interactive shell: a choose-your-own-adventure driver for the ranked API.
//!
//! Everything the client can do is a command here, including the things a
//! well-behaved client never does. The point is to watch how the server reacts,
//! with every request and response printed verbatim.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::api::{compact, redact};
use crate::log;
use crate::player::{ActiveMatch, NativeUnauthorized, Player, require_match, text};
use crate::session::RefreshFailed;

/// Why a background read failed. A fixed label, never a server-provided string.
#[derive(Clone, Copy, PartialEq, Eq)]
enum FailureKind {
    Transport,
    Refresh,
}

/// What a background reader tells the shell, tagged with the player's index.
enum EventMsg {
    Event { index: usize, value: Value },
    Unauthorized { index: usize },
    Failed { index: usize, kind: FailureKind },
    Recovered { index: usize },
}

impl EventMsg {
    /// Classify a failed read by type, not by parsing its message.
    fn failure(index: usize, error: &anyhow::Error) -> Self {
        if error.downcast_ref::<NativeUnauthorized>().is_some() {
            Self::Unauthorized { index }
        } else if error.downcast_ref::<RefreshFailed>().is_some() {
            Self::Failed {
                index,
                kind: FailureKind::Refresh,
            }
        } else {
            Self::Failed {
                index,
                kind: FailureKind::Transport,
            }
        }
    }
}

/// A player's background connection trouble; absent means healthy.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Connection {
    Unauthorized,
    Failed(FailureKind),
}

const HELP: &str = "\
commands (act as the focused player unless a player is named):

  help                       this list
  players                    players, focus, and the last thing each reported
  focus <p1|p2>              choose which player later commands act as
  packets <on|off>           print or silence every request and response
  poll <on|off>              run/stop background polling — 'off' is a disconnect
  tick <secs>                how often the focused state is re-read (default 2)

queueing
  queue [difficulty]         POST a queue command (default difficulty: normal)
  cancel                     cancel the queued entry
  heartbeat                  keepalive
  wait <secs>                do nothing for a while, letting timers run

match
  accept | decline           answer a found match
  trying                     report the lobby rendezvous (role read from state)
  connect                    report that you reached the peer
  ready                      report ready
  abandon <self|peer>        abandon the match
  report <win|loss|draw> [score] [checksum] [frame]
                             submit the final result (defaults 2-0/deadbeef/900)

inspection
  state [p1|p2|all]          GET /api/ranked/state, pretty-printed
  events [p1|p2|all]         GET /api/events from the next cursor
  receipts                   the focused player's command receipts
  cursors                    current event cursors

deliberately abnormal
  raw <json>                 POST an arbitrary body verbatim
  replay <type>              resend the last <type> with the SAME command_id (expect 200)
  conflict <type>            resend it with a changed field      (expect 409 conflict)
  wrong-attempt <trying|connect|ready>
                             send with a bogus attempt_id        (expect stale_attempt)
  bad-token                  send a heartbeat with a garbage bearer (expect 401)
  steal <match_token>        accept a match token you were not given (expect a refusal)

session
  logout                     end this access session only; background reads report
                             'session unauthorized (401)' until 'refresh'
  refresh                    exchange the saved device credential for a fresh bearer
                             now; resumes automatic refresh after 'logout'
  revoke-device              revoke the device credential on the server, then clear
                             it from the token file (log in again to come back)

flow
  auto                       run the whole normal flow for both players
  quit

notes
  A player left queued outlives this process and can pair with your NEXT run
  instead of the peer you meant — 'cancel' (or 'abandon self' mid-match) first.
  'packets off' leaves only outcomes; 'poll off' stops all background traffic.";

pub struct Shell {
    players: Vec<Player>,
    focus: usize,
    polling: bool,
    tick: Duration,
    /// The last body sent, per (player, command type), so `replay` and
    /// `conflict` can resend it untouched or with one field changed.
    last: HashMap<(usize, String), Value>,
    /// Last status line printed per player, so ticks only speak on change.
    last_status: HashMap<usize, String>,
    /// Current connection trouble per player, so a failure is reported once
    /// per transition rather than every poll.
    connection: HashMap<usize, Connection>,
    cursors: Vec<i64>,
    events_tx: mpsc::UnboundedSender<EventMsg>,
    event_readers: Vec<JoinHandle<()>>,
    mode: String,
    difficulty: String,
}

impl Shell {
    pub async fn new(
        players: Vec<Player>,
        mode: String,
        difficulty: String,
        tick: Duration,
    ) -> Result<Self> {
        let mut cursors = Vec::with_capacity(players.len());
        for player in &players {
            cursors.push(player.cursor().await?);
        }
        let (events_tx, _) = mpsc::unbounded_channel();
        Ok(Self {
            players,
            focus: 0,
            polling: true,
            tick,
            last: HashMap::new(),
            last_status: HashMap::new(),
            connection: HashMap::new(),
            cursors,
            events_tx,
            event_readers: Vec::new(),
            mode,
            difficulty,
        })
    }

    fn focused(&mut self) -> &mut Player {
        &mut self.players[self.focus]
    }

    /// Resolve an optional player argument to an index.
    fn resolve(&self, name: Option<&str>) -> Result<usize> {
        match name {
            None => Ok(self.focus),
            Some("p1") | Some("1") => Ok(0),
            Some("p2") | Some("2") => Ok(1.min(self.players.len().saturating_sub(1))),
            Some(other) => bail!("unknown player '{other}' (use p1 or p2)"),
        }
    }

    pub async fn loop_forever(&mut self) -> Result<()> {
        let mut stdin = spawn_stdin();
        // The event readers send into this channel; the shell drains it so
        // received events interleave with commands without either blocking.
        let (tx, mut events_rx) = mpsc::unbounded_channel::<EventMsg>();
        self.events_tx = tx;

        self.start_polling();
        self.banner();

        let mut ticker = tokio::time::interval(self.tick);
        loop {
            tokio::select! {
                line = stdin.recv() => {
                    match line {
                        Some(line) => {
                            // A failed command must not end the session: trying
                            // things that fail is the entire point of the shell.
                            match self.handle(&line).await {
                                Ok(true) => {}
                                Ok(false) => break,
                                Err(error) => log::note(format!("error: {error:#}")),
                            }
                        }
                        // stdin closed (piped input exhausted, or Ctrl-D).
                        None => break,
                    }
                }
                Some(message) = events_rx.recv() => self.receive(message),
                _ = ticker.tick() => {
                    if self.polling {
                        self.poll_states().await;
                    }
                }
            }
        }
        self.stop_polling();
        Ok(())
    }

    fn banner(&self) {
        log::note(format!(
            "interactive: {} player(s), focus P1, mode {}, polling on",
            self.players.len(),
            self.mode
        ));
        log::note("type 'help' for the full command list");
    }

    // ── background polling ───────────────────────────────────────────────

    /// Reading events long-polls for up to 25s, so it cannot share the shell's
    /// loop; each player gets a task that is cancelled outright when polling is
    /// switched off, which is what makes 'poll off' take effect immediately.
    fn start_polling(&mut self) {
        self.polling = true;
        for (index, player) in self.players.iter().enumerate() {
            let player = player.clone();
            let tx = self.events_tx.clone();
            let mut cursor = self.cursors[index];
            self.event_readers.push(tokio::spawn(async move {
                loop {
                    let message = match player.events_quiet(cursor).await {
                        Ok(page) => {
                            cursor = page["next_cursor"].as_i64().unwrap_or(cursor);
                            for value in page["events"].as_array().into_iter().flatten() {
                                let value = value.clone();
                                if tx.send(EventMsg::Event { index, value }).is_err() {
                                    return;
                                }
                            }
                            EventMsg::Recovered { index }
                        }
                        // Keep trying, so a revoked or refused session is reported
                        // once and its recovery is noticed; never log in from here.
                        Err(error) => {
                            let message = EventMsg::failure(index, &error);
                            if tx.send(message).is_err() {
                                return;
                            }
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            continue;
                        }
                    };
                    if tx.send(message).is_err() {
                        return;
                    }
                }
            }));
        }
    }

    fn stop_polling(&mut self) {
        self.polling = false;
        for reader in self.event_readers.drain(..) {
            reader.abort();
        }
    }

    fn receive(&mut self, message: EventMsg) {
        match message {
            EventMsg::Event { index, value } => self.report_event(index, &value),
            EventMsg::Unauthorized { index } => {
                self.report_connection(index, Some(Connection::Unauthorized));
            }
            EventMsg::Failed { index, kind } => {
                self.report_connection(index, Some(Connection::Failed(kind)));
            }
            EventMsg::Recovered { index } => self.report_connection(index, None),
        }
    }

    fn report_event(&mut self, index: usize, event: &Value) {
        let label = self.players[index].label.clone();
        log::line(
            &label,
            format!(
                "event #{} {}  {}",
                event["sequence_no"].as_i64().unwrap_or_default(),
                event["type"].as_str().unwrap_or("?"),
                compact(&redact(&event["payload"]))
            ),
        );
    }

    /// Speak only when a player's background connection changes state, so a
    /// dead session reads as one line rather than one per poll.
    fn report_connection(&mut self, index: usize, state: Option<Connection>) {
        if self.connection.get(&index).copied() == state {
            return;
        }
        let message = match state {
            Some(Connection::Unauthorized) => "session unauthorized (401)",
            Some(Connection::Failed(FailureKind::Transport)) => {
                "background read failed (transport); retrying quietly"
            }
            Some(Connection::Failed(FailureKind::Refresh)) => {
                "background read failed (refresh); retrying quietly"
            }
            None => "session recovered",
        };
        match state {
            Some(state) => self.connection.insert(index, state),
            None => self.connection.remove(&index),
        };
        let label = self.players[index].label.clone();
        log::line(&label, message);
    }

    /// Re-read every player's state and speak only when something changed, so
    /// the transcript shows transitions rather than a heartbeat.
    async fn poll_states(&mut self) {
        for index in 0..self.players.len() {
            let snapshot = match self.players[index].state_quiet().await {
                Ok(snapshot) => {
                    self.report_connection(index, None);
                    snapshot
                }
                // The poller stays silent on the wire by design; a failure is
                // reported once per transition, not every tick.
                Err(error) => {
                    self.receive(EventMsg::failure(index, &error));
                    continue;
                }
            };
            let active = ActiveMatch::from_state(&snapshot);
            let summary = match &active {
                Some(active) => format!(
                    "queued={} {}",
                    text(&snapshot, "/queue/mode"),
                    active.summary()
                ),
                None => {
                    let queued = text(&snapshot, "/queue/mode");
                    let queued = if queued.is_empty() {
                        "-".to_string()
                    } else {
                        queued
                    };
                    format!("queued={queued} no active match")
                }
            };
            if self.last_status.get(&index) != Some(&summary) {
                self.last_status.insert(index, summary.clone());
                let label = self.players[index].label.clone();
                log::line(&label, format!("status {summary}"));
            }
        }
    }

    // ── command handling ─────────────────────────────────────────────────

    async fn handle(&mut self, line: &str) -> Result<bool> {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return Ok(true);
        }
        // `raw` takes the rest of the line verbatim so JSON keeps its spacing.
        if let Some(rest) = trimmed.strip_prefix("raw ") {
            return self.cmd_raw(rest.trim()).await;
        }
        let mut parts = trimmed.split_whitespace();
        let command = parts.next().unwrap_or_default();
        let args: Vec<&str> = parts.collect();
        match command {
            "help" | "?" => println!("{HELP}"),
            "quit" | "exit" | "q" => {
                self.cleanup_notice().await;
                return Ok(false);
            }
            "players" => self.cmd_players(),
            "focus" => {
                let index = self.resolve(args.first().copied())?;
                self.focus = index;
                log::note(format!("focus is now {}", self.players[index].label));
            }
            "packets" => match args.first().copied() {
                Some("on") => {
                    crate::api::show_packets(true);
                    log::note("packet log on");
                }
                Some("off") => {
                    crate::api::show_packets(false);
                    log::note("packet log off");
                }
                _ => log::note(format!(
                    "packet log is {}",
                    if crate::api::packets_shown() {
                        "on"
                    } else {
                        "off"
                    }
                )),
            },
            "poll" => match args.first().copied() {
                Some("on") => {
                    self.stop_polling();
                    self.start_polling();
                    self.last_status.clear();
                    log::note("polling on");
                }
                Some("off") => {
                    self.stop_polling();
                    log::note("polling off — this client has stopped talking to the server");
                }
                _ => log::note(format!(
                    "polling is {}",
                    if self.polling { "on" } else { "off" }
                )),
            },
            "tick" => {
                let seconds = parse_arg(args.first(), "tick")?;
                self.tick = Duration::from_secs(seconds.max(1));
                log::note(format!(
                    "tick is {}s (takes effect on the next loop)",
                    self.tick.as_secs()
                ));
            }
            "queue" => {
                let difficulty = args
                    .first()
                    .copied()
                    .unwrap_or(&self.difficulty)
                    .to_string();
                let body = {
                    let mode = self.mode.clone();
                    self.focused().queue_body(&mode, &difficulty)
                };
                self.send_recording(body).await?;
            }
            "cancel" => {
                let body = self.focused().cancel_queue_body();
                self.send_recording(body).await?;
            }
            "heartbeat" => {
                let body = self.focused().heartbeat_body();
                self.send_recording(body).await?;
            }
            "wait" => {
                let seconds = parse_arg(args.first(), "wait")?;
                log::note(format!("waiting {seconds}s"));
                tokio::time::sleep(Duration::from_secs(seconds)).await;
            }
            "accept" | "decline" => {
                let active = require_match(self.focused().active().await?)?;
                let body = if command == "accept" {
                    self.focused().accept_body(&active.match_token)
                } else {
                    self.focused().decline_body(&active.match_token)
                };
                self.send_recording(body).await?;
            }
            "trying" | "connect" | "ready" => {
                let active = require_match(self.focused().active().await?)?;
                let body = self.lifecycle_body(command, &active, None);
                self.send_recording(body).await?;
            }
            "abandon" => {
                let target = args.first().copied().unwrap_or("self");
                if target != "self" && target != "peer" {
                    bail!("abandon takes 'self' or 'peer'");
                }
                let active = require_match(self.focused().active().await?)?;
                let body = self.focused().abandon_body(&active.match_token, target);
                self.send_recording(body).await?;
            }
            "report" => {
                let outcome = args.first().copied().unwrap_or("win");
                let score = args.get(1).copied().unwrap_or("2-0");
                let checksum = args.get(2).copied().unwrap_or("deadbeef");
                let frame = args.get(3).and_then(|raw| raw.parse().ok()).unwrap_or(900);
                let active = require_match(self.focused().active().await?)?;
                let body = self.focused().report_body(
                    &active.match_token,
                    outcome,
                    score,
                    checksum,
                    frame,
                );
                self.send_recording(body).await?;
            }
            "state" => self.cmd_state(args.first().copied()).await?,
            "events" => self.cmd_events(args.first().copied()).await?,
            "receipts" => self.cmd_receipts().await?,
            "cursors" => {
                for index in 0..self.players.len() {
                    let cursor = self.players[index].cursor().await?;
                    self.cursors[index] = cursor;
                    let label = self.players[index].label.clone();
                    log::line(&label, format!("cursor {cursor}"));
                }
            }
            "replay" => {
                let kind = args.first().copied().context("replay <type>")?;
                let body = self.recalled(kind)?.clone();
                log::note(format!("replaying {kind} with the same command_id"));
                self.send_raw(body).await?;
            }
            "conflict" => {
                let kind = args.first().copied().context("conflict <type>")?;
                let body = self.recalled(kind)?.clone();
                let mutated = mutate(body);
                log::note(format!(
                    "resending {kind} with the same command_id and one field changed"
                ));
                self.send_raw(mutated).await?;
            }
            "wrong-attempt" => {
                let kind = args.first().copied().unwrap_or("ready");
                if !matches!(kind, "trying" | "connect" | "ready") {
                    bail!("wrong-attempt takes trying, connect or ready");
                }
                let active = require_match(self.focused().active().await?)?;
                let bogus = uuid::Uuid::new_v4().to_string();
                log::note(format!("sending {kind} with attempt_id {bogus}"));
                let body = self.lifecycle_body(kind, &active, Some(&bogus));
                self.send_raw(body).await?;
            }
            "bad-token" => {
                log::note("sending a heartbeat with a garbage bearer");
                let player = self.focused();
                let body = player.heartbeat_body();
                player.send_with_token("not-a-real-token", body).await?;
            }
            "steal" => {
                let match_token = args.first().copied().context("steal <match_token>")?;
                log::note(format!("accepting {match_token} as this player"));
                let body = self.focused().accept_body(match_token);
                self.send_raw(body).await?;
            }
            "logout" => {
                let player = self.focused();
                let status = player.session.logout_access().await?;
                log::line(&player.label, format!("    = logout {status}"));
                log::note(
                    "access session revoked; automatic refresh paused, so reads return 401 \
                     until 'refresh' (the saved device credential is untouched)",
                );
            }
            "refresh" => {
                let player = self.focused();
                player.session.refresh_now().await?;
                log::line(&player.label, "    = refreshed; automatic refresh resumed");
            }
            "revoke-device" => {
                let player = self.focused();
                player.session.revoke_device().await?;
                log::line(
                    &player.label,
                    "    = device revoked on the server and cleared from its token file",
                );
            }
            "auto" => self.cmd_auto().await?,
            other => log::note(format!("unknown command '{other}' (try 'help')")),
        }
        Ok(true)
    }

    /// Build a lifecycle body, optionally with a deliberately wrong attempt id.
    fn lifecycle_body(&mut self, kind: &str, active: &ActiveMatch, attempt: Option<&str>) -> Value {
        let player = self.focused();
        match kind {
            "trying" => player.trying_body(active, attempt),
            "connect" => player.connect_body(active, attempt),
            _ => player.ready_body(active, attempt),
        }
    }

    fn recalled(&self, kind: &str) -> Result<&Value> {
        self.last
            .get(&(self.focus, kind.to_string()))
            .with_context(|| format!("no {kind} command has been sent as this player yet"))
    }

    /// Send a body, remember it, and report the outcome.
    async fn send_recording(&mut self, body: Value) -> Result<()> {
        let kind = body
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        self.last.insert((self.focus, kind), body.clone());
        self.send_raw(body).await
    }

    async fn send_raw(&mut self, body: Value) -> Result<()> {
        self.focused().send(body).await?;
        Ok(())
    }

    async fn cmd_raw(&mut self, json_text: &str) -> Result<bool> {
        let body: Value = serde_json::from_str(json_text)
            .ok()
            .context("raw expects one JSON object")?;
        self.send_raw(body).await?;
        Ok(true)
    }

    fn cmd_players(&self) {
        for (index, player) in self.players.iter().enumerate() {
            let marker = if index == self.focus { "*" } else { " " };
            log::line(
                &player.label,
                format!(
                    "{marker} steam_id={} cursor={}",
                    player.steam_id_text(),
                    self.cursors[index]
                ),
            );
        }
    }

    /// Leaving a player queued is the one piece of state that outlives this
    /// process, and it will silently pair with the next run that queues anyone
    /// — which reads like a client bug when it happens. So say so on the way out.
    async fn cleanup_notice(&mut self) {
        for index in 0..self.players.len() {
            let Ok(snapshot) = self.players[index].state_quiet().await else {
                continue;
            };
            let label = self.players[index].label.clone();
            if let Some(active) = ActiveMatch::from_state(&snapshot) {
                log::line(
                    &label,
                    format!(
                        "WARNING still in match {} ({}): 'abandon self' to leave it",
                        active.match_token, active.phase
                    ),
                );
            } else {
                let queued = text(&snapshot, "/queue/mode");
                if !queued.is_empty() {
                    log::line(
                        &label,
                        format!(
                            "WARNING still queued for {queued}: run 'cancel' first, or this \
                             player may pair with your next run instead of its peer"
                        ),
                    );
                }
            }
        }
    }

    async fn cmd_state(&mut self, which: Option<&str>) -> Result<()> {
        let indices = self.indices(which)?;
        for index in indices {
            let snapshot = self.players[index].state().await?;
            let label = self.players[index].label.clone();
            block(&label, &pretty(&snapshot));
        }
        Ok(())
    }

    async fn cmd_events(&mut self, which: Option<&str>) -> Result<()> {
        let indices = self.indices(which)?;
        for index in indices {
            let cursor = self.cursors[index];
            let page = self.players[index].events(cursor).await?;
            self.cursors[index] = page["next_cursor"].as_i64().unwrap_or(cursor);
            let label = self.players[index].label.clone();
            block(&label, &pretty(&page));
        }
        Ok(())
    }

    async fn cmd_receipts(&mut self) -> Result<()> {
        let snapshot = self.focused().state().await?;
        let label = self.focused().label.clone();
        block(&label, &pretty(&snapshot["receipts"]));
        Ok(())
    }

    fn indices(&self, which: Option<&str>) -> Result<Vec<usize>> {
        match which {
            Some("all") => Ok((0..self.players.len()).collect()),
            Some(name) => Ok(vec![self.resolve(Some(name))?]),
            None => Ok(vec![self.focus]),
        }
    }

    /// The scripted end-to-end flow, for when you want the baseline rather than
    /// an adventure.
    async fn cmd_auto(&mut self) -> Result<()> {
        let mode = self.mode.clone();
        let difficulty = self.difficulty.clone();
        log::note("running the normal two-player flow");
        for index in 0..self.players.len() {
            let body = self.players[index].queue_body(&mode, &difficulty);
            self.players[index].send_applied(body).await?;
        }
        for index in 0..self.players.len() {
            self.players[index]
                .await_queued(&mode, Duration::from_secs(15))
                .await?;
        }
        let mut actives = Vec::new();
        for index in 0..self.players.len() {
            actives.push(Some(
                self.players[index]
                    .await_pairing(Duration::from_secs(60))
                    .await?,
            ));
        }
        let active = actives[0].clone().context("no pairing")?;
        log::note(format!("paired on {}", active.match_token));

        let stages: [(&str, &str); 4] = [
            ("accept", "AwaitingTrying"),
            ("trying", "AwaitingConnect"),
            ("connect", "AwaitingReady"),
            ("ready", "Playing"),
        ];
        for (kind, phase) in stages {
            for (index, current) in actives.iter().enumerate() {
                let current = current.clone().context("no active match")?;
                let body = match kind {
                    "accept" => self.players[index].accept_body(&current.match_token),
                    "trying" => self.players[index].trying_body(&current, None),
                    "connect" => self.players[index].connect_body(&current, None),
                    _ => self.players[index].ready_body(&current, None),
                };
                self.players[index].send_applied(body).await?;
            }
            for index in 0..self.players.len() {
                self.players[index]
                    .await_phase(phase, Duration::from_secs(30))
                    .await?;
            }
        }

        let mut cursors = Vec::new();
        for index in 0..self.players.len() {
            cursors.push(self.players[index].cursor().await?);
        }
        for (index, outcome) in [(0, "win"), (1, "loss")] {
            if index >= self.players.len() {
                break;
            }
            let body = self.players[index].report_body(
                &active.match_token,
                outcome,
                "2-0",
                "deadbeef",
                900,
            );
            self.players[index].send_applied(body).await?;
        }
        for (index, cursor) in cursors.iter().enumerate() {
            let outcome = if index == 0 { "win" } else { "loss" };
            let result = self.players[index]
                .await_result(*cursor, &active.match_token, Duration::from_secs(30))
                .await?;
            let label = self.players[index].label.clone();
            log::line(
                &label,
                format!(
                    "{}  outcome={}  mmr_delta={}",
                    result.event_type,
                    result.outcome,
                    result
                        .mmr_delta
                        .map(|delta| format!("{delta:+.2}"))
                        .unwrap_or_else(|| "none".into())
                ),
            );
            if result.outcome != outcome {
                bail!(
                    "{}: expected '{outcome}', server resolved '{}'",
                    label,
                    result.outcome
                );
            }
        }
        log::note("normal flow complete");
        Ok(())
    }
}

/// Print a multi-line block under a tag.
fn block(tag: &str, body: &str) {
    for line in body.lines() {
        log::line(tag, format!("    {line}"));
    }
}

/// Pretty-print arbitrary server JSON, with every credential redacted first.
fn pretty(value: &Value) -> String {
    let value = redact(value);
    serde_json::to_string_pretty(&value).unwrap_or_else(|_| compact(&value))
}

fn parse_arg(raw: Option<&&str>, name: &str) -> Result<u64> {
    let raw = raw.with_context(|| format!("{name} needs a number"))?;
    raw.parse()
        .with_context(|| format!("{name} expects a number, got '{raw}'"))
}

/// Change exactly one *known* field, so the canonical payload differs.
///
/// Unknown extra fields would be dropped before hashing, so adding a marker
/// would *not* turn a replay into a conflict — the field has to be one the server
/// deserialises.
fn mutate(mut body: Value) -> Value {
    let flip = |current: &str| match current {
        "easy" => "normal",
        "normal" => "hard",
        _ => "easy",
    };
    let current_difficulty = body
        .get("difficulty")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    if body.get("difficulty").is_some() {
        body["difficulty"] = json!(flip(&current_difficulty));
        return body;
    }
    if body.get("score").is_some() {
        let score = body["score"].as_str().unwrap_or_default().to_string();
        body["score"] = json!(format!("{score}-changed"));
        return body;
    }
    if body.get("attempt_id").is_some() {
        body["attempt_id"] = json!(uuid::Uuid::new_v4().to_string());
        return body;
    }
    if body.get("match_token").is_some() {
        let token = body["match_token"].as_str().unwrap_or_default().to_string();
        body["match_token"] = json!(format!("{token}-changed"));
        return body;
    }
    if body.get("outcome").is_some() {
        let outcome = body["outcome"].as_str().unwrap_or_default();
        body["outcome"] = json!(if outcome == "win" { "loss" } else { "win" });
        return body;
    }
    // Nothing else to vary (heartbeat, cancel_queue): switching the type still
    // changes the canonical payload for the same command_id.
    let kind = body["type"].as_str().unwrap_or_default();
    body["type"] = json!(if kind == "heartbeat" {
        "cancel_queue"
    } else {
        "heartbeat"
    });
    body
}

/// Read stdin on a dedicated thread: reading a line blocks, and the shell's
/// event loop must stay responsive to polls and events meanwhile.
fn spawn_stdin() -> mpsc::UnboundedReceiver<String> {
    let (tx, rx) = mpsc::unbounded_channel();
    std::thread::spawn(move || {
        use std::io::BufRead;
        let stdin = std::io::stdin();
        for line in stdin.lock().lines() {
            match line {
                Ok(line) => {
                    if tx.send(line).is_err() {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx
}
