//! Durable UMVC3 ranked command admission, ordered draining, events, and snapshots.
//!
//! PostgreSQL is the command-stream authority.  A successful `dispatch` means
//! the command is durably admitted, not that it has already been applied.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use lobby_core::error::LobbyError;
use lobby_core::types::{MatchDifficulty, ResultAuthority};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

use crate::db::{RequeueDecision, StoredResolution, Umvc3Verdict};
use crate::state::AppState;

const EVENTS_PAGE_SIZE: i64 = 100;
const RECEIPTS_PAGE_SIZE: i64 = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKind {
    Native,
    Websocket,
}

impl SessionKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Websocket => "websocket",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "native" => Self::Native,
            _ => Self::Websocket,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Pending,
    Applied,
    Rejected,
}

impl CommandStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Applied => "applied",
            Self::Rejected => "rejected",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "applied" => Self::Applied,
            "rejected" => Self::Rejected,
            _ => Self::Pending,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelativeOutcome {
    Win,
    Loss,
    Draw,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AbandonTarget {
    #[serde(rename = "self")]
    SelfPlayer,
    Opponent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LobbyRole {
    Create,
    Join,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Umvc3Phase {
    AwaitingAccepted,
    AwaitingTrying,
    AwaitingConnect,
    AwaitingReady,
    Playing,
    AwaitingReport,
    Terminal,
}

impl Umvc3Phase {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::AwaitingAccepted => "AwaitingAccepted",
            Self::AwaitingTrying => "AwaitingTrying",
            Self::AwaitingConnect => "AwaitingConnect",
            Self::AwaitingReady => "AwaitingReady",
            Self::Playing => "Playing",
            Self::AwaitingReport => "AwaitingReport",
            Self::Terminal => "Terminal",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "AwaitingAccepted" => Self::AwaitingAccepted,
            "AwaitingTrying" => Self::AwaitingTrying,
            "AwaitingConnect" => Self::AwaitingConnect,
            "AwaitingReady" => Self::AwaitingReady,
            "Playing" => Self::Playing,
            "AwaitingReport" => Self::AwaitingReport,
            _ => Self::Terminal,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandActor {
    pub user_id: Uuid,
    pub session_kind: SessionKind,
    pub session_id: Uuid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RankedCommand {
    Queue {
        mode: String,
        difficulty: MatchDifficulty,
    },
    CancelQueue,
    Heartbeat,
    Accept {
        match_token: String,
    },
    Decline {
        match_token: String,
    },
    Trying {
        match_token: String,
        attempt_id: Uuid,
        role: LobbyRole,
        lobby_id: Option<String>,
    },
    Connect {
        match_token: String,
        attempt_id: Uuid,
        local_reached: bool,
        peer_reached: bool,
    },
    Ready {
        match_token: String,
        attempt_id: Uuid,
    },
    Report {
        match_token: String,
        outcome: RelativeOutcome,
        score: Option<String>,
        checksum: Option<String>,
        end_frame: Option<u64>,
    },
    Abandon {
        match_token: String,
        target: AbandonTarget,
    },
}

impl RankedCommand {
    pub fn match_token(&self) -> Option<&str> {
        match self {
            Self::Accept { match_token }
            | Self::Decline { match_token }
            | Self::Trying { match_token, .. }
            | Self::Connect { match_token, .. }
            | Self::Ready { match_token, .. }
            | Self::Report { match_token, .. }
            | Self::Abandon { match_token, .. } => Some(match_token),
            Self::Queue { .. } | Self::CancelQueue | Self::Heartbeat => None,
        }
    }

    pub const fn kind(&self) -> &'static str {
        match self {
            Self::Queue { .. } => "queue",
            Self::CancelQueue => "cancel_queue",
            Self::Heartbeat => "heartbeat",
            Self::Accept { .. } => "accept",
            Self::Decline { .. } => "decline",
            Self::Trying { .. } => "trying",
            Self::Connect { .. } => "connect",
            Self::Ready { .. } => "ready",
            Self::Report { .. } => "report",
            Self::Abandon { .. } => "abandon",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandReceipt {
    pub receipt: Uuid,
    pub status: CommandStatus,
    pub error_code: Option<String>,
    #[serde(skip)]
    pub(crate) newly_admitted: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainMatchState {
    pub match_token: String,
    pub phase: Umvc3Phase,
    pub phase_version: i64,
    pub phase_deadline: Option<DateTime<Utc>>,
    pub remaining_ms: Option<u64>,
    pub attempt_id: Uuid,
    pub terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedEvent {
    pub sequence_no: i64,
    #[serde(rename = "type")]
    pub event_type: String,
    pub match_token: String,
    pub actor: Option<Uuid>,
    pub payload: Option<Value>,
    pub time: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventsPage {
    pub events: Vec<RankedEvent>,
    pub next_cursor: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedQueueSnapshot {
    pub mode: String,
    pub difficulty: MatchDifficulty,
    pub queued_at: DateTime<Utc>,
    pub lease_expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedResultSnapshot {
    pub outcome: String,
    pub mmr_delta: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedMatchSnapshot {
    pub match_token: String,
    pub opponent: Uuid,
    pub phase: Umvc3Phase,
    pub phase_version: i64,
    pub deadline: Option<DateTime<Utc>>,
    pub attempt_id: Uuid,
    pub role: LobbyRole,
    pub lobby_id: Option<String>,
    pub trying: bool,
    pub opponent_trying: bool,
    pub connect_local: bool,
    pub connect_peer: bool,
    pub opponent_connect_local: bool,
    pub opponent_connect_peer: bool,
    pub ready: bool,
    pub opponent_ready: bool,
    pub result: Option<RankedResultSnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedReceiptSnapshot {
    pub receipt: Uuid,
    pub session_kind: SessionKind,
    pub status: CommandStatus,
    pub error_code: Option<String>,
    pub processed_event_sequence: Option<i64>,
    pub received_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RankedSnapshot {
    pub cursor: i64,
    pub queue: Option<RankedQueueSnapshot>,
    pub active_match: Option<RankedMatchSnapshot>,
    pub receipts: Vec<RankedReceiptSnapshot>,
}

#[derive(sqlx::FromRow)]
struct InboxRow {
    receipt: Uuid,
    session_kind: String,
    session_id: Uuid,
    command_id: Uuid,
    user_id: Uuid,
    payload: Value,
}

#[derive(sqlx::FromRow)]
struct LockedMatch {
    match_token: String,
    player_a: Uuid,
    player_b: Uuid,
    game_mode: String,
    player_a_difficulty: String,
    player_b_difficulty: String,
    accepted_a: bool,
    accepted_b: bool,
}

#[derive(sqlx::FromRow)]
struct LockedUmvc3 {
    phase: String,
    phase_version: i64,
    phase_deadline: Option<DateTime<Utc>>,
    attempt_id: Uuid,
    lobby_id: Option<String>,
    trying_a: bool,
    trying_b: bool,
    connect_local_a: bool,
    connect_peer_a: bool,
    connect_local_b: bool,
    connect_peer_b: bool,
    ready_a: bool,
    ready_b: bool,
    abandon_vote_a: Option<Uuid>,
    abandon_vote_b: Option<Uuid>,
}

fn db_error(error: sqlx::Error) -> LobbyError {
    LobbyError::Database(error.to_string())
}

/// Stable machine code for errors emitted by this module.  HTTP and WebSocket
/// adapters should use this rather than parsing `Display` text.
pub fn error_code(error: &LobbyError) -> &'static str {
    match error {
        LobbyError::NotParticipant(_) => "not_participant",
        LobbyError::MatchNotFound(_) => "match_state_mismatch",
        LobbyError::MatchStateMismatch(code) if code == "stale_attempt" => "stale_attempt",
        LobbyError::MatchStateMismatch(code) if code == "deadline_elapsed" => "deadline_elapsed",
        LobbyError::MatchStateMismatch(code) if code == "command_id_conflict" => "command_id_conflict",
        LobbyError::MatchStateMismatch(code) if code == "ranked_queue_disabled" => "ranked_queue_disabled",
        LobbyError::MatchStateMismatch(code) if code == "unsupported_ranked_mode" => "unsupported_ranked_mode",
        LobbyError::MatchStateMismatch(code) if code == "already_queued" => "already_queued",
        LobbyError::MatchStateMismatch(_) => "match_state_mismatch",
        LobbyError::InvalidReport(_) => "duplicate_report",
        LobbyError::PlayerNotFound(_) => "player_not_found",
        LobbyError::InvalidStateTransition { .. } => "match_state_mismatch",
        LobbyError::SteamAuthFailed(_) => "unauthorized",
        LobbyError::Database(_) => "database_error",
    }
}

fn command_error(code: &'static str) -> LobbyError {
    LobbyError::MatchStateMismatch(code.to_owned())
}

fn pool(state: &AppState) -> &PgPool {
    // Required store seam: `PostgresStore::pool(&self) -> &PgPool`.
    state.store.pool()
}

fn canonical_payload(command: &RankedCommand) -> Result<(Value, Vec<u8>), LobbyError> {
    let mut value = serde_json::to_value(command)
        .map_err(|error| LobbyError::Database(format!("serialize ranked command: {error}")))?;
    canonicalize_json(&mut value);
    let encoded = serde_json::to_vec(&value)
        .map_err(|error| LobbyError::Database(format!("encode ranked command: {error}")))?;
    Ok((value, Sha256::digest(encoded).to_vec()))
}

fn canonicalize_json(value: &mut Value) {
    match value {
        Value::Array(items) => {
            for item in items {
                canonicalize_json(item);
            }
        }
        Value::Object(object) => {
            let old = std::mem::take(object);
            let mut entries: Vec<_> = old.into_iter().collect();
            entries.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            for (key, mut item) in entries {
                canonicalize_json(&mut item);
                object.insert(key, item);
            }
        }
        _ => {}
    }
}

async fn replay_receipt(
    executor: &PgPool,
    actor: &CommandActor,
    command_id: Uuid,
    payload_hash: &[u8],
) -> Result<Option<CommandReceipt>, LobbyError> {
    let existing = sqlx::query_as::<_, (Uuid, Vec<u8>, String, Option<String>)>(
        "SELECT receipt,payload_hash,status,error_code FROM command_inbox \
         WHERE session_kind=$1 AND session_id=$2 AND command_id=$3",
    )
    .bind(actor.session_kind.as_str())
    .bind(actor.session_id)
    .bind(command_id)
    .fetch_optional(executor)
    .await
    .map_err(db_error)?;
    match existing {
        None => Ok(None),
        Some((receipt, stored_hash, status, error_code)) if stored_hash == payload_hash => {
            Ok(Some(CommandReceipt {
                receipt,
                status: CommandStatus::parse(&status),
                error_code,
                newly_admitted: false,
            }))
        }
        Some(_) => Err(command_error("command_id_conflict")),
    }
}

/// Durably and synchronously admit a ranked command.
pub async fn dispatch(
    state: &Arc<AppState>,
    actor: CommandActor,
    command_id: Uuid,
    command: RankedCommand,
) -> Result<CommandReceipt, LobbyError> {
    let (payload, payload_hash) = canonical_payload(&command)?;
    if let Some(receipt) = replay_receipt(pool(state), &actor, command_id, &payload_hash).await? {
        return Ok(receipt);
    }

    if let RankedCommand::Queue { mode, .. } = &command {
        if !state.config.ranked_queue_enabled {
            return Err(command_error("ranked_queue_disabled"));
        }
        let supported = state
            .game_modes
            .iter()
            .any(|spec| spec.id == mode && spec.authority == ResultAuthority::NativeReport);
        if !supported {
            return Err(command_error("unsupported_ranked_mode"));
        }
    }

    if command.match_token().is_some() {
        admit_match_command(state, actor, command_id, command, payload, payload_hash).await
    } else {
        admit_user_command(state, actor, command_id, command, payload, payload_hash).await
    }
}

async fn admit_user_command(
    state: &Arc<AppState>,
    actor: CommandActor,
    command_id: Uuid,
    command: RankedCommand,
    payload: Value,
    payload_hash: Vec<u8>,
) -> Result<CommandReceipt, LobbyError> {
    let mut tx = pool(state).begin().await.map_err(db_error)?;
    lock_dedupe_key(&mut tx, &actor, command_id).await?;
    sqlx::query(
        "INSERT INTO command_user_counters(user_id,next_sequence) VALUES($1,1) \
         ON CONFLICT(user_id) DO NOTHING",
    )
    .bind(actor.user_id)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;
    let next_sequence: i64 = sqlx::query_scalar(
        "SELECT next_sequence FROM command_user_counters WHERE user_id=$1 FOR UPDATE",
    )
    .bind(actor.user_id)
    .fetch_one(&mut *tx)
    .await
    .map_err(db_error)?;

    if let Some(existing) = replay_receipt_in_tx(&mut tx, &actor, command_id, &payload_hash).await? {
        tx.commit().await.map_err(db_error)?;
        return Ok(existing);
    }

    // Admission validates queue exclusivity under the same global order used by
    // draining: player_state before matchmaking_queue.
    if let RankedCommand::Queue { mode, .. } = &command {
        let state_row = sqlx::query_as::<_, (String, Option<String>)>(
            "SELECT state,active_match_token FROM player_state WHERE user_id=$1 FOR UPDATE",
        )
        .bind(actor.user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_error)?
        .ok_or(LobbyError::PlayerNotFound(actor.user_id))?;
        if state_row.1.is_some() || !matches!(state_row.0.as_str(), "InMenus" | "Queueing") {
            return Err(command_error("match_state_mismatch"));
        }
        let queued_mode: Option<String> = sqlx::query_scalar(
            "SELECT game_mode FROM matchmaking_queue WHERE user_id=$1 ORDER BY user_id FOR UPDATE",
        )
        .bind(actor.user_id)
        .fetch_optional(&mut *tx)
        .await
        .map_err(db_error)?;
        if queued_mode.as_deref().is_some_and(|queued| queued != mode) {
            return Err(command_error("already_queued"));
        }
    }

    let receipt = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO command_inbox(session_kind,session_id,command_id,receipt,user_id,user_sequence,kind,payload,payload_hash) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)",
    )
    .bind(actor.session_kind.as_str())
    .bind(actor.session_id)
    .bind(command_id)
    .bind(receipt)
    .bind(actor.user_id)
    .bind(next_sequence)
    .bind(command.kind())
    .bind(payload)
    .bind(payload_hash)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;
    sqlx::query("UPDATE command_user_counters SET next_sequence=next_sequence+1 WHERE user_id=$1")
        .bind(actor.user_id)
        .execute(&mut *tx)
        .await
        .map_err(db_error)?;
    tx.commit().await.map_err(db_error)?;
    Ok(CommandReceipt {
        receipt,
        status: CommandStatus::Pending,
        error_code: None,
        newly_admitted: true,
    })
}

async fn lock_dedupe_key(
    tx: &mut Transaction<'_, Postgres>,
    actor: &CommandActor,
    command_id: Uuid,
) -> Result<(), LobbyError> {
    let key = format!("{}:{}:{command_id}", actor.session_kind.as_str(), actor.session_id);
    sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
        .bind(key)
        .execute(&mut **tx)
        .await
        .map_err(db_error)?;
    Ok(())
}

async fn replay_receipt_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    actor: &CommandActor,
    command_id: Uuid,
    payload_hash: &[u8],
) -> Result<Option<CommandReceipt>, LobbyError> {
    let existing = sqlx::query_as::<_, (Uuid, Vec<u8>, String, Option<String>)>(
        "SELECT receipt,payload_hash,status,error_code FROM command_inbox \
         WHERE session_kind=$1 AND session_id=$2 AND command_id=$3",
    )
    .bind(actor.session_kind.as_str())
    .bind(actor.session_id)
    .bind(command_id)
    .fetch_optional(&mut **tx)
    .await
    .map_err(db_error)?;
    match existing {
        None => Ok(None),
        Some((receipt, hash, status, error_code)) if hash == payload_hash => {
            Ok(Some(CommandReceipt {
                receipt,
                status: CommandStatus::parse(&status),
                error_code,
                newly_admitted: false,
            }))
        }
        Some(_) => Err(command_error("command_id_conflict")),
    }
}

async fn admit_match_command(
    state: &Arc<AppState>,
    actor: CommandActor,
    command_id: Uuid,
    command: RankedCommand,
    payload: Value,
    payload_hash: Vec<u8>,
) -> Result<CommandReceipt, LobbyError> {
    let match_token = command.match_token().expect("match command has token");
    let mut tx = pool(state).begin().await.map_err(db_error)?;
    lock_dedupe_key(&mut tx, &actor, command_id).await?;

    // Fixed global lock order: canonical match, then UMVC3 extension.
    let participants = sqlx::query_as::<_, (Uuid, Uuid)>(
        "SELECT player_a,player_b FROM matches WHERE match_token=$1 FOR UPDATE",
    )
    .bind(match_token)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db_error)?
    .ok_or_else(|| LobbyError::MatchNotFound(match_token.to_owned()))?;
    let umvc3 = sqlx::query_as::<_, (String, i64, i64, Option<DateTime<Utc>>, Uuid, DateTime<Utc>)>(
        "SELECT phase,phase_version,next_command_sequence,phase_deadline,attempt_id,NOW() \
         FROM umvc3_matches WHERE match_token=$1 FOR UPDATE",
    )
    .bind(match_token)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db_error)?
    .ok_or_else(|| command_error("match_state_mismatch"))?;

    if let Some(existing) = replay_receipt_in_tx(&mut tx, &actor, command_id, &payload_hash).await? {
        tx.commit().await.map_err(db_error)?;
        return Ok(existing);
    }
    if actor.user_id != participants.0 && actor.user_id != participants.1 {
        return Err(LobbyError::NotParticipant(match_token.to_owned()));
    }
    let phase = Umvc3Phase::parse(&umvc3.0);
    if phase == Umvc3Phase::Terminal {
        return Err(command_error("match_state_mismatch"));
    }
    if umvc3.3.is_some_and(|deadline| umvc3.5 > deadline) {
        return Err(command_error("deadline_elapsed"));
    }
    validate_match_admission(&command, phase, umvc3.4, actor.user_id == participants.0)?;

    let receipt = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO command_inbox(session_kind,session_id,command_id,receipt,user_id,match_token,match_sequence,expected_phase_version,kind,payload,payload_hash) \
         VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)",
    )
    .bind(actor.session_kind.as_str())
    .bind(actor.session_id)
    .bind(command_id)
    .bind(receipt)
    .bind(actor.user_id)
    .bind(match_token)
    .bind(umvc3.2)
    .bind(umvc3.1)
    .bind(command.kind())
    .bind(payload)
    .bind(payload_hash)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;
    sqlx::query(
        "UPDATE umvc3_matches SET next_command_sequence=next_command_sequence+1 WHERE match_token=$1",
    )
    .bind(match_token)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;
    tx.commit().await.map_err(db_error)?;
    Ok(CommandReceipt {
        receipt,
        status: CommandStatus::Pending,
        error_code: None,
        newly_admitted: true,
    })
}

fn validate_match_admission(
    command: &RankedCommand,
    phase: Umvc3Phase,
    attempt_id: Uuid,
    actor_is_a: bool,
) -> Result<(), LobbyError> {
    let allowed = match command {
        RankedCommand::Accept { .. } | RankedCommand::Decline { .. } => {
            phase == Umvc3Phase::AwaitingAccepted
        }
        RankedCommand::Trying {
            attempt_id: supplied,
            role,
            lobby_id,
            ..
        } => {
            if *supplied != attempt_id {
                return Err(command_error("stale_attempt"));
            }
            let role_valid = if actor_is_a {
                *role == LobbyRole::Create && lobby_id.as_deref().is_some_and(|id| !id.is_empty())
            } else {
                *role == LobbyRole::Join && lobby_id.is_none()
            };
            role_valid
                && (phase == Umvc3Phase::AwaitingTrying
                    || (actor_is_a && phase == Umvc3Phase::AwaitingConnect))
        }
        RankedCommand::Connect {
            attempt_id: supplied,
            ..
        }
        | RankedCommand::Ready {
            attempt_id: supplied,
            ..
        } => {
            if *supplied != attempt_id {
                return Err(command_error("stale_attempt"));
            }
            matches!(command, RankedCommand::Connect { .. })
                && phase == Umvc3Phase::AwaitingConnect
                || matches!(command, RankedCommand::Ready { .. })
                    && phase == Umvc3Phase::AwaitingReady
        }
        RankedCommand::Report { .. } | RankedCommand::Abandon { .. } => {
            matches!(phase, Umvc3Phase::Playing | Umvc3Phase::AwaitingReport)
        }
        RankedCommand::Queue { .. } | RankedCommand::CancelQueue | RankedCommand::Heartbeat => false,
    };
    if allowed {
        Ok(())
    } else {
        Err(command_error("match_state_mismatch"))
    }
}

/// Drain every pending user and match stream. Candidate discovery is lock-free;
/// each owner is claimed and processed inside `drain_one_*`'s transaction.
pub async fn drain_pending(state: &Arc<AppState>) -> Result<usize, LobbyError> {
    let mut processed = 0;
    loop {
        // Candidate discovery takes no lock.  The subsequent drain transaction
        // claims the owner by locking matches->umvc3 (match streams) or the
        // user counter (user streams), then locks its lowest pending command.
        // Keeping SKIP LOCKED in this autocommit query would release the owner
        // lock before processing and provide a false concurrency guarantee.
        let match_owner: Option<String> = sqlx::query_scalar(
            "SELECT u.match_token FROM umvc3_matches u \
             WHERE EXISTS(SELECT 1 FROM command_inbox c WHERE c.match_token=u.match_token AND c.status='pending') \
             ORDER BY u.match_token LIMIT 1",
        )
        .fetch_optional(pool(state))
        .await
        .map_err(db_error)?;
        if let Some(match_token) = match_owner {
            processed += drain_one_match_command(state, &match_token).await? as usize;
            continue;
        }

        let user_owner: Option<Uuid> = sqlx::query_scalar(
            "SELECT c.user_id FROM command_user_counters c \
             WHERE EXISTS(SELECT 1 FROM command_inbox i WHERE i.user_id=c.user_id AND i.user_sequence IS NOT NULL AND i.status='pending') \
             ORDER BY c.user_id LIMIT 1",
        )
        .fetch_optional(pool(state))
        .await
        .map_err(db_error)?;
        if let Some(user_id) = user_owner {
            processed += drain_one_user_command(state, user_id).await? as usize;
            continue;
        }
        return Ok(processed);
    }
}

/// Drain all admitted commands for one match and return its durable timer view.
pub async fn drain_match(
    state: &Arc<AppState>,
    match_token: &str,
) -> Result<DrainMatchState, LobbyError> {
    while drain_one_match_command(state, match_token).await? {}
    load_match_state(pool(state), match_token).await
}

async fn drain_one_match_command(
    state: &Arc<AppState>,
    match_token: &str,
) -> Result<bool, LobbyError> {
    let mut tx = pool(state).begin().await.map_err(db_error)?;
    let matched = lock_match(&mut tx, match_token).await?;
    let mut umvc3 = lock_umvc3(&mut tx, match_token).await?;
    let command = sqlx::query_as::<_, InboxRow>(
        "SELECT receipt,session_kind,session_id,command_id,user_id,payload \
         FROM command_inbox WHERE match_token=$1 AND status='pending' \
         ORDER BY match_sequence ASC LIMIT 1 FOR UPDATE",
    )
    .bind(match_token)
    .fetch_optional(&mut *tx)
    .await
    .map_err(db_error)?;
    let Some(command) = command else {
        tx.rollback().await.map_err(db_error)?;
        return Ok(false);
    };

    let ranked: RankedCommand = serde_json::from_value(command.payload.clone())
        .map_err(|error| LobbyError::Database(format!("invalid persisted ranked command: {error}")))?;
    let phase = Umvc3Phase::parse(&umvc3.phase);
    let result = if phase == Umvc3Phase::Terminal {
        Err("match_state_mismatch")
    } else {
        apply_match_command(state, &mut tx, &matched, &mut umvc3, &command, ranked).await
    };

    // A policy rejection leaves the transaction healthy and is committed as the
    // command's outcome. A database failure aborts the transaction, so it must
    // be rolled back and surfaced: the claim stays `pending` and the drain loop
    // retries it instead of writing into an already-aborted transaction.
    let (status, code) = match result {
        Ok(()) => (CommandStatus::Applied, None),
        Err("database_error") => {
            tx.rollback().await.map_err(db_error)?;
            tracing::warn!(match_token, "ranked command application failed; left pending for retry");
            return Err(LobbyError::Database(format!(
                "ranked command application failed for {match_token}"
            )));
        }
        Err(code) => (CommandStatus::Rejected, Some(code)),
    };
    let event_sequence = emit_recipient_events(
        &mut tx,
        match_token,
        Some(command.user_id),
        vec![(
            command.user_id,
            "command_result",
            json!({"receipt":command.receipt,"status":status,"error":code}),
        )],
    )
    .await?
    .into_iter()
    .next();
    sqlx::query(
        "UPDATE command_inbox SET status=$1,error_code=$2,processed_event_sequence=$3,processed_at=NOW() \
         WHERE session_kind=$4 AND session_id=$5 AND command_id=$6",
    )
    .bind(status.as_str())
    .bind(code)
    .bind(event_sequence)
    .bind(&command.session_kind)
    .bind(command.session_id)
    .bind(command.command_id)
    .execute(&mut *tx)
    .await
    .map_err(db_error)?;
    tx.commit().await.map_err(db_error)?;
    Ok(true)
}

async fn apply_match_command(
    state: &Arc<AppState>,
    tx: &mut Transaction<'_, Postgres>,
    matched: &LockedMatch,
    umvc3: &mut LockedUmvc3,
    inbox: &InboxRow,
    command: RankedCommand,
) -> Result<(), &'static str> {
    let actor_is_a = inbox.user_id == matched.player_a;
    if !actor_is_a && inbox.user_id != matched.player_b {
        return Err("not_participant");
    }
    match command {
        RankedCommand::Accept { .. } => {
            if Umvc3Phase::parse(&umvc3.phase) != Umvc3Phase::AwaitingAccepted {
                return Err("match_state_mismatch");
            }
            if actor_is_a {
                sqlx::query("UPDATE matches SET accepted_a=TRUE WHERE match_token=$1")
                    .bind(&matched.match_token)
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
            } else {
                sqlx::query("UPDATE matches SET accepted_b=TRUE WHERE match_token=$1")
                    .bind(&matched.match_token)
                    .execute(&mut **tx)
                    .await
                    .map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
            }
            let both = if actor_is_a {
                matched.accepted_b
            } else {
                matched.accepted_a
            };
            emit_simple_participant_event(tx, matched, Some(inbox.user_id), "accepted", json!({})).await?;
            if both {
                transition_phase(state, tx, matched, umvc3, Umvc3Phase::AwaitingTrying).await?;
            }
        }
        RankedCommand::Decline { .. } => {
            if Umvc3Phase::parse(&umvc3.phase) != Umvc3Phase::AwaitingAccepted {
                return Err("match_state_mismatch");
            }
            let requeue = RequeueDecision {
                player_a: !actor_is_a,
                player_b: actor_is_a,
            };
            terminal_unrated(state, tx, matched, umvc3, "declined", requeue, "declined").await?;
        }
        RankedCommand::Trying {
            attempt_id,
            role,
            lobby_id,
            ..
        } => {
            if attempt_id != umvc3.attempt_id {
                return Err("stale_attempt");
            }
            let phase = Umvc3Phase::parse(&umvc3.phase);
            if actor_is_a && role == LobbyRole::Create && lobby_id.is_some() && phase == Umvc3Phase::AwaitingConnect {
                let new_attempt = Uuid::new_v4();
                let (version, deadline) = sqlx::query_as::<_, (i64, Option<DateTime<Utc>>)>(
                    "UPDATE umvc3_matches SET attempt_id=$2,lobby_id=$3,lobby_reported_at=NOW(), \
                     trying_a=TRUE,trying_b=FALSE,connect_local_a=FALSE,connect_peer_a=FALSE,connect_local_b=FALSE,connect_peer_b=FALSE,ready_a=FALSE,ready_b=FALSE, \
                     phase_version=phase_version+1,phase_deadline=NOW()+make_interval(secs=>$4) WHERE match_token=$1 \
                     RETURNING phase_version,phase_deadline",
                )
                .bind(&matched.match_token)
                .bind(new_attempt)
                .bind(&lobby_id)
                .bind(state.config.umvc3_connect_timeout_secs as f64)
                .fetch_one(&mut **tx)
                .await
                .map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                umvc3.attempt_id = new_attempt;
                umvc3.lobby_id = lobby_id.clone();
                umvc3.trying_a = true;
                umvc3.trying_b = false;
                umvc3.phase_version = version;
                umvc3.phase_deadline = deadline;
                emit_simple_participant_event(tx, matched, Some(inbox.user_id), "lobby_published", json!({"lobby_id":lobby_id,"attempt_id":new_attempt})).await?;
                emit_simple_participant_event(tx, matched, Some(inbox.user_id), "phase_changed", json!({"phase":Umvc3Phase::AwaitingConnect,"phase_version":version,"deadline":deadline,"attempt_id":new_attempt})).await?;
            } else {
                if phase != Umvc3Phase::AwaitingTrying
                    || actor_is_a != (role == LobbyRole::Create)
                    || (actor_is_a && lobby_id.is_none())
                    || (!actor_is_a && lobby_id.is_some())
                {
                    return Err("match_state_mismatch");
                }
                if actor_is_a {
                    sqlx::query("UPDATE umvc3_matches SET trying_a=TRUE,lobby_id=$2,lobby_reported_at=NOW() WHERE match_token=$1")
                        .bind(&matched.match_token).bind(&lobby_id).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                    umvc3.trying_a = true;
                    umvc3.lobby_id = lobby_id.clone();
                    emit_simple_participant_event(tx, matched, Some(inbox.user_id), "lobby_published", json!({"lobby_id":lobby_id,"attempt_id":attempt_id})).await?;
                } else {
                    sqlx::query("UPDATE umvc3_matches SET trying_b=TRUE WHERE match_token=$1")
                        .bind(&matched.match_token).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                    umvc3.trying_b = true;
                }
                if umvc3.trying_a && umvc3.trying_b {
                    transition_phase(state, tx, matched, umvc3, Umvc3Phase::AwaitingConnect).await?;
                }
            }
        }
        RankedCommand::Connect { attempt_id, local_reached, peer_reached, .. } => {
            if attempt_id != umvc3.attempt_id {
                return Err("stale_attempt");
            }
            if Umvc3Phase::parse(&umvc3.phase) != Umvc3Phase::AwaitingConnect {
                return Err("match_state_mismatch");
            }
            if actor_is_a {
                sqlx::query("UPDATE umvc3_matches SET connect_local_a=connect_local_a OR $2,connect_peer_a=connect_peer_a OR $3 WHERE match_token=$1")
                    .bind(&matched.match_token).bind(local_reached).bind(peer_reached).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                umvc3.connect_local_a |= local_reached;
                umvc3.connect_peer_a |= peer_reached;
            } else {
                sqlx::query("UPDATE umvc3_matches SET connect_local_b=connect_local_b OR $2,connect_peer_b=connect_peer_b OR $3 WHERE match_token=$1")
                    .bind(&matched.match_token).bind(local_reached).bind(peer_reached).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                umvc3.connect_local_b |= local_reached;
                umvc3.connect_peer_b |= peer_reached;
            }
            if umvc3.connect_local_a && umvc3.connect_peer_a && umvc3.connect_local_b && umvc3.connect_peer_b {
                transition_phase(state, tx, matched, umvc3, Umvc3Phase::AwaitingReady).await?;
            }
        }
        RankedCommand::Ready { attempt_id, .. } => {
            if attempt_id != umvc3.attempt_id {
                return Err("stale_attempt");
            }
            if Umvc3Phase::parse(&umvc3.phase) != Umvc3Phase::AwaitingReady {
                return Err("match_state_mismatch");
            }
            if actor_is_a {
                sqlx::query("UPDATE umvc3_matches SET ready_a=TRUE WHERE match_token=$1").bind(&matched.match_token).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                umvc3.ready_a = true;
            } else {
                sqlx::query("UPDATE umvc3_matches SET ready_b=TRUE WHERE match_token=$1").bind(&matched.match_token).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                umvc3.ready_b = true;
            }
            if umvc3.ready_a && umvc3.ready_b {
                sqlx::query("UPDATE matches SET status='Playing',started_at=COALESCE(started_at,NOW()) WHERE match_token=$1")
                    .bind(&matched.match_token).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                lock_participant_states(tx, matched).await?;
                sqlx::query("UPDATE player_state SET state='InMatch' WHERE user_id=ANY($1)")
                    .bind(&vec![matched.player_a, matched.player_b]).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                transition_phase(state, tx, matched, umvc3, Umvc3Phase::Playing).await?;
            }
        }
        RankedCommand::Report { outcome, score, checksum, end_frame, .. } => {
            let phase = Umvc3Phase::parse(&umvc3.phase);
            if !matches!(phase, Umvc3Phase::Playing | Umvc3Phase::AwaitingReport) {
                return Err("match_state_mismatch");
            }
            let inserted = sqlx::query(
                "INSERT INTO match_reports(match_token,reporting_player,winner,demo_hash,outcome,score,checksum,end_frame) \
                 VALUES($1,$2,NULL,NULL,$3,$4,$5,$6) ON CONFLICT(match_token,reporting_player) DO NOTHING",
            )
            .bind(&matched.match_token).bind(inbox.user_id)
            .bind(match outcome { RelativeOutcome::Win=>"win",RelativeOutcome::Loss=>"loss",RelativeOutcome::Draw=>"draw" })
            .bind(score).bind(checksum).bind(end_frame.map(|frame| frame as i64))
            .execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
            if inserted.rows_affected() == 0 {
                return Err("duplicate_report");
            }
            if phase == Umvc3Phase::Playing {
                transition_phase(state, tx, matched, umvc3, Umvc3Phase::AwaitingReport).await?;
            }
            resolve_evidence(state, tx, matched, umvc3).await?;
        }
        RankedCommand::Abandon { target, .. } => {
            let phase = Umvc3Phase::parse(&umvc3.phase);
            if !matches!(phase, Umvc3Phase::Playing | Umvc3Phase::AwaitingReport) {
                return Err("match_state_mismatch");
            }
            let opponent = if actor_is_a { matched.player_b } else { matched.player_a };
            let target_id = if target == AbandonTarget::SelfPlayer { inbox.user_id } else { opponent };
            let existing_vote = if actor_is_a { umvc3.abandon_vote_a } else { umvc3.abandon_vote_b };
            if existing_vote.is_some() {
                return Err("duplicate_report");
            }
            if actor_is_a {
                sqlx::query("UPDATE umvc3_matches SET abandon_vote_a=COALESCE(abandon_vote_a,$2),abandon_vote_a_at=COALESCE(abandon_vote_a_at,NOW()) WHERE match_token=$1")
                    .bind(&matched.match_token).bind(target_id).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                umvc3.abandon_vote_a = Some(target_id);
            } else {
                sqlx::query("UPDATE umvc3_matches SET abandon_vote_b=COALESCE(abandon_vote_b,$2),abandon_vote_b_at=COALESCE(abandon_vote_b_at,NOW()) WHERE match_token=$1")
                    .bind(&matched.match_token).bind(target_id).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
                umvc3.abandon_vote_b = Some(target_id);
            }
            if phase == Umvc3Phase::Playing {
                transition_phase(state, tx, matched, umvc3, Umvc3Phase::AwaitingReport).await?;
            }
            resolve_evidence(state, tx, matched, umvc3).await?;
        }
        RankedCommand::Queue { .. } | RankedCommand::CancelQueue | RankedCommand::Heartbeat => {
            return Err("match_state_mismatch");
        }
    }
    Ok(())
}

async fn resolve_evidence(
    state: &Arc<AppState>,
    tx: &mut Transaction<'_, Postgres>,
    matched: &LockedMatch,
    umvc3: &LockedUmvc3,
) -> Result<(), &'static str> {
    let reports = sqlx::query_as::<_, (Uuid, String, Option<String>, Option<String>, Option<i64>)>(
        "SELECT reporting_player,outcome,score,checksum,end_frame FROM match_reports WHERE match_token=$1 ORDER BY reporting_player",
    )
    .bind(&matched.match_token).fetch_all(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
    let normalized = reports.iter().map(|(reporter,outcome,score,checksum,end_frame)| {
        let opponent = if *reporter == matched.player_a { matched.player_b } else { matched.player_a };
        let winner = match outcome.as_str() { "win"=>Some(*reporter), "loss"=>Some(opponent), _=>None };
        (winner, score, checksum, end_frame)
    }).collect::<Vec<_>>();

    // RATIFIED RULE (2026-09-14): a missing or conflicting report resolves as
    // `Disputed` with no MMR change. Do not turn a lone report or a transport
    // loss into a forfeit: transport loss is never evidence (see ws.rs), and a
    // rated loss requires corroboration from the other participant.
    // PLANNED: on game close the client will send an explicit
    // intentional-close-vs-crash signal. When it lands, it becomes an extra
    // field on the report/abandon command and feeds this same chain — it does
    // NOT replace the corroboration requirement.
    let verdict = if normalized.len() >= 2 {
        let same = normalized[0].0 == normalized[1].0
            && normalized[0].1 == normalized[1].1
            && normalized[0].2 == normalized[1].2
            && normalized[0].3 == normalized[1].3;
        Some(if same { normalized[0].0.map_or(Umvc3Verdict::Draw, Umvc3Verdict::Win) } else { Umvc3Verdict::Disputed })
    } else if umvc3.abandon_vote_a.is_some() && umvc3.abandon_vote_b.is_some() {
        Some(if umvc3.abandon_vote_a == umvc3.abandon_vote_b {
            let loser = umvc3.abandon_vote_a.expect("both votes present");
            Umvc3Verdict::Win(if loser == matched.player_a { matched.player_b } else { matched.player_a })
        } else {
            Umvc3Verdict::Disputed
        })
    } else if normalized.len() == 1 {
        let reporter = reports[0].0;
        // Mixed evidence is bilateral: only the other participant's abandon
        // allegation can corroborate a report. A player's own two commands are
        // never sufficient to rate a match.
        let abandon = if reporter == matched.player_a {
            umvc3.abandon_vote_b
        } else {
            umvc3.abandon_vote_a
        };
        abandon.map(|loser| match normalized[0].0 {
            Some(winner) if winner != loser => Umvc3Verdict::Win(winner),
            _ => Umvc3Verdict::Disputed,
        })
    } else {
        None
    };

    if let Some(verdict) = verdict {
        let resolution: StoredResolution = state.store.finalize_umvc3_in_tx(
            tx,
            &matched.match_token,
            verdict,
            RequeueDecision { player_a:false, player_b:false },
        ).await.map_err(|error| {
            tracing::warn!(%error, "finalize_umvc3_in_tx failed");
            "database_error"
        })?;
        let _ = resolution;
    }
    Ok(())
}

async fn transition_phase(
    state: &Arc<AppState>,
    tx: &mut Transaction<'_, Postgres>,
    matched: &LockedMatch,
    umvc3: &mut LockedUmvc3,
    next: Umvc3Phase,
) -> Result<(), &'static str> {
    let timeout = match next {
        Umvc3Phase::AwaitingAccepted => state.config.match_accept_timeout_secs,
        Umvc3Phase::AwaitingTrying => state.config.umvc3_trying_timeout_secs,
        Umvc3Phase::AwaitingConnect => state.config.umvc3_connect_timeout_secs,
        Umvc3Phase::AwaitingReady => state.config.umvc3_ready_timeout_secs,
        Umvc3Phase::Playing => state.config.umvc3_play_timeout_secs,
        Umvc3Phase::AwaitingReport => state.config.report_timeout_secs,
        Umvc3Phase::Terminal => 0,
    };
    let (version, deadline) = sqlx::query_as::<_, (i64, Option<DateTime<Utc>>)>(
        "UPDATE umvc3_matches SET phase=$2,phase_version=phase_version+1, \
         phase_deadline=CASE WHEN $3::double precision=0 THEN NULL ELSE NOW()+make_interval(secs=>$3) END \
         WHERE match_token=$1 RETURNING phase_version,phase_deadline",
    )
    .bind(&matched.match_token).bind(next.as_str()).bind(timeout as f64)
    .fetch_one(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
    umvc3.phase = next.as_str().to_owned();
    umvc3.phase_version = version;
    umvc3.phase_deadline = deadline;
    emit_simple_participant_event(tx, matched, None, "phase_changed", json!({
        "phase":next,"phase_version":version,"deadline":deadline,"attempt_id":umvc3.attempt_id
    })).await
}

async fn lock_match(
    tx: &mut Transaction<'_, Postgres>,
    match_token: &str,
) -> Result<LockedMatch, LobbyError> {
    sqlx::query_as::<_, LockedMatch>(
        "SELECT match_token,player_a,player_b,game_mode,player_a_difficulty,player_b_difficulty,accepted_a,accepted_b \
         FROM matches WHERE match_token=$1 FOR UPDATE",
    )
    .bind(match_token).fetch_optional(&mut **tx).await.map_err(db_error)?
    .ok_or_else(|| LobbyError::MatchNotFound(match_token.to_owned()))
}

async fn lock_umvc3(
    tx: &mut Transaction<'_, Postgres>,
    match_token: &str,
) -> Result<LockedUmvc3, LobbyError> {
    sqlx::query_as::<_, LockedUmvc3>(
        "SELECT phase,phase_version,phase_deadline,attempt_id,lobby_id,trying_a,trying_b,connect_local_a,connect_peer_a,connect_local_b,connect_peer_b,ready_a,ready_b,abandon_vote_a,abandon_vote_b \
         FROM umvc3_matches WHERE match_token=$1 FOR UPDATE",
    )
    .bind(match_token).fetch_optional(&mut **tx).await.map_err(db_error)?
    .ok_or_else(|| command_error("match_state_mismatch"))
}

async fn lock_participant_states(
    tx: &mut Transaction<'_, Postgres>,
    matched: &LockedMatch,
) -> Result<(), &'static str> {
    let mut ids = vec![matched.player_a, matched.player_b];
    ids.sort_unstable();
    sqlx::query("SELECT user_id FROM player_state WHERE user_id=ANY($1) ORDER BY user_id FOR UPDATE")
        .bind(ids).fetch_all(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
    Ok(())
}

async fn terminal_unrated(
    state: &Arc<AppState>,
    tx: &mut Transaction<'_, Postgres>,
    matched: &LockedMatch,
    umvc3: &mut LockedUmvc3,
    reason: &'static str,
    requeue: RequeueDecision,
    event_type: &'static str,
) -> Result<(), &'static str> {
    lock_participant_states(tx, matched).await?;
    let mut ids = vec![matched.player_a, matched.player_b];
    ids.sort_unstable();
    sqlx::query("SELECT user_id FROM matchmaking_queue WHERE user_id=ANY($1) ORDER BY user_id FOR UPDATE")
        .bind(&ids).fetch_all(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;

    let original_times = sqlx::query_as::<_, (DateTime<Utc>, DateTime<Utc>)>(
        "SELECT original_queued_at_a,original_queued_at_b FROM umvc3_matches WHERE match_token=$1",
    )
    .bind(&matched.match_token)
    .fetch_one(&mut **tx)
    .await
    .map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
    for (user_id, should_requeue, difficulty, queued_at) in [
        (matched.player_a, requeue.player_a, &matched.player_a_difficulty, original_times.0),
        (matched.player_b, requeue.player_b, &matched.player_b_difficulty, original_times.1),
    ] {
        if should_requeue {
            let mu: f64 = sqlx::query_scalar(
                "INSERT INTO ratings(user_id,game_mode,mu,sigma,last_updated) VALUES($1,$2,25.0,25.0/3.0,NOW()) \
                 ON CONFLICT(user_id,game_mode) DO UPDATE SET game_mode=EXCLUDED.game_mode RETURNING mu",
            ).bind(user_id).bind(&matched.game_mode).fetch_one(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
            sqlx::query("DELETE FROM matchmaking_queue WHERE user_id=$1 AND game_mode<>$2")
                .bind(user_id).bind(&matched.game_mode).execute(&mut **tx)
                .await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
            sqlx::query(
                "INSERT INTO matchmaking_queue(user_id,game_mode,match_difficulty,mu,queued_at,lease_expires_at) \
                 VALUES($1,$2,$3,$4,$5,NOW()+make_interval(secs=>$6)) \
                 ON CONFLICT(user_id,game_mode) DO UPDATE SET match_difficulty=EXCLUDED.match_difficulty,mu=EXCLUDED.mu, \
                 queued_at=LEAST(matchmaking_queue.queued_at,EXCLUDED.queued_at),lease_expires_at=EXCLUDED.lease_expires_at",
            ).bind(user_id).bind(&matched.game_mode).bind(difficulty).bind(mu).bind(queued_at)
             .bind(state.config.ranked_queue_lease_secs as f64).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
        }
        sqlx::query(
            "UPDATE player_state SET state=$2,active_match_token=NULL WHERE user_id=$1 AND active_match_token=$3",
        ).bind(user_id).bind(if should_requeue {"Queueing"} else {"InMenus"}).bind(&matched.match_token)
         .execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
    }
    sqlx::query("UPDATE matches SET status='Resolved',ended_at=NOW() WHERE match_token=$1")
        .bind(&matched.match_token).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
    sqlx::query("UPDATE umvc3_matches SET phase='Terminal',terminal_reason=$2,phase_version=phase_version+1,phase_deadline=NULL WHERE match_token=$1")
        .bind(&matched.match_token).bind(reason).execute(&mut **tx).await.map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })?;
    umvc3.phase = "Terminal".to_owned();
    umvc3.phase_version += 1;
    emit_simple_participant_event(tx, matched, None, event_type, json!({"terminal_reason":reason,"requeue":{"player_a":requeue.player_a,"player_b":requeue.player_b}})).await
}

async fn emit_simple_participant_event(
    tx: &mut Transaction<'_, Postgres>,
    matched: &LockedMatch,
    actor: Option<Uuid>,
    event_type: &'static str,
    payload: Value,
) -> Result<(), &'static str> {
    emit_recipient_events(
        tx,
        &matched.match_token,
        actor,
        vec![(matched.player_a,event_type,payload.clone()),(matched.player_b,event_type,payload)],
    ).await.map(|_| ()).map_err(|error| { tracing::warn!(line = line!(), %error, "ranked command database error"); "database_error" })
}

/// Allocate recipient-local event numbers in ascending recipient UUID order.
pub async fn emit_recipient_events(
    tx: &mut Transaction<'_, Postgres>,
    match_token: &str,
    actor: Option<Uuid>,
    mut events: Vec<(Uuid, &'static str, Value)>,
) -> Result<Vec<i64>, LobbyError> {
    events.sort_unstable_by_key(|event| event.0);
    events.dedup_by(|left, right| left.0 == right.0 && left.1 == right.1);
    let ids: Vec<Uuid> = events.iter().map(|event| event.0).collect();
    for id in &ids {
        sqlx::query("INSERT INTO event_recipient_counters(recipient_user_id,next_sequence) VALUES($1,1) ON CONFLICT DO NOTHING")
            .bind(id).execute(&mut **tx).await.map_err(db_error)?;
    }
    sqlx::query("SELECT recipient_user_id FROM event_recipient_counters WHERE recipient_user_id=ANY($1) ORDER BY recipient_user_id FOR UPDATE")
        .bind(&ids).fetch_all(&mut **tx).await.map_err(db_error)?;

    let mut sequences = Vec::with_capacity(events.len());
    for (recipient, event_type, payload) in events {
        let sequence: i64 = sqlx::query_scalar(
            "UPDATE event_recipient_counters SET next_sequence=next_sequence+1 WHERE recipient_user_id=$1 RETURNING next_sequence-1",
        ).bind(recipient).fetch_one(&mut **tx).await.map_err(db_error)?;
        sqlx::query(
            "INSERT INTO match_events(match_token,event_type,actor_user_id,recipient_user_id,recipient_sequence,payload) VALUES($1,$2,$3,$4,$5,$6)",
        ).bind(match_token).bind(event_type).bind(actor).bind(recipient).bind(sequence).bind(payload)
         .execute(&mut **tx).await.map_err(db_error)?;
        sequences.push(sequence);
    }
    Ok(sequences)
}

async fn drain_one_user_command(state: &Arc<AppState>, user_id: Uuid) -> Result<bool, LobbyError> {
    let mut tx = pool(state).begin().await.map_err(db_error)?;
    sqlx::query("SELECT next_sequence FROM command_user_counters WHERE user_id=$1 FOR UPDATE")
        .bind(user_id).fetch_one(&mut *tx).await.map_err(db_error)?;
    let command = sqlx::query_as::<_, InboxRow>(
        "SELECT receipt,session_kind,session_id,command_id,user_id,payload \
         FROM command_inbox WHERE user_id=$1 AND user_sequence IS NOT NULL AND status='pending' \
         ORDER BY user_sequence LIMIT 1 FOR UPDATE",
    ).bind(user_id).fetch_optional(&mut *tx).await.map_err(db_error)?;
    let Some(command) = command else { tx.rollback().await.map_err(db_error)?; return Ok(false); };
    let ranked: RankedCommand = serde_json::from_value(command.payload.clone())
        .map_err(|error| LobbyError::Database(format!("invalid persisted ranked command: {error}")))?;

    sqlx::query("SELECT user_id FROM player_state WHERE user_id=$1 ORDER BY user_id FOR UPDATE")
        .bind(user_id).fetch_optional(&mut *tx).await.map_err(db_error)?
        .ok_or(LobbyError::PlayerNotFound(user_id))?;
    sqlx::query("SELECT user_id FROM matchmaking_queue WHERE user_id=$1 ORDER BY user_id FOR UPDATE")
        .bind(user_id).fetch_optional(&mut *tx).await.map_err(db_error)?;

    let result: Result<(), &'static str> = match ranked {
        RankedCommand::Queue { mode, difficulty } => {
            let active: Option<String> = sqlx::query_scalar("SELECT active_match_token FROM player_state WHERE user_id=$1")
                .bind(user_id).fetch_one(&mut *tx).await.map_err(db_error)?;
            let other: Option<String> = sqlx::query_scalar("SELECT game_mode FROM matchmaking_queue WHERE user_id=$1 AND game_mode<>$2")
                .bind(user_id).bind(&mode).fetch_optional(&mut *tx).await.map_err(db_error)?;
            if active.is_some() { Err("match_state_mismatch") }
            else if other.is_some() { Err("already_queued") }
            else {
                let mu: f64 = sqlx::query_scalar(
                    "INSERT INTO ratings(user_id,game_mode,mu,sigma,last_updated) VALUES($1,$2,25.0,25.0/3.0,NOW()) \
                     ON CONFLICT(user_id,game_mode) DO UPDATE SET game_mode=EXCLUDED.game_mode RETURNING mu",
                ).bind(user_id).bind(&mode).fetch_one(&mut *tx).await.map_err(db_error)?;
                sqlx::query(
                    "INSERT INTO matchmaking_queue(user_id,game_mode,match_difficulty,mu,queued_at,lease_expires_at) \
                     VALUES($1,$2,$3,$4,NOW(),NOW()+make_interval(secs=>$5)) \
                     ON CONFLICT(user_id,game_mode) DO UPDATE SET match_difficulty=EXCLUDED.match_difficulty,mu=EXCLUDED.mu,lease_expires_at=EXCLUDED.lease_expires_at",
                ).bind(user_id).bind(mode).bind(format!("{difficulty:?}").to_lowercase()).bind(mu)
                 .bind(state.config.ranked_queue_lease_secs as f64).execute(&mut *tx).await.map_err(db_error)?;
                sqlx::query("UPDATE player_state SET state='Queueing',last_heartbeat=NOW() WHERE user_id=$1")
                    .bind(user_id).execute(&mut *tx).await.map_err(db_error)?;
                Ok(())
            }
        }
        RankedCommand::CancelQueue => {
            sqlx::query("DELETE FROM matchmaking_queue WHERE user_id=$1 AND game_mode='umvc3_1v1'").bind(user_id).execute(&mut *tx).await.map_err(db_error)?;
            sqlx::query("UPDATE player_state SET state='InMenus' WHERE user_id=$1 AND active_match_token IS NULL AND NOT EXISTS(SELECT 1 FROM matchmaking_queue WHERE user_id=$1)")
                .bind(user_id).execute(&mut *tx).await.map_err(db_error)?;
            Ok(())
        }
        RankedCommand::Heartbeat => {
            sqlx::query("UPDATE player_state SET last_heartbeat=NOW() WHERE user_id=$1")
                .bind(user_id).execute(&mut *tx).await.map_err(db_error)?;
            if SessionKind::parse(&command.session_kind) == SessionKind::Native {
                sqlx::query("UPDATE native_sessions SET last_seen_at=NOW() WHERE session_id=$1 AND user_id=$2 AND revoked_at IS NULL AND expires_at>NOW()")
                    .bind(command.session_id).bind(user_id).execute(&mut *tx).await.map_err(db_error)?;
            }
            sqlx::query(
                "UPDATE matchmaking_queue SET lease_expires_at=NOW()+make_interval(secs=>$2) \
                 WHERE user_id=$1 AND game_mode='umvc3_1v1' AND lease_expires_at IS NOT NULL",
            ).bind(user_id).bind(state.config.ranked_queue_lease_secs as f64)
             .execute(&mut *tx).await.map_err(db_error)?;
            Ok(())
        }
        _ => Err("match_state_mismatch"),
    };
    let (status, code) = match result { Ok(())=>(CommandStatus::Applied,None),Err(code)=>(CommandStatus::Rejected,Some(code)) };
    sqlx::query("UPDATE command_inbox SET status=$1,error_code=$2,processed_at=NOW() WHERE session_kind=$3 AND session_id=$4 AND command_id=$5")
        .bind(status.as_str()).bind(code).bind(&command.session_kind).bind(command.session_id).bind(command.command_id)
        .execute(&mut *tx).await.map_err(db_error)?;
    tx.commit().await.map_err(db_error)?;
    Ok(true)
}

/// Expire a phase only if the workflow-observed version is still current.
pub async fn expire(
    state: &Arc<AppState>,
    match_token: &str,
    expected_phase_version: i64,
) -> Result<DrainMatchState, LobbyError> {
    // Admission and expiry serialize on the same match locks. Drain first so
    // every command durably admitted before the deadline wins even after a
    // process/workflow restart; the resulting phase-version change fences a
    // stale workflow timer.
    while drain_one_match_command(state, match_token).await? {}
    let mut tx = pool(state).begin().await.map_err(db_error)?;
    let matched = lock_match(&mut tx, match_token).await?;
    let mut umvc3 = lock_umvc3(&mut tx, match_token).await?;
    let now: DateTime<Utc> = sqlx::query_scalar("SELECT NOW()").fetch_one(&mut *tx).await.map_err(db_error)?;
    let phase = Umvc3Phase::parse(&umvc3.phase);
    if phase != Umvc3Phase::Terminal
        && umvc3.phase_version == expected_phase_version
        && umvc3.phase_deadline.is_some_and(|deadline| now >= deadline)
    {
        let requeue = match phase {
            Umvc3Phase::AwaitingAccepted => RequeueDecision { player_a:matched.accepted_a, player_b:matched.accepted_b },
            Umvc3Phase::AwaitingTrying => RequeueDecision { player_a:umvc3.trying_a, player_b:umvc3.trying_b },
            Umvc3Phase::AwaitingConnect => RequeueDecision { player_a:true, player_b:true },
            Umvc3Phase::AwaitingReady => RequeueDecision { player_a:umvc3.ready_a, player_b:umvc3.ready_b },
            Umvc3Phase::Playing | Umvc3Phase::AwaitingReport => {
                emit_simple_participant_event(
                    &mut tx,
                    &matched,
                    None,
                    "phase_expired",
                    json!({
                        "terminal_reason": if phase == Umvc3Phase::Playing { "playing_timeout" } else { "disputed" },
                        "requeue": { "player_a": false, "player_b": false }
                    }),
                )
                .await
                .map_err(command_error)?;
                state.store.finalize_umvc3_in_tx(
                    &mut tx,
                    match_token,
                    Umvc3Verdict::Disputed,
                    RequeueDecision { player_a:false,player_b:false },
                ).await?;
                tx.commit().await.map_err(db_error)?;
                return load_match_state(pool(state), match_token).await;
            }
            Umvc3Phase::Terminal => unreachable!(),
        };
        let reason = match phase {
            Umvc3Phase::AwaitingAccepted=>"accept_timeout",
            Umvc3Phase::AwaitingTrying=>"trying_timeout",
            Umvc3Phase::AwaitingConnect=>"connect_timeout",
            Umvc3Phase::AwaitingReady=>"ready_timeout",
            Umvc3Phase::Playing=>"playing_timeout",
            Umvc3Phase::AwaitingReport=>"disputed",
            Umvc3Phase::Terminal=>unreachable!(),
        };
        terminal_unrated(state,&mut tx,&matched,&mut umvc3,reason,requeue,"phase_expired")
            .await.map_err(command_error)?;
    }
    tx.commit().await.map_err(db_error)?;
    load_match_state(pool(state), match_token).await
}

async fn load_match_state(pool: &PgPool, match_token: &str) -> Result<DrainMatchState, LobbyError> {
    let row = sqlx::query(
        "SELECT phase,phase_version,phase_deadline,attempt_id,NOW() AS db_now FROM umvc3_matches WHERE match_token=$1",
    ).bind(match_token).fetch_optional(pool).await.map_err(db_error)?
     .ok_or_else(|| command_error("match_state_mismatch"))?;
    let phase_text: String = row.try_get("phase").map_err(db_error)?;
    let deadline: Option<DateTime<Utc>> = row.try_get("phase_deadline").map_err(db_error)?;
    let now: DateTime<Utc> = row.try_get("db_now").map_err(db_error)?;
    let phase = Umvc3Phase::parse(&phase_text);
    Ok(DrainMatchState {
        match_token:match_token.to_owned(),
        phase,
        phase_version:row.try_get("phase_version").map_err(db_error)?,
        phase_deadline:deadline,
        remaining_ms:deadline.map(|value| (value-now).num_milliseconds().max(0) as u64),
        attempt_id:row.try_get("attempt_id").map_err(db_error)?,
        terminal:phase==Umvc3Phase::Terminal,
    })
}

/// Fetch one recipient-local page.  The route owns the 25-second Notify/timeout
/// loop and simply repeats this helper when `events` is empty.
pub async fn get_events(state: &Arc<AppState>, user_id: Uuid, after: i64) -> Result<EventsPage, LobbyError> {
    let rows = sqlx::query_as::<_, (i64,String,String,Option<Uuid>,Option<Value>,DateTime<Utc>)>(
        "SELECT recipient_sequence,event_type,match_token,actor_user_id,payload,created_at \
         FROM match_events WHERE recipient_user_id=$1 AND recipient_sequence>$2 \
         ORDER BY recipient_sequence ASC LIMIT $3",
    ).bind(user_id).bind(after.max(0)).bind(EVENTS_PAGE_SIZE)
     .fetch_all(pool(state)).await.map_err(db_error)?;
    let events = rows.into_iter().map(|(sequence_no,event_type,match_token,actor,payload,time)| RankedEvent {
        sequence_no,event_type,match_token,actor,payload,time,
    }).collect::<Vec<_>>();
    let next_cursor = events.last().map_or(after.max(0), |event| event.sequence_no);
    Ok(EventsPage { events,next_cursor })
}

/// Read a caller-only ranked snapshot under one REPEATABLE READ transaction.
pub async fn get_ranked_snapshot(state: &Arc<AppState>, user_id: Uuid) -> Result<RankedSnapshot, LobbyError> {
    let mut tx = pool(state).begin().await.map_err(db_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx).await.map_err(db_error)?;
    let cursor: i64 = sqlx::query_scalar(
        "SELECT COALESCE((SELECT next_sequence-1 FROM event_recipient_counters WHERE recipient_user_id=$1),0)",
    ).bind(user_id).fetch_one(&mut *tx).await.map_err(db_error)?;
    let queue_row = sqlx::query_as::<_, (String,String,DateTime<Utc>,Option<DateTime<Utc>>)>(
        "SELECT game_mode,match_difficulty,queued_at,lease_expires_at FROM matchmaking_queue WHERE user_id=$1 AND game_mode='umvc3_1v1' ORDER BY queued_at LIMIT 1",
    ).bind(user_id).fetch_optional(&mut *tx).await.map_err(db_error)?;
    let queue = queue_row.map(|(mode,difficulty,queued_at,lease_expires_at)| RankedQueueSnapshot {
        mode,difficulty:parse_difficulty(&difficulty),queued_at,lease_expires_at,
    });

    let active_row = sqlx::query(
        "SELECT m.match_token,m.player_a,m.player_b,u.phase,u.phase_version,u.phase_deadline,u.attempt_id,u.lobby_id, \
         u.trying_a,u.trying_b,u.connect_local_a,u.connect_peer_a,u.connect_local_b,u.connect_peer_b,u.ready_a,u.ready_b, \
         r.outcome,r.mu_change_a,r.mu_change_b \
         FROM player_state ps JOIN matches m ON m.match_token=ps.active_match_token \
         JOIN umvc3_matches u ON u.match_token=m.match_token LEFT JOIN match_results r ON r.match_token=m.match_token \
         WHERE ps.user_id=$1",
    ).bind(user_id).fetch_optional(&mut *tx).await.map_err(db_error)?;
    let active_match = active_row.map(|row| {
        let player_a: Uuid = row.get("player_a");
        let player_b: Uuid = row.get("player_b");
        let is_a = user_id == player_a;
        let result_outcome: Option<String> = row.get("outcome");
        let result = result_outcome.map(|outcome| {
            let relative = if is_a { outcome } else { flip_outcome(&outcome).to_owned() };
            RankedResultSnapshot { outcome:relative, mmr_delta:row.get(if is_a {"mu_change_a"} else {"mu_change_b"}) }
        });
        RankedMatchSnapshot {
            match_token:row.get("match_token"), opponent:if is_a {player_b} else {player_a},
            phase:Umvc3Phase::parse(row.get::<String,_>("phase").as_str()), phase_version:row.get("phase_version"),
            deadline:row.get("phase_deadline"),attempt_id:row.get("attempt_id"),role:if is_a {LobbyRole::Create}else{LobbyRole::Join},
            lobby_id:row.get("lobby_id"),trying:row.get(if is_a{"trying_a"}else{"trying_b"}),opponent_trying:row.get(if is_a{"trying_b"}else{"trying_a"}),
            connect_local:row.get(if is_a{"connect_local_a"}else{"connect_local_b"}),connect_peer:row.get(if is_a{"connect_peer_a"}else{"connect_peer_b"}),
            opponent_connect_local:row.get(if is_a{"connect_local_b"}else{"connect_local_a"}),opponent_connect_peer:row.get(if is_a{"connect_peer_b"}else{"connect_peer_a"}),
            ready:row.get(if is_a{"ready_a"}else{"ready_b"}),opponent_ready:row.get(if is_a{"ready_b"}else{"ready_a"}),result,
        }
    });
    let receipt_rows = sqlx::query_as::<_, (Uuid,String,String,Option<String>,Option<i64>,DateTime<Utc>)>(
        "SELECT receipt,session_kind,status,error_code,processed_event_sequence,received_at FROM command_inbox \
         WHERE user_id=$1 ORDER BY received_at DESC,receipt DESC LIMIT $2",
    ).bind(user_id).bind(RECEIPTS_PAGE_SIZE).fetch_all(&mut *tx).await.map_err(db_error)?;
    let receipts = receipt_rows.into_iter().map(|(receipt,session_kind,status,error_code,processed_event_sequence,received_at)| RankedReceiptSnapshot {
        receipt,session_kind:SessionKind::parse(&session_kind),status:CommandStatus::parse(&status),error_code,processed_event_sequence,received_at,
    }).collect();
    tx.commit().await.map_err(db_error)?;
    Ok(RankedSnapshot { cursor,queue,active_match,receipts })
}

fn parse_difficulty(value: &str) -> MatchDifficulty {
    match value { "easy"=>MatchDifficulty::Easy,"hard"=>MatchDifficulty::Hard,_=>MatchDifficulty::Normal }
}

fn flip_outcome(value: &str) -> &str {
    match value { "Win"|"win"=>"Loss","Loss"|"loss"=>"Win",other=>other }
}
