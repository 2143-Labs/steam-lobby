//! Durable ranked HTTP surface for the native client: command admission,
//! recipient-local event long-poll, and a caller-only ranked snapshot.
//!
//! These handlers are thin adapters. All ordering, dedupe, and state authority
//! live in [`crate::commands`]; PostgreSQL is canonical and Temporal only
//! supplies timers, so an admitted command survives a lost signal or restart.
use std::sync::Arc;
use std::time::Duration;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::Deserialize;
use uuid::Uuid;

use crate::commands::{self, CommandActor, RankedCommand, SessionKind};
use crate::state::AppState;
use crate::steam_auth::ValidatedSession;

/// How long one `/api/events` request may wait for new events before returning
/// an empty page with an unchanged cursor. Clients immediately repeat.
const EVENTS_LONG_POLL: Duration = Duration::from_secs(25);

#[derive(Deserialize)]
pub struct CommandBody {
    command_id: Uuid,
    #[serde(flatten)]
    command: RankedCommand,
}

#[derive(Deserialize)]
pub struct EventsQuery {
    after: Option<i64>,
}

/// The actor identity for an admitted ranked command.
///
/// `POST /api/command` is the native client's path: it requires a live native
/// bearer session, whose durable session id becomes the command-stream owner.
/// Browser sessions drive the same service over the WebSocket instead.
async fn native_actor(state: &AppState, headers: &HeaderMap) -> Option<(CommandActor, Uuid)> {
    let ValidatedSession::Native(claims) =
        crate::routes::authenticate_headers(state, headers, false).await?
    else {
        return None;
    };
    let user_id = Uuid::parse_str(&claims.sub).ok()?;
    Some((
        CommandActor {
            user_id,
            session_kind: SessionKind::Native,
            session_id: claims.sid,
        },
        user_id,
    ))
}

/// The caller for read-only ranked endpoints: either session kind is fine, but
/// the result is always scoped to that caller's own user id.
async fn reader(state: &AppState, headers: &HeaderMap) -> Option<Uuid> {
    let session = crate::routes::authenticate_headers(state, headers, false).await?;
    let claims = match session {
        ValidatedSession::Browser(claims) | ValidatedSession::Native(claims) => claims,
    };
    Uuid::parse_str(&claims.sub).ok()
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn error_response(error: &lobby_core::error::LobbyError) -> Response {
    let code = commands::error_code(error);
    let status = match code {
        "unauthorized" => StatusCode::UNAUTHORIZED,
        "not_participant" => StatusCode::FORBIDDEN,
        "player_not_found" => StatusCode::NOT_FOUND,
        "ranked_queue_disabled" | "provider_unavailable" => StatusCode::SERVICE_UNAVAILABLE,
        "database_error" => StatusCode::INTERNAL_SERVER_ERROR,
        _ => StatusCode::CONFLICT,
    };
    (status, Json(serde_json::json!({ "error": code }))).into_response()
}

/// `POST /api/command` — durably admit one ranked command.
///
/// `202` with the fresh receipt on first admission; `200` with the persisted
/// receipt on an identical replay; `409 command_id_conflict` when the same
/// command id arrives with a different payload.
pub async fn api_command(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(body): Json<CommandBody>,
) -> Response {
    let Some((actor, _user_id)) = native_actor(&state, &headers).await else {
        return (StatusCode::UNAUTHORIZED, Json(serde_json::json!({
            "error": "native_session_required"
        })))
            .into_response();
    };

    match commands::dispatch(&state, actor, body.command_id, body.command).await {
        Ok(receipt) => {
            let payload = serde_json::json!({
                "receipt": receipt.receipt,
                "status": receipt.status.as_str(),
                "error_code": receipt.error_code,
            });
            if receipt.newly_admitted {
                (StatusCode::ACCEPTED, Json(payload)).into_response()
            } else {
                (StatusCode::OK, Json(payload)).into_response()
            }
        }
        Err(error) => error_response(&error),
    }
}

/// `GET /api/events?after=<cursor>` — recipient-local event page.
///
/// Holds the request for at most 25 seconds so a native client can long-poll
/// without busy-looping. The cursor is the last returned sequence, or the
/// requested one when the page is empty, so no event is ever skipped.
pub async fn api_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(query): Query<EventsQuery>,
) -> Response {
    let Some(user_id) = reader(&state, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let after = query.after.unwrap_or(0).max(0);
    let deadline = tokio::time::Instant::now() + EVENTS_LONG_POLL;

    loop {
        match commands::get_events(&state, user_id, after).await {
            Ok(page) => {
                if !page.events.is_empty() || tokio::time::Instant::now() >= deadline {
                    return no_store(Json(page).into_response());
                }
            }
            Err(error) => return no_store(error_response(&error)),
        }
        // Wake as soon as the drain loop applies a command that emits events;
        // otherwise fall through on the poll timeout and re-query once.
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            continue;
        }
        let _ = tokio::time::timeout(remaining, state.event_notify.notified()).await;
    }
}

/// `GET /api/ranked/state` — caller-only queue, active match, and receipts.
pub async fn api_ranked_state(State(state): State<Arc<AppState>>, headers: HeaderMap) -> Response {
    let Some(user_id) = reader(&state, &headers).await else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match commands::get_ranked_snapshot(&state, user_id).await {
        Ok(snapshot) => no_store(Json(snapshot).into_response()),
        Err(error) => no_store(error_response(&error)),
    }
}

fn service_unavailable(code: &str) -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(serde_json::json!({ "error": code })),
    )
        .into_response()
}

/// `GET /ready` — readiness gate for the ranked Deployment.
///
/// Liveness (`/health`) only proves the process is up. Readiness additionally
/// proves the canonical store answers, the in-process Temporal worker is
/// connected (its client is cleared when the worker exits, so a stale handle
/// can never report ready), the client's configured Temporal namespace is
/// actually usable, and no UMVC3 match is left non-terminal while its durable
/// workflow is already closed. Steam/Discord are deliberately not part of
/// readiness: a provider outage must not take the service out of rotation.
pub async fn ready(State(state): State<Arc<AppState>>) -> Response {
    if let Err(error) = sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(state.store.pool())
        .await
    {
        tracing::warn!(%error, "readiness: canonical store unreachable");
        return service_unavailable("database_unavailable");
    }

    let Some(client) = state.temporal.read().ok().and_then(|slot| slot.clone()) else {
        return service_unavailable("temporal_worker_unavailable");
    };

    // A connected client is not proof the namespace exists: every call still
    // fails when the namespace was never created, which is exactly the state
    // the worker's poll loop swallows. Count in the configured namespace so a
    // missing/renamed namespace takes the pod out of rotation.
    if let Err(error) = client
        .count_workflows("", temporalio_client::WorkflowCountOptions::default())
        .await
    {
        tracing::error!(%error, "readiness: temporal namespace unusable");
        return service_unavailable("temporal_namespace_unavailable");
    }

    let tokens = match state.store.nonterminal_umvc3_tokens().await {
        Ok(tokens) => tokens,
        Err(error) => {
            tracing::warn!(%error, "readiness: non-terminal match query failed");
            return service_unavailable("database_unavailable");
        }
    };
    for token in tokens {
        let handle = client
            .get_workflow_handle::<crate::temporal::umvc3::UMVC3MatchWorkflow>(
                crate::temporal::umvc3::workflow_id(&token),
            );
        // A missing execution is transient by design — the reconciler starts
        // it — so only a *closed* execution under non-terminal canonical state
        // is the invariant violation this probe refuses to serve.
        if let Ok(description) = handle.describe(Default::default()).await {
            let status = description.status();
            if !matches!(
                status,
                temporalio_client::WorkflowExecutionStatus::Running
                    | temporalio_client::WorkflowExecutionStatus::Unspecified
                    | temporalio_client::WorkflowExecutionStatus::Unknown
            ) {
                tracing::error!(
                    match_token = %token,
                    ?status,
                    "readiness: closed workflow for a non-terminal match"
                );
                return service_unavailable("workflow_invariant_violation");
            }
        }
    }

    (StatusCode::OK, "ok").into_response()
}
