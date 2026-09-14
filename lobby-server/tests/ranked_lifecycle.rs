use lobby_server::commands::{
    self, AbandonTarget, CommandActor, LobbyRole, RankedCommand, RelativeOutcome, SessionKind,
    Umvc3Phase,
};
use lobby_server::{RequeueDecision, Umvc3Verdict};
use uuid::Uuid;

mod common;
use common::{TestConfig, TestHarness, setup_with_config};

fn ranked_config() -> TestConfig {
    TestConfig {
        game_modes: vec![lobby_core::types::mode_spec("umvc3_1v1").unwrap()],
        match_accept_timeout_secs: 15,
        umvc3_trying_timeout_secs: 15,
        umvc3_connect_timeout_secs: 30,
        umvc3_ready_timeout_secs: 60,
        umvc3_play_timeout_secs: 7200,
        report_timeout_secs: 300,
        ..TestConfig::default()
    }
}

fn actor(user_id: Uuid, session_id: Uuid) -> CommandActor {
    CommandActor { user_id, session_kind: SessionKind::Native, session_id }
}

async fn seed_match(
    h: &TestHarness,
    a: Uuid,
    b: Uuid,
    phase: &str,
    difficulty_a: &str,
    difficulty_b: &str,
) -> (String, Uuid) {
    let token = Uuid::new_v4().to_string();
    let attempt = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO matches(match_token,player_a,player_a_difficulty,player_b,player_b_difficulty,game_mode,game_type,status,created_at) \
         VALUES($1,$2,$3,$4,$5,'umvc3_1v1','p2p',$6,NOW())",
    )
    .bind(&token)
    .bind(a)
    .bind(difficulty_a)
    .bind(b)
    .bind(difficulty_b)
    .bind(if phase == "Playing" { "Playing" } else { "PendingAccept" })
    .execute(&h.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO umvc3_matches(match_token,phase,phase_deadline,attempt_id,original_queued_at_a,original_queued_at_b) \
         VALUES($1,$2,NOW()+INTERVAL '1 hour',$3,NOW()-INTERVAL '9 minutes',NOW()-INTERVAL '4 minutes')",
    )
    .bind(&token)
    .bind(phase)
    .bind(attempt)
    .execute(&h.pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE player_state SET state=$1,active_match_token=$2 WHERE user_id=ANY($3)",
    )
    .bind(if phase == "Playing" { "InMatch" } else { "MatchAccepted" })
    .bind(&token)
    .bind(&[a, b][..])
    .execute(&h.pool)
    .await
    .unwrap();
    (token, attempt)
}

async fn apply(h: &TestHarness, who: CommandActor, command: RankedCommand) {
    commands::dispatch(&h.state, who, Uuid::new_v4(), command)
        .await
        .expect("command admitted");
    commands::drain_pending(&h.state).await.expect("command applied");
}

async fn progress_to_playing(
    h: &TestHarness,
    a: CommandActor,
    b: CommandActor,
    token: &str,
    attempt: Uuid,
) {
    for who in [a.clone(), b.clone()] {
        apply(h, who, RankedCommand::Accept { match_token: token.into() }).await;
    }
    apply(
        h,
        a.clone(),
        RankedCommand::Trying {
            match_token: token.into(), attempt_id: attempt, role: LobbyRole::Create,
            lobby_id: Some("109775242781234567".into()),
        },
    ).await;
    apply(
        h,
        b.clone(),
        RankedCommand::Trying {
            match_token: token.into(), attempt_id: attempt, role: LobbyRole::Join, lobby_id: None,
        },
    ).await;
    for who in [a.clone(), b.clone()] {
        apply(
            h,
            who,
            RankedCommand::Connect {
                match_token: token.into(), attempt_id: attempt,
                local_reached: true, peer_reached: true,
            },
        ).await;
    }
    for who in [a, b] {
        apply(h, who, RankedCommand::Ready { match_token: token.into(), attempt_id: attempt }).await;
    }
}

#[sqlx::test]
async fn bilateral_barriers_and_replacement_attempt_reset_connection_facts(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, ranked_config()).await;
    let (a, sa, _) = h.native_principal(91001).await;
    let (b, sb, _) = h.native_principal(91002).await;
    let (token, attempt) = seed_match(&h, a, b, "AwaitingAccepted", "normal", "hard").await;
    let aa = actor(a, sa);
    let ab = actor(b, sb);

    for who in [aa.clone(), ab.clone()] {
        apply(&h, who, RankedCommand::Accept { match_token: token.clone() }).await;
    }
    let after_accept = commands::drain_match(&h.state, &token).await.unwrap();
    assert_eq!(after_accept.phase, Umvc3Phase::AwaitingTrying);

    apply(&h, aa.clone(), RankedCommand::Trying {
        match_token: token.clone(), attempt_id: attempt, role: LobbyRole::Create,
        lobby_id: Some("lobby-one".into()),
    }).await;
    apply(&h, ab.clone(), RankedCommand::Trying {
        match_token: token.clone(), attempt_id: attempt, role: LobbyRole::Join, lobby_id: None,
    }).await;
    apply(&h, aa.clone(), RankedCommand::Connect {
        match_token: token.clone(), attempt_id: attempt, local_reached: true, peer_reached: true,
    }).await;

    apply(&h, aa.clone(), RankedCommand::Trying {
        match_token: token.clone(), attempt_id: attempt, role: LobbyRole::Create,
        lobby_id: Some("lobby-two".into()),
    }).await;
    let row: (Uuid, bool, bool, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT attempt_id,connect_local_a,connect_peer_a,connect_local_b,connect_peer_b,ready_a,ready_b \
         FROM umvc3_matches WHERE match_token=$1",
    ).bind(&token).fetch_one(&h.pool).await.unwrap();
    assert_ne!(row.0, attempt, "replacement lobby must fence commands from the old attempt");
    assert_eq!((row.1, row.2, row.3, row.4, row.5, row.6), (false, false, false, false, false, false));

    let stale = commands::dispatch(&h.state, ab, Uuid::new_v4(), RankedCommand::Connect {
        match_token: token, attempt_id: attempt, local_reached: true, peer_reached: true,
    }).await.unwrap_err();
    assert_eq!(commands::error_code(&stale), "stale_attempt");
}

#[sqlx::test]
async fn matching_reports_rate_exactly_once_and_metadata_conflicts_never_rate(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, ranked_config()).await;
    let (a, sa, _) = h.native_principal(92001).await;
    let (b, sb, _) = h.native_principal(92002).await;
    let aa = actor(a, sa);
    let ab = actor(b, sb);

    let (rated, attempt) = seed_match(&h, a, b, "AwaitingAccepted", "easy", "hard").await;
    progress_to_playing(&h, aa.clone(), ab.clone(), &rated, attempt).await;
    for (who, outcome) in [(aa.clone(), RelativeOutcome::Win), (ab.clone(), RelativeOutcome::Loss)] {
        apply(&h, who, RankedCommand::Report {
            match_token: rated.clone(), outcome, score: Some("3-1".into()),
            checksum: Some("abc123".into()), end_frame: Some(88_001),
        }).await;
    }
    let before_retry: Vec<(Uuid, f64)> = sqlx::query_as(
        "SELECT user_id,mu FROM ratings WHERE game_mode='umvc3_1v1' AND user_id=ANY($1) ORDER BY user_id",
    ).bind(&[a, b][..]).fetch_all(&h.pool).await.unwrap();
    assert_eq!(before_retry.len(), 2);
    assert!(before_retry.iter().any(|(_, mu)| *mu > 25.0));
    assert!(before_retry.iter().any(|(_, mu)| *mu < 25.0));
    let retry = h.state.store.finalize_umvc3(
        &rated, Umvc3Verdict::Win(a), RequeueDecision::default(),
    ).await.unwrap();
    assert!(!retry.newly_finalized);
    let after_retry: Vec<(Uuid, f64)> = sqlx::query_as(
        "SELECT user_id,mu FROM ratings WHERE game_mode='umvc3_1v1' AND user_id=ANY($1) ORDER BY user_id",
    ).bind(&[a, b][..]).fetch_all(&h.pool).await.unwrap();
    assert_eq!(after_retry, before_retry, "finalizer retry must not apply a second MMR change");
    let result_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM match_results WHERE match_token=$1")
        .bind(&rated).fetch_one(&h.pool).await.unwrap();
    assert_eq!(result_count, 1);

    for field in ["score", "checksum", "end_frame"] {
        let (token, _) = seed_match(&h, a, b, "Playing", "normal", "normal").await;
        let (score_b, checksum_b, frame_b) = match field {
            "score" => (Some("3-2"), Some("same"), Some(10_u64)),
            "checksum" => (Some("3-0"), Some("different"), Some(10_u64)),
            _ => (Some("3-0"), Some("same"), Some(11_u64)),
        };
        apply(&h, aa.clone(), RankedCommand::Report {
            match_token: token.clone(), outcome: RelativeOutcome::Win,
            score: Some("3-0".into()), checksum: Some("same".into()), end_frame: Some(10),
        }).await;
        apply(&h, ab.clone(), RankedCommand::Report {
            match_token: token.clone(), outcome: RelativeOutcome::Loss,
            score: score_b.map(str::to_owned), checksum: checksum_b.map(str::to_owned), end_frame: frame_b,
        }).await;
        let result: (String, Option<f64>, Option<f64>) = sqlx::query_as(
            "SELECT outcome,mu_change_a,mu_change_b FROM match_results WHERE match_token=$1",
        ).bind(&token).fetch_one(&h.pool).await.unwrap();
        assert_eq!(result, ("Disputed".into(), None, None), "{field} conflict must be unrated");
    }
}

#[sqlx::test]
async fn phase_expiry_requeues_only_valid_responders_with_original_priority(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, ranked_config()).await;
    let (a, _, _) = h.native_principal(93001).await;
    let (b, _, _) = h.native_principal(93002).await;

    enum Facts { AcceptedA, TryingB, ConnectA, ReadyA }
    struct Case { phase: &'static str, facts: Facts, expected_a: bool, expected_b: bool, reason: &'static str }
    let cases = [
        Case { phase: "AwaitingAccepted", facts: Facts::AcceptedA, expected_a: true, expected_b: false, reason: "accept_timeout" },
        Case { phase: "AwaitingTrying", facts: Facts::TryingB, expected_a: false, expected_b: true, reason: "trying_timeout" },
        Case { phase: "AwaitingConnect", facts: Facts::ConnectA, expected_a: true, expected_b: true, reason: "connect_timeout" },
        Case { phase: "AwaitingReady", facts: Facts::ReadyA, expected_a: true, expected_b: false, reason: "ready_timeout" },
    ];
    for case in cases {
        sqlx::query("DELETE FROM matchmaking_queue WHERE user_id=ANY($1)").bind(&[a,b][..]).execute(&h.pool).await.unwrap();
        sqlx::query("UPDATE player_state SET state='InMenus',active_match_token=NULL WHERE user_id=ANY($1)").bind(&[a,b][..]).execute(&h.pool).await.unwrap();
        let (token, _) = seed_match(&h, a, b, case.phase, "easy", "hard").await;
        let statement = match case.facts {
            Facts::AcceptedA => "UPDATE matches SET accepted_a=TRUE WHERE match_token=$1",
            Facts::TryingB => "UPDATE umvc3_matches SET trying_b=TRUE WHERE match_token=$1",
            Facts::ConnectA => "UPDATE umvc3_matches SET connect_local_a=TRUE WHERE match_token=$1",
            Facts::ReadyA => "UPDATE umvc3_matches SET ready_a=TRUE WHERE match_token=$1",
        };
        sqlx::query(statement).bind(&token).execute(&h.pool).await.unwrap();
        sqlx::query("UPDATE umvc3_matches SET phase_deadline=NOW()-INTERVAL '1 second' WHERE match_token=$1")
            .bind(&token).execute(&h.pool).await.unwrap();
        let version: i64 = sqlx::query_scalar("SELECT phase_version FROM umvc3_matches WHERE match_token=$1")
            .bind(&token).fetch_one(&h.pool).await.unwrap();
        let terminal = commands::expire(&h.state, &token, version).await.unwrap();
        assert!(terminal.terminal);
        let reason: String = sqlx::query_scalar("SELECT terminal_reason FROM umvc3_matches WHERE match_token=$1")
            .bind(&token).fetch_one(&h.pool).await.unwrap();
        assert_eq!(reason, case.reason);
        let queued: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT user_id,match_difficulty FROM matchmaking_queue WHERE user_id=ANY($1) ORDER BY user_id",
        ).bind(&[a,b][..]).fetch_all(&h.pool).await.unwrap();
        assert_eq!(queued.iter().any(|row| row.0 == a), case.expected_a);
        assert_eq!(queued.iter().any(|row| row.0 == b), case.expected_b);
        if case.expected_a { assert!(queued.contains(&(a, "easy".into()))); }
        if case.expected_b { assert!(queued.contains(&(b, "hard".into()))); }
    }
}

#[sqlx::test]
async fn bilateral_abandon_agreement_resolves_but_conflicting_targets_dispute(pool: sqlx::PgPool) {
    let h = setup_with_config(pool, ranked_config()).await;
    let (a, sa, _) = h.native_principal(94001).await;
    let (b, sb, _) = h.native_principal(94002).await;
    let aa = actor(a, sa);
    let ab = actor(b, sb);

    let (agreed, _) = seed_match(&h, a, b, "Playing", "normal", "normal").await;
    apply(&h, aa.clone(), RankedCommand::Abandon { match_token: agreed.clone(), target: AbandonTarget::Opponent }).await;
    apply(&h, ab.clone(), RankedCommand::Abandon { match_token: agreed.clone(), target: AbandonTarget::SelfPlayer }).await;
    let outcome: String = sqlx::query_scalar("SELECT outcome FROM match_results WHERE match_token=$1")
        .bind(&agreed).fetch_one(&h.pool).await.unwrap();
    assert_eq!(outcome, "Win", "both actors identified player B as loser");

    let (conflict, _) = seed_match(&h, a, b, "Playing", "normal", "normal").await;
    apply(&h, aa, RankedCommand::Abandon { match_token: conflict.clone(), target: AbandonTarget::SelfPlayer }).await;
    apply(&h, ab, RankedCommand::Abandon { match_token: conflict.clone(), target: AbandonTarget::SelfPlayer }).await;
    let disputed: (String, Option<f64>) = sqlx::query_as(
        "SELECT outcome,mu_change_a FROM match_results WHERE match_token=$1",
    ).bind(&conflict).fetch_one(&h.pool).await.unwrap();
    assert_eq!(disputed, ("Disputed".into(), None));
}

#[sqlx::test]
async fn readiness_gates_on_store_worker_and_workflow_invariant(pool: sqlx::PgPool) {
    // Without a Temporal worker the ranked service must refuse traffic even
    // though the process (and /health) is fine.
    let cold = setup_with_config(pool.clone(), ranked_config()).await;
    let http = reqwest::Client::new();
    let cold_ready = http
        .get(format!("{}/ready", cold.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        cold_ready.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "readiness must require the Temporal worker"
    );
    drop(cold);

    let mut config = ranked_config();
    config.temporal_enabled = true;
    let h = setup_with_config(pool, config).await;
    let warm_ready = http
        .get(format!("{}/ready", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(warm_ready.status(), reqwest::StatusCode::OK);

    // A canonical non-terminal match whose workflow execution has closed is the
    // one state the service must not serve: admission would target a match no
    // workflow can ever advance.
    let (a, _, _) = h.native_principal(92001).await;
    let (b, _, _) = h.native_principal(92002).await;
    let (token, _attempt) = seed_match(&h, a, b, "AwaitingAccepted", "normal", "normal").await;
    let workflow_id = format!("match-{token}");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
    loop {
        if h.workflow_status(&workflow_id).await.is_some() {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the reconciler never started the match workflow"
        );
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }
    let client = h
        .state
        .temporal
        .read()
        .unwrap()
        .clone()
        .expect("worker client");
    client
        .get_workflow_handle::<temporalio_client::UntypedWorkflow>(workflow_id)
        .terminate(temporalio_client::WorkflowTerminateOptions::default())
        .await
        .unwrap();
    let broken = http
        .get(format!("{}/ready", h.base_url))
        .send()
        .await
        .unwrap();
    assert_eq!(
        broken.status(),
        reqwest::StatusCode::SERVICE_UNAVAILABLE,
        "a closed workflow under non-terminal state must fail readiness"
    );
}
