use std::borrow::Cow;

use sqlx::migrate::{Migration, MigrationType, Migrator};
use sqlx::{PgPool, SqlSafeStr};
use uuid::Uuid;

static PRODUCTION_MIGRATOR: Migrator = sqlx::migrate!("./migrations");

fn migration(
    version: i64,
    description: &'static str,
    sql: &'static str,
    no_tx: bool,
) -> Migration {
    Migration::new(
        version,
        Cow::Borrowed(description),
        MigrationType::Simple,
        sql.into_sql_str(),
        no_tx,
    )
}

fn legacy_migrator() -> Migrator {
    Migrator::with_migrations(vec![
        migration(
            20240723000001,
            "initial",
            include_str!("../migrations/20240723000001_initial.sql"),
            false,
        ),
        migration(
            20260803000001,
            "security",
            include_str!("../migrations/20260803000001_security.sql"),
            false,
        ),
        migration(
            20260805000001,
            "game types",
            include_str!("../migrations/20260805000001_game_types.sql"),
            false,
        ),
        migration(
            20260805000002,
            "match events",
            include_str!("../migrations/20260805000002_match_events.sql"),
            false,
        ),
        migration(
            20260807000001,
            "identity",
            include_str!("../migrations/20260807000001_identity.sql"),
            false,
        ),
        migration(
            20260809000001,
            "uuid players",
            include_str!("../migrations/20260809000001_uuid_players.sql"),
            false,
        ),
    ])
}

fn ranked_migrator() -> Migrator {
    let mut migrator = Migrator::with_migrations(
        PRODUCTION_MIGRATOR
            .iter()
            .filter(|migration| {
                (20260915000001..=20260915000008).contains(&migration.version)
            })
            .cloned()
            .collect(),
    );
    migrator.set_ignore_missing(true);
    migrator
}

async fn migrate_legacy(pool: &PgPool) {
    legacy_migrator().run(pool).await.expect("legacy migrations");
}

async fn seed_legacy_ranked_data(pool: &PgPool) -> (Uuid, Uuid, String) {
    let first: Uuid = sqlx::query_scalar(
        "INSERT INTO users (steam_id, display_name) VALUES (76561198000000001, 'legacy-a') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("legacy user a");
    let second: Uuid = sqlx::query_scalar(
        "INSERT INTO users (steam_id, display_name) VALUES (76561198000000002, 'legacy-b') RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("legacy user b");

    sqlx::query(
        "INSERT INTO user_identities (provider, provider_uid, user_id, last_login_at) \
         VALUES ('steam', '76561198000000001', $1, '2026-01-02 03:04:05+00')",
    )
    .bind(first)
    .execute(pool)
    .await
    .expect("legacy identity");
    sqlx::query("INSERT INTO player_state (user_id, state) VALUES ($1, 'Queueing'), ($2, 'InMenus')")
        .bind(first)
        .bind(second)
        .execute(pool)
        .await
        .expect("legacy player state");
    sqlx::query(
        "INSERT INTO ratings (user_id, game_mode, mu, sigma) VALUES ($1, 'ranked_1v1', 31.5, 4.25)",
    )
    .bind(first)
    .execute(pool)
    .await
    .expect("legacy rating");
    sqlx::query(
        "INSERT INTO matchmaking_queue (user_id, game_mode, match_difficulty, mu, queued_at) \
         VALUES ($1, 'ranked_1v1', 'hard', 31.5, '2026-01-01 00:00:00+00')",
    )
    .bind(first)
    .execute(pool)
    .await
    .expect("legacy queue");

    let token = "legacy-ranked-match".to_owned();
    sqlx::query(
        "INSERT INTO matches \
         (match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, game_type) \
         VALUES ($1, $2, 'hard', $3, 'normal', 'ranked_1v1', 'p2p')",
    )
    .bind(&token)
    .bind(first)
    .bind(second)
    .execute(pool)
    .await
    .expect("legacy match");
    sqlx::query(
        "INSERT INTO match_events (match_token, event_type, user_id) VALUES ($1, 'accepted', $2)",
    )
    .bind(&token)
    .bind(first)
    .execute(pool)
    .await
    .expect("legacy event");

    (first, second, token)
}

#[sqlx::test(migrations = false)]
async fn populated_legacy_database_migrates_without_losing_compatibility(pool: PgPool) {
    migrate_legacy(&pool).await;
    let (first, second, token) = seed_legacy_ranked_data(&pool).await;

    ranked_migrator()
        .run(&pool)
        .await
        .expect("ranked migrations over populated legacy schema");

    let rating: (String, f64, f64) = sqlx::query_as(
        "SELECT game_mode, mu, sigma FROM ratings WHERE user_id = $1",
    )
    .bind(first)
    .fetch_one(&pool)
    .await
    .expect("migrated rating");
    assert_eq!(rating, ("pong_1v1".to_owned(), 31.5, 4.25));

    let queue: (String, String, f64, bool) = sqlx::query_as(
        "SELECT game_mode, match_difficulty, mu, lease_expires_at IS NULL \
         FROM matchmaking_queue WHERE user_id = $1",
    )
    .bind(first)
    .fetch_one(&pool)
    .await
    .expect("migrated queue");
    assert_eq!(queue, ("pong_1v1".to_owned(), "hard".to_owned(), 31.5, true));

    let mode: String = sqlx::query_scalar("SELECT game_mode FROM matches WHERE match_token = $1")
        .bind(&token)
        .fetch_one(&pool)
        .await
        .expect("migrated match mode");
    assert_eq!(mode, "pong_1v1");

    let account: (Uuid, chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>) =
        sqlx::query_as(
            "SELECT user_id, last_login_at, linked_at FROM accounts \
             WHERE provider = 'steam' AND provider_uid = '76561198000000001'",
        )
        .fetch_one(&pool)
        .await
        .expect("legacy identity through compatibility view");
    assert_eq!(account.0, first);
    assert_eq!(account.1, account.2, "linked_at is backfilled from login time");

    sqlx::query(
        "INSERT INTO accounts (provider, provider_uid, user_id, last_login_at, linked_at) \
         VALUES ('discord', 'legacy-discord', $1, NOW(), NOW())",
    )
    .bind(first)
    .execute(&pool)
    .await
    .expect("new writer inserts through accounts view");
    let physical_owner: Uuid = sqlx::query_scalar(
        "SELECT user_id FROM user_identities WHERE provider = 'discord' AND provider_uid = 'legacy-discord'",
    )
    .fetch_one(&pool)
    .await
    .expect("old writer sees view insert");
    assert_eq!(physical_owner, first);

    sqlx::query(
        "INSERT INTO user_identities (provider, provider_uid, user_id, last_login_at) \
         VALUES ('au2143', 'legacy-oidc', $1, NOW())",
    )
    .bind(second)
    .execute(&pool)
    .await
    .expect("old writer inserts physical identity");
    let view_owner: Uuid = sqlx::query_scalar(
        "SELECT user_id FROM accounts WHERE provider = 'au2143' AND provider_uid = 'legacy-oidc'",
    )
    .fetch_one(&pool)
    .await
    .expect("new writer sees old insert");
    assert_eq!(view_owner, second);

    let actor: (Option<Uuid>, Option<Uuid>) = sqlx::query_as(
        "SELECT user_id, actor_user_id FROM match_events WHERE match_token = $1",
    )
    .bind(&token)
    .fetch_one(&pool)
    .await
    .expect("backfilled event actor");
    assert_eq!(actor, (Some(first), Some(first)));

    let old_writer_event: i64 = sqlx::query_scalar(
        "INSERT INTO match_events (match_token, event_type, user_id) \
         VALUES ($1, 'accepted', $2) RETURNING id",
    )
    .bind(&token)
    .bind(second)
    .fetch_one(&pool)
    .await
    .expect("old event writer");
    let mirrored_new: Option<Uuid> =
        sqlx::query_scalar("SELECT actor_user_id FROM match_events WHERE id = $1")
            .bind(old_writer_event)
            .fetch_one(&pool)
            .await
            .expect("new actor mirror");
    assert_eq!(mirrored_new, Some(second));

    let new_writer_event: i64 = sqlx::query_scalar(
        "INSERT INTO match_events (match_token, event_type, actor_user_id) \
         VALUES ($1, 'command_result', $2) RETURNING id",
    )
    .bind(&token)
    .bind(first)
    .fetch_one(&pool)
    .await
    .expect("new event writer");
    let mirrored_old: Option<Uuid> =
        sqlx::query_scalar("SELECT user_id FROM match_events WHERE id = $1")
            .bind(new_writer_event)
            .fetch_one(&pool)
            .await
            .expect("legacy actor mirror");
    assert_eq!(mirrored_old, Some(first));

    let mismatch = sqlx::query(
        "INSERT INTO match_events (match_token, event_type, user_id, actor_user_id) \
         VALUES ($1, 'accepted', $2, $3)",
    )
    .bind(&token)
    .bind(first)
    .bind(second)
    .execute(&pool)
    .await;
    assert!(mismatch.is_err(), "trigger must reject contradictory actor columns");

    let applied_ranked: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM _sqlx_migrations WHERE version BETWEEN 20260915000001 AND 20260915000008 AND success",
    )
    .fetch_one(&pool)
    .await
    .expect("ranked migration records");
    // Seven ranked migrations (000005 was folded away): the transactional
    // schema plus one no-transaction concurrent-index migration each. The
    // partial UNIQUE index on (recipient_user_id, recipient_sequence) is also
    // the delivery index, so no duplicate non-unique index is created.
    assert_eq!(applied_ranked, 7);

    let valid_indexes: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid \
         WHERE c.relname = ANY($1) AND i.indisvalid AND i.indisready",
    )
    .bind(&vec![
        "browser_sessions_user_provider_idx",
        "player_state_active_match_token_idx",
        "match_events_recipient_sequence_uidx",
        "matchmaking_queue_native_lease_idx",
        "match_reports_token_reporter_uidx",
    ])
    .fetch_one(&pool)
    .await
    .expect("concurrent indexes");
    assert_eq!(valid_indexes, 5, "all concurrent indexes must be usable");

    ranked_migrator()
        .run(&pool)
        .await
        .expect("rerunning migrator is idempotent");
    let still_once: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM _sqlx_migrations WHERE version BETWEEN 20260915000001 AND 20260915000008",
    )
    .fetch_one(&pool)
    .await
    .expect("migration records after rerun");
    assert_eq!(still_once, 7, "no-transaction migrations are recorded once");
}

#[sqlx::test(migrations = false)]
async fn ranked_schema_enforces_durable_state_constraints(pool: PgPool) {
    migrate_legacy(&pool).await;
    ranked_migrator().run(&pool).await.expect("ranked migrations");

    let a: Uuid = sqlx::query_scalar(
        "INSERT INTO users (steam_id, display_name) VALUES (76561198000000011, 'a') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let b: Uuid = sqlx::query_scalar(
        "INSERT INTO users (steam_id, display_name) VALUES (76561198000000012, 'b') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query("INSERT INTO player_state (user_id, state) VALUES ($1, 'InMenus'), ($2, 'InMenus')")
        .bind(a)
        .bind(b)
        .execute(&pool)
        .await
        .unwrap();

    let state_hash = vec![1_u8; 32];
    let nonce_hash = vec![2_u8; 32];
    sqlx::query(
        "INSERT INTO oauth_login_states \
         (state_hash, browser_nonce_hash, provider, return_to, code_verifier, expires_at) \
         VALUES ($1, $2, 'discord', '/link', 'persisted-pkce-secret', NOW() + INTERVAL '10 minutes')",
    )
    .bind(&state_hash)
    .bind(&nonce_hash)
    .execute(&pool)
    .await
    .expect("durable oauth login state");
    let oauth: (Vec<u8>, String, String, Option<String>) = sqlx::query_as(
        "SELECT browser_nonce_hash, provider, return_to, code_verifier \
         FROM oauth_login_states WHERE state_hash = $1",
    )
    .bind(&state_hash)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        oauth,
        (
            nonce_hash,
            "discord".to_owned(),
            "/link".to_owned(),
            Some("persisted-pkce-secret".to_owned())
        )
    );
    assert!(
        sqlx::query(
            "INSERT INTO oauth_login_states \
             (state_hash, browser_nonce_hash, provider, return_to, expires_at) \
             VALUES ($1, $2, 'github', '/', NOW() + INTERVAL '1 minute')",
        )
        .bind(vec![3_u8; 32])
        .bind(vec![4_u8; 32])
        .execute(&pool)
        .await
        .is_err(),
        "unsupported OAuth providers must be rejected"
    );

    sqlx::query(
        "INSERT INTO matchmaking_queue \
         (user_id, game_mode, match_difficulty, mu, lease_expires_at) \
         VALUES ($1, 'umvc3_1v1', 'normal', 25, NOW() + INTERVAL '45 seconds')",
    )
    .bind(a)
    .execute(&pool)
    .await
    .expect("native queue lease");
    let lease_live: bool = sqlx::query_scalar(
        "SELECT lease_expires_at > NOW() FROM matchmaking_queue WHERE user_id = $1",
    )
    .bind(a)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(lease_live);

    let token = "constraint-match";
    sqlx::query(
        "INSERT INTO matches \
         (match_token, player_a, player_a_difficulty, player_b, player_b_difficulty, game_mode, game_type) \
         VALUES ($1, $2, 'normal', $3, 'normal', 'umvc3_1v1', 'p2p')",
    )
    .bind(token)
    .bind(a)
    .bind(b)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO umvc3_matches \
         (match_token, phase, original_queued_at_a, original_queued_at_b) \
         VALUES ($1, 'AwaitingAccepted', NOW(), NOW())",
    )
    .bind(token)
    .execute(&pool)
    .await
    .expect("valid UMVC3 state");
    assert!(
        sqlx::query(
            "UPDATE umvc3_matches SET phase = 'ConnectedButNotReady' WHERE match_token = $1",
        )
        .bind(token)
        .execute(&pool)
        .await
        .is_err(),
        "unknown lifecycle phase must be rejected"
    );

    sqlx::query("INSERT INTO command_user_counters (user_id, next_sequence) VALUES ($1, 2)")
        .bind(a)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO event_recipient_counters (recipient_user_id, next_sequence) VALUES ($1, 2)")
        .bind(a)
        .execute(&pool)
        .await
        .unwrap();

    let session = Uuid::new_v4();
    let command = Uuid::new_v4();
    let receipt: Uuid = sqlx::query_scalar(
        "INSERT INTO command_inbox \
         (session_kind, session_id, command_id, user_id, user_sequence, kind, payload, payload_hash) \
         VALUES ('native', $1, $2, $3, 1, 'queue', '{\"type\":\"queue\"}', $4) RETURNING receipt",
    )
    .bind(session)
    .bind(command)
    .bind(a)
    .bind(vec![9_u8; 32])
    .fetch_one(&pool)
    .await
    .expect("valid durable command");
    assert!(
        sqlx::query(
            "INSERT INTO command_inbox \
             (session_kind, session_id, command_id, user_id, user_sequence, kind, payload, payload_hash) \
             VALUES ('native', $1, $2, $3, 2, 'heartbeat', '{\"type\":\"heartbeat\"}', $4)",
        )
        .bind(session)
        .bind(command)
        .bind(a)
        .bind(vec![8_u8; 32])
        .execute(&pool)
        .await
        .is_err(),
        "session-scoped command IDs must deduplicate"
    );
    let persisted_receipt: Uuid = sqlx::query_scalar(
        "SELECT receipt FROM command_inbox \
         WHERE session_kind = 'native' AND session_id = $1 AND command_id = $2",
    )
    .bind(session)
    .bind(command)
    .fetch_one(&pool)
    .await
    .expect("deduplicated command remains intact");
    assert_eq!(persisted_receipt, receipt);
    assert!(
        sqlx::query(
            "INSERT INTO command_inbox \
             (session_kind, session_id, command_id, user_id, user_sequence, kind, payload, payload_hash) \
             VALUES ('native', $1, $2, $3, 1, 'heartbeat', '{\"type\":\"heartbeat\"}', $4)",
        )
        .bind(Uuid::new_v4())
        .bind(Uuid::new_v4())
        .bind(a)
        .bind(vec![7_u8; 32])
        .execute(&pool)
        .await
        .is_err(),
        "user stream sequence cannot be reused across sessions"
    );
    assert!(
        sqlx::query(
            "UPDATE command_inbox SET status = 'lost' WHERE receipt = $1",
        )
        .bind(receipt)
        .execute(&pool)
        .await
        .is_err(),
        "unknown command status must be rejected"
    );

    sqlx::query(
        "INSERT INTO match_events \
         (match_token, event_type, recipient_user_id, recipient_sequence, payload) \
         VALUES ($1, 'paired', $2, 1, '{}')",
    )
    .bind(token)
    .bind(a)
    .execute(&pool)
    .await
    .expect("valid recipient-local event");
    assert!(
        sqlx::query(
            "INSERT INTO match_events \
             (match_token, event_type, recipient_user_id, recipient_sequence) \
             VALUES ($1, 'phase_changed', $2, 1)",
        )
        .bind(token)
        .bind(a)
        .execute(&pool)
        .await
        .is_err(),
        "recipient-local event sequence must be unique"
    );
    assert!(
        sqlx::query(
            "INSERT INTO match_events \
             (match_token, event_type, recipient_user_id) VALUES ($1, 'paired', $2)",
        )
        .bind(token)
        .bind(b)
        .execute(&pool)
        .await
        .is_err(),
        "recipient and positive sequence must be supplied together"
    );
    assert!(
        sqlx::query(
            "INSERT INTO match_reports \
             (match_token, reporting_player, outcome, end_frame) \
             VALUES ($1, $2, 'win', -1)",
        )
        .bind(token)
        .bind(a)
        .execute(&pool)
        .await
        .is_err(),
        "negative end frames must be rejected"
    );

    let missing_user = sqlx::query(
        "INSERT INTO command_inbox \
         (session_kind, session_id, command_id, user_id, user_sequence, kind, payload, payload_hash) \
         VALUES ('websocket', $1, $2, $3, 1, 'heartbeat', '{}', $4)",
    )
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(Uuid::new_v4())
    .bind(vec![6_u8; 32])
    .execute(&pool)
    .await;
    assert!(missing_user.is_err(), "command actor must reference a real user");

    let invalid_link_kind = sqlx::query(
        "INSERT INTO provider_sessions \
         (nonce_hash, provider, provider_uid, display_name, expires_at) \
         VALUES ($1, 'discord', 'd-1', 'Discord User', NOW() + INTERVAL '5 minutes')",
    )
    .bind(vec![5_u8; 32])
    .execute(&pool)
    .await
    .expect("provider proof");
    assert_eq!(invalid_link_kind.rows_affected(), 1);
    let browser_session: Uuid = sqlx::query_scalar(
        "INSERT INTO browser_sessions (user_id, auth_provider, csrf_hash, expires_at) \
         VALUES ($1, 'steam', $2, NOW() + INTERVAL '1 hour') RETURNING session_id",
    )
    .bind(b)
    .bind(vec![4_u8; 32])
    .fetch_one(&pool)
    .await
    .expect("browser session");
    let native_session: Uuid = sqlx::query_scalar(
        "INSERT INTO native_sessions (user_id, expires_at) \
         VALUES ($1, NOW() + INTERVAL '1 hour') RETURNING session_id",
    )
    .bind(b)
    .fetch_one(&pool)
    .await
    .expect("native session");
    assert!(
        sqlx::query(
            "INSERT INTO link_intents \
             (user_id, kind, provider_session_hash, browser_session_id, native_session_id) \
             VALUES ($1, 'browser', $2, $3, $4)",
        )
        .bind(b)
        .bind(vec![5_u8; 32])
        .bind(browser_session)
        .bind(native_session)
        .execute(&pool)
        .await
        .is_err(),
        "browser intent cannot also bind a native session"
    );

    let delete_actor = sqlx::query("DELETE FROM users WHERE id = $1")
        .bind(a)
        .execute(&pool)
        .await;
    assert!(
        delete_actor.is_err(),
        "durable command history must restrict deletion of its actor"
    );
}

#[sqlx::test(migrations = false)]
async fn conflicting_legacy_mode_rows_abort_before_any_schema_change(pool: PgPool) {
    migrate_legacy(&pool).await;
    let user: Uuid = sqlx::query_scalar(
        "INSERT INTO users (steam_id, display_name) VALUES (76561198000000021, 'conflict') RETURNING id",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO ratings (user_id, game_mode, mu, sigma) \
         VALUES ($1, 'ranked_1v1', 31, 4), ($1, 'pong_1v1', 18, 7)",
    )
    .bind(user)
    .execute(&pool)
    .await
    .unwrap();

    let err = ranked_migrator()
        .run(&pool)
        .await
        .expect_err("conflicting ratings must stop migration");
    assert!(
        err.to_string().contains("conflicting ratings"),
        "migration must identify the unsafe collision: {err}"
    );

    let rows: Vec<(String, f64, f64)> = sqlx::query_as(
        "SELECT game_mode, mu, sigma FROM ratings WHERE user_id = $1 ORDER BY game_mode",
    )
    .bind(user)
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        rows,
        vec![
            ("pong_1v1".to_owned(), 18.0, 7.0),
            ("ranked_1v1".to_owned(), 31.0, 4.0),
        ],
        "the guard must not merge or overwrite either rating"
    );

    let linked_at_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = 'user_identities' AND column_name = 'linked_at')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        !linked_at_exists,
        "transactional migration must roll back earlier DDL when the guard aborts"
    );
}
