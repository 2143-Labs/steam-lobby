use std::process::Command;
use std::time::{Duration, Instant};

use lobby_client::LobbyClient;
use lobby_core::traits::MatchStore;
use lobby_core::types::{ConnectionStrategy, ResultAuthority};
use sqlx::PgPool;
use tokio::time::timeout;
use uuid::Uuid;

mod common;
use common::{TestConfig, setup, setup_with_config};

fn server_binary() -> &'static str {
    env!("CARGO_BIN_EXE_lobby-server")
}

#[test]
fn invalid_game_mode_configuration_stops_startup_before_binding() {
    let cases = [
        ("", "must contain at least one mode"),
        ("pong_1v1", "malformed entry"),
        ("pong_1v1:p2p,pong_1v1:p2p", "duplicate mode"),
        ("unknown:p2p", "unknown mode"),
        ("pong_1v1:server", "requires P2p"),
        ("server_arena:p2p", "requires Server"),
        ("pong_1v1:relay", "unknown connection"),
    ];

    for (configured, expected) in cases {
        let output = Command::new(server_binary())
            .env_remove("PUBLIC_URL")
            .env_remove("DISCORD_CLIENT_ID")
            .env_remove("DISCORD_CLIENT_SECRET")
            .env_remove("AU2143_CLIENT_ID")
            .env_remove("AU2143_CLIENT_SECRET")
            .env("GAME_MODES", configured)
            .env("DATABASE_URL", "postgres://invalid.invalid/should-not-connect")
            .env("JWT_SECRET", "reference-mode-test-secret-0123456789abcdef")
            .env("TEMPORAL_ADDRESS", "http://invalid.invalid:7233")
            .output()
            .expect("run lobby-server");
        assert!(
            !output.status.success(),
            "invalid GAME_MODES={configured:?} must stop startup"
        );
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains(expected),
            "GAME_MODES={configured:?} must report {expected:?}, stderr was {error:?}"
        );
        assert!(
            !error.contains("database connection failed"),
            "mode validation must happen before database startup"
        );
    }
}

#[sqlx::test]
async fn advertised_modes_preserve_the_public_game_type_contract(pool: PgPool) {
    let h = setup_with_config(
        pool,
        TestConfig {
            game_modes: vec![
                lobby_core::types::mode_spec("pong_1v1").unwrap(),
                lobby_core::types::mode_spec("rps_1v1").unwrap(),
                lobby_core::types::mode_spec("server_arena").unwrap(),
                lobby_core::types::mode_spec("umvc3_1v1").unwrap(),
            ],
            ..TestConfig::default()
        },
    )
    .await;

    let response = reqwest::Client::new()
        .get(format!("{}/modes", h.base_url))
        .send()
        .await
        .expect("GET /modes");
    assert!(response.status().is_success());
    let body: serde_json::Value = response.json().await.expect("modes JSON");
    assert_eq!(
        body,
        serde_json::json!({
            "modes": [
                {"name": "pong_1v1", "game_type": "p2p"},
                {"name": "rps_1v1", "game_type": "p2p"},
                {"name": "server_arena", "game_type": "server"},
                {"name": "umvc3_1v1", "game_type": "p2p"}
            ]
        }),
        "the compatibility wire field stays game_type with p2p/server values"
    );
}

async fn seed_pair(pool: &PgPool, mode: &str, suffix: i64) -> (Uuid, Uuid) {
    let a: Uuid = sqlx::query_scalar(
        "INSERT INTO users (steam_id, display_name) VALUES ($1, $2) RETURNING id",
    )
    .bind(76561198100000000_i64 + suffix * 2)
    .bind(format!("{mode}-a"))
    .fetch_one(pool)
    .await
    .expect("insert player a");
    let b: Uuid = sqlx::query_scalar(
        "INSERT INTO users (steam_id, display_name) VALUES ($1, $2) RETURNING id",
    )
    .bind(76561198100000001_i64 + suffix * 2)
    .bind(format!("{mode}-b"))
    .fetch_one(pool)
    .await
    .expect("insert player b");
    sqlx::query(
        "INSERT INTO player_state (user_id, state, last_heartbeat) \
         VALUES ($1, 'Queueing', NOW()), ($2, 'Queueing', NOW())",
    )
    .bind(a)
    .bind(b)
    .execute(pool)
    .await
    .expect("insert player states");
    sqlx::query(
        "INSERT INTO matchmaking_queue \
         (user_id, game_mode, match_difficulty, mu, queued_at, lease_expires_at) \
         VALUES ($1, $3, 'normal', 25, NOW() - INTERVAL '10 minutes', \
                 CASE WHEN $3 = 'umvc3_1v1' THEN NOW() + INTERVAL '45 seconds' ELSE NULL END), \
                ($2, $3, 'normal', 25, NOW() - INTERVAL '10 minutes', \
                 CASE WHEN $3 = 'umvc3_1v1' THEN NOW() + INTERVAL '45 seconds' ELSE NULL END)",
    )
    .bind(a)
    .bind(b)
    .bind(mode)
    .execute(pool)
    .await
    .expect("insert queue pair");
    (a, b)
}

#[sqlx::test]
async fn pairing_dispatches_connection_and_result_authority_by_mode(pool: PgPool) {
    let h = setup(pool).await;
    let cases = [
        (
            "pong_1v1",
            ConnectionStrategy::P2p,
            ResultAuthority::ServerReferee,
            false,
        ),
        (
            "rps_1v1",
            ConnectionStrategy::P2p,
            ResultAuthority::ServerReferee,
            false,
        ),
        (
            "server_arena",
            ConnectionStrategy::Server,
            ResultAuthority::Gameserver,
            true,
        ),
        (
            "umvc3_1v1",
            ConnectionStrategy::P2p,
            ResultAuthority::NativeReport,
            false,
        ),
    ];

    for (index, (mode, connection, authority, has_result_secret)) in
        cases.into_iter().enumerate()
    {
        seed_pair(&h.pool, mode, index as i64 + 1).await;
        let spec = lobby_core::types::mode_spec(mode).expect("registered mode");
        assert_eq!(spec.connection, connection, "{mode} connection policy");
        assert_eq!(spec.authority, authority, "{mode} result authority policy");

        let formed = h
            .state
            .store
            .pair_next_match(mode, spec, 300)
            .await
            .expect("pair queued players")
            .expect("compatible pair");
        assert_eq!(formed.connection, connection);
        assert_eq!(
            formed.result_secret.is_some(),
            has_result_secret,
            "only Gameserver authority receives callback credentials"
        );

        let stored: (String, Option<String>) = sqlx::query_as(
            "SELECT game_type, result_secret FROM matches WHERE match_token = $1",
        )
        .bind(&formed.match_token)
        .fetch_one(&h.pool)
        .await
        .expect("stored match compatibility fields");
        let expected_game_type = match connection {
            ConnectionStrategy::P2p => "p2p",
            ConnectionStrategy::Server => "server",
        };
        assert_eq!(stored.0, expected_game_type);
        assert_eq!(stored.1.is_some(), has_result_secret);

        let loaded = h
            .state
            .store
            .get_match(&formed.match_token)
            .await
            .expect("load match")
            .expect("stored match");
        assert_eq!(
            loaded.connection, connection,
            "legacy game_type must round-trip to ConnectionStrategy"
        );
    }
}

#[sqlx::test]
async fn non_strict_mode_still_allows_guest_gameplay_identity(pool: PgPool) {
    let h = setup(pool).await;
    let mut guest = LobbyClient::connect(&h.ws_url).await.expect("connect guest");
    let authenticated = guest
        .authenticate_guest(&h.base_url)
        .await
        .expect("non-strict guest login");
    let guest_id = Uuid::parse_str(&authenticated.player_id).expect("guest UUID");

    let account: (Option<i64>, String, i64) = sqlx::query_as(
        "SELECT u.steam_id, u.primary_provider, COUNT(a.provider) \
         FROM users u LEFT JOIN accounts a ON a.user_id = u.id \
         WHERE u.id = $1 GROUP BY u.id",
    )
    .bind(guest_id)
    .fetch_one(&h.pool)
    .await
    .expect("guest account");
    assert_eq!(
        account,
        (None, "guest".to_owned(), 0),
        "a guest is playable without impersonating a provider-backed identity"
    );
}

#[sqlx::test]
async fn match_found_wire_keeps_game_type_after_internal_mode_cutover(pool: PgPool) {
    let h = setup(pool).await;
    let mut a = LobbyClient::connect(&h.ws_url).await.expect("connect a");
    let mut b = LobbyClient::connect(&h.ws_url).await.expect("connect b");
    let auth_a = a
        .authenticate_test_token(91001, &h.base_url)
        .await
        .expect("auth a");
    let auth_b = b
        .authenticate_test_token(91002, &h.base_url)
        .await
        .expect("auth b");
    let user_a = Uuid::parse_str(&auth_a.player_id).unwrap();
    let user_b = Uuid::parse_str(&auth_b.player_id).unwrap();

    // The WebSocket auth frame is what creates each player's `player_state`
    // row (`find_or_create_user`), and it is processed asynchronously after
    // the HTTP token exchange returns. Wait for both rows before seeding
    // Queueing, otherwise one player's default `InMenus` overwrites nothing
    // yet and the pairing transaction correctly refuses the pair.
    for user in [user_a, user_b] {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let exists: bool =
                sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM player_state WHERE user_id = $1)")
                    .bind(user)
                    .fetch_one(&h.pool)
                    .await
                    .unwrap();
            if exists {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "player_state row for {user} was never created"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    sqlx::query("UPDATE player_state SET state = 'Queueing' WHERE user_id IN ($1, $2)")
        .bind(user_a)
        .bind(user_b)
        .execute(&h.pool)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO matchmaking_queue (user_id, game_mode, match_difficulty, mu, queued_at) \
         VALUES ($1, 'server_arena', 'normal', 25, NOW() - INTERVAL '10 minutes'), \
                ($2, 'server_arena', 'normal', 25, NOW() - INTERVAL '10 minutes')",
    )
    .bind(user_a)
    .bind(user_b)
    .execute(&h.pool)
    .await
    .unwrap();

    let found_a = timeout(Duration::from_secs(8), a.wait_for_match())
        .await
        .expect("player a receives match")
        .expect("client read a")
        .expect("match a");
    let found_b = timeout(Duration::from_secs(8), b.wait_for_match())
        .await
        .expect("player b receives match")
        .expect("client read b")
        .expect("match b");
    assert_eq!(found_a.match_token, found_b.match_token);
    assert_eq!(found_a.game_type, ConnectionStrategy::Server);
    assert_eq!(found_b.game_type, ConnectionStrategy::Server);

    let stored: String =
        sqlx::query_scalar("SELECT game_type FROM matches WHERE match_token = $1")
            .bind(&found_a.match_token)
            .fetch_one(&h.pool)
            .await
            .expect("stored game_type");
    assert_eq!(stored, "server");
}
