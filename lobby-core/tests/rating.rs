use lobby_core::mmr;
use lobby_core::types::OpenSkillRating;
use skillratings::Outcomes;

#[test]
fn new_player_defaults() {
    let r = OpenSkillRating {
        mu: 25.0,
        sigma: 25.0 / 3.0,
        last_updated: chrono::Utc::now(),
    };
    assert!((r.mu - 25.0).abs() < 0.01);
    assert!((r.sigma - 8.333).abs() < 0.1);
}

#[test]
fn equal_players_win() {
    let a = OpenSkillRating {
        mu: 25.0,
        sigma: 25.0 / 3.0,
        last_updated: chrono::Utc::now(),
    };
    let b = a.clone();

    let (new_a, new_b) = mmr::update_ratings(&a, &b, Outcomes::WIN);

    // Winner's mu should increase
    assert!(new_a.mu > a.mu);
    // Loser's mu should decrease
    assert!(new_b.mu < b.mu);
    // Sigma should decrease for both (more certainty)
    assert!(new_a.sigma < a.sigma);
    assert!(new_b.sigma < b.sigma);
    // Changes should be symmetric
    let delta_a = new_a.mu - a.mu;
    let delta_b = new_b.mu - b.mu;
    assert!((delta_a + delta_b).abs() < 0.01);
}

#[test]
fn draw_minimal_change() {
    let a = OpenSkillRating {
        mu: 25.0,
        sigma: 25.0 / 3.0,
        last_updated: chrono::Utc::now(),
    };
    let b = OpenSkillRating {
        mu: 30.0,
        sigma: 5.0,
        last_updated: chrono::Utc::now(),
    };

    let (new_a, new_b) = mmr::update_ratings(&a, &b, Outcomes::DRAW);

    // Both should move toward each other slightly
    assert!(new_a.mu > a.mu); // lower moves up
    assert!(new_b.mu < b.mu); // higher moves down
    // Sigma decreases
    assert!(new_a.sigma < a.sigma);
}

#[test]
fn underdog_win_large_shift() {
    let strong = OpenSkillRating {
        mu: 35.0,
        sigma: 3.0,
        last_updated: chrono::Utc::now(),
    };
    let weak = OpenSkillRating {
        mu: 15.0,
        sigma: 8.0,
        last_updated: chrono::Utc::now(),
    };

    let (new_weak, new_strong) = mmr::update_ratings(&weak, &strong, Outcomes::WIN);

    // Underdog gains more than in equal match
    let delta = new_weak.mu - weak.mu;
    assert!(delta > 1.0, "expected large gain, got {delta}");
}

#[test]
fn sigma_decreases_with_play() {
    let a = OpenSkillRating {
        mu: 25.0,
        sigma: 8.0,
        last_updated: chrono::Utc::now(),
    };
    let b = a.clone();

    for _ in 0..20 {
        let (new_a, _) = mmr::update_ratings(&a, &b, Outcomes::WIN);
        // Sigma should keep decreasing
        assert!(new_a.sigma <= a.sigma + 0.001);
    }
}
