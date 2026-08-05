//! Thin wrapper over `skillratings`' Weng-Lin implementation: converts the
//! core's `OpenSkillRating` to/from the library types, applies an `Outcomes`
//! result, and stamps both new ratings with the current time.
use chrono::Utc;
use skillratings::weng_lin::{weng_lin, WengLinConfig, WengLinRating};
use skillratings::Outcomes;

use crate::types::OpenSkillRating;

/// Compute new ratings for a 1v1 match.
/// Returns (player_a_new, player_b_new).
pub fn update_ratings(
    player_a: &OpenSkillRating,
    player_b: &OpenSkillRating,
    outcome: Outcomes,
) -> (OpenSkillRating, OpenSkillRating) {
    let config = WengLinConfig::default();
    let rating_a = WengLinRating {
        rating: player_a.mu,
        uncertainty: player_a.sigma,
    };
    let rating_b = WengLinRating {
        rating: player_b.mu,
        uncertainty: player_b.sigma,
    };
    let (new_a, new_b) = weng_lin(&rating_a, &rating_b, &outcome, &config);
    (
        OpenSkillRating {
            mu: new_a.rating,
            sigma: new_a.uncertainty,
            last_updated: Utc::now(),
        },
        OpenSkillRating {
            mu: new_b.rating,
            sigma: new_b.uncertainty,
            last_updated: Utc::now(),
        },
    )
}
