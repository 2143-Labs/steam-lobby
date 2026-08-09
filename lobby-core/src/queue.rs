//! Matchmaking helpers: an expanding MMR `search_band` (50 at t=0, +25 every
//! 10s, capped at 400) and `cleanup_stale`, which drops heartbeat-dead queue
//! entries. Pairing itself lives in `PostgresStore::pair_next_match`
//! (lobby-server/db/queue.rs) — one `FOR UPDATE` transaction per tick, no
//! in-memory queue object.
use crate::error::Result;
use crate::traits::QueueStore;
use crate::types::SteamId;

/// Expanding search band: 50 at t=0, +25 every 10s of wait, capped at 400.
/// Returns `(lo, hi)` in MMR terms, both shifted by the difficulty offset.
pub fn search_band(wait_secs: f64, mu: f64, offset: f64) -> (f64, f64) {
    let band = (50.0 + (wait_secs / 10.0).floor() * 25.0).min(400.0);
    (mu - band + offset, mu + band + offset)
}

/// Remove queue entries for players with no heartbeat in the last 30 seconds.
/// Returns the steam_ids that were removed. Runs in the server's maintenance
/// ticker (out of Temporal — the cleanup is not durable workflow state).
pub async fn cleanup_stale(queue_store: &dyn QueueStore) -> Result<Vec<SteamId>> {
    queue_store
        .remove_stale_queue_entries(chrono::Duration::seconds(30))
        .await
}

#[cfg(test)]
mod tests {
    use super::search_band;

    #[test]
    fn band_starts_50_wide() {
        assert_eq!(search_band(0.0, 1000.0, 0.0), (950.0, 1050.0));
    }

    #[test]
    fn band_widens_in_ten_second_steps() {
        // Sub-step waits keep the base 50-wide band.
        assert_eq!(search_band(9.9, 1000.0, 0.0), (950.0, 1050.0));
        // Each full 10s of waiting widens the band by 25 per side.
        assert_eq!(search_band(10.0, 1000.0, 0.0), (925.0, 1075.0));
        assert_eq!(search_band(30.0, 1000.0, 0.0), (875.0, 1125.0));
    }

    #[test]
    fn band_caps_at_400() {
        assert_eq!(search_band(200.0, 1000.0, 0.0), (600.0, 1400.0));
        // Far longer waits never widen past the cap.
        assert_eq!(search_band(10_000.0, 1000.0, 0.0), (600.0, 1400.0));
    }

    #[test]
    fn difficulty_offset_shifts_the_whole_band() {
        // Easy (-150) targets weaker opponents; Hard (+150) targets stronger.
        assert_eq!(search_band(0.0, 1000.0, -150.0), (800.0, 900.0));
        assert_eq!(search_band(0.0, 1000.0, 150.0), (1100.0, 1200.0));
    }

    #[test]
    fn low_mmr_easy_offset_can_go_negative() {
        // The lower bound may dip below zero for a new player on Easy; that is
        // fine because pairing compares `opponent.mu >= lo` (everyone passes).
        assert_eq!(search_band(0.0, 25.0, -150.0), (-175.0, -75.0));
    }
}
