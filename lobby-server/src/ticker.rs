use std::sync::Arc;
use std::time::Duration;

use crate::state::AppState;

pub async fn tick_loop(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(2));
    loop {
        interval.tick().await;
        for mode in &["ranked_1v1"] {
            let _ = state
                .matchmaking_queue
                .tick(mode, &state.store, &state.store, &state.store, &state.store)
                .await;
        }
        let _ = state.matchmaking_queue.cleanup_stale(&state.store).await;
        let _ = state.match_manager.expire_pending_accepts(&state.store).await;
        let _ = state
            .match_manager
            .expire_pending_reports(&state.store)
            .await;
    }
}
