//! `RatingStore` impl for `PostgresStore`: ratings rows and leaderboard reads.
use super::*;

impl PostgresStore {
    /// Leaderboard rows (user_id, display_name, mu, sigma) ordered by rating
    /// (mu - 3*sigma) descending — same ordering as `list_ratings` plus the
    /// users join for names.
    pub async fn leaderboard_with_names(
        &self,
        mode: &str,
    ) -> Result<Vec<(uuid::Uuid, String, f64, f64)>> {
        let rows = sqlx::query_as::<_, (uuid::Uuid, String, f64, f64)>(
            "SELECT r.user_id, u.display_name, r.mu, r.sigma \
             FROM ratings r JOIN users u ON u.id = r.user_id \
             WHERE r.game_mode = $1 \
             ORDER BY (r.mu - 3 * r.sigma) DESC",
        )
        .bind(mode)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(rows)
    }

    /// Every rating row for a player across all game modes.
    pub async fn all_ratings_for_user(
        &self,
        user_id: uuid::Uuid,
    ) -> Result<Vec<(String, f64, f64, DateTime<Utc>)>> {
        let rows = sqlx::query_as::<_, (String, f64, f64, DateTime<Utc>)>(
            "SELECT game_mode, mu, sigma, last_updated FROM ratings \
             WHERE user_id = $1 ORDER BY game_mode",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(rows)
    }
}

#[async_trait]
impl RatingStore for PostgresStore {
    async fn get_rating(&self, user_id: uuid::Uuid, mode: &str) -> Result<OpenSkillRating> {
        <Self as PlayerStore>::get_rating(self, user_id, mode).await
    }

    async fn list_ratings(&self, mode: &str) -> Result<Vec<(uuid::Uuid, OpenSkillRating)>> {
        let rows = sqlx::query_as::<_, (uuid::Uuid, f64, f64, DateTime<Utc>)>(
            "SELECT user_id, mu, sigma, last_updated FROM ratings \
             WHERE game_mode = $1 \
             ORDER BY (mu - 3 * sigma) DESC",
        )
        .bind(mode)
        .fetch_all(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(rows
            .into_iter()
            .map(|(id, mu, sigma, last_updated)| {
                (
                    id,
                    OpenSkillRating {
                        mu,
                        sigma,
                        last_updated,
                    },
                )
            })
            .collect())
    }

    async fn update_rating(
        &self,
        user_id: uuid::Uuid,
        mode: &str,
        rating: &OpenSkillRating,
    ) -> Result<()> {
        sqlx::query(
            "INSERT INTO ratings (user_id, game_mode, mu, sigma, last_updated) \
             VALUES ($1, $2, $3, $4, NOW()) \
             ON CONFLICT (user_id, game_mode) DO UPDATE SET \
               mu = EXCLUDED.mu, sigma = EXCLUDED.sigma, last_updated = EXCLUDED.last_updated",
        )
        .bind(user_id)
        .bind(mode)
        .bind(rating.mu)
        .bind(rating.sigma)
        .execute(&self.pool)
        .await
        .map_err(map_db_error)?;
        Ok(())
    }
}
