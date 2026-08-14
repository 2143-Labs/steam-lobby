//! `RatingStore` impl for `PostgresStore`: ratings rows and leaderboard reads.
use super::*;

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
