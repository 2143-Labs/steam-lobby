-- no-transaction
-- match_reports is populated in production, so the restored uniqueness is
-- created concurrently (a plain CREATE UNIQUE INDEX would hold an ACCESS
-- EXCLUSIVE lock on the table for the whole build).
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS match_reports_token_reporter_uidx
    ON match_reports (match_token, reporting_player);
