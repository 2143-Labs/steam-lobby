-- Restore the (match_token, reporting_player) uniqueness that the UUID player
-- column migration silently dropped: removing the BIGINT `reporting_player`
-- column also removed its UNIQUE constraint, which makes every
-- `ON CONFLICT (match_token, reporting_player) DO NOTHING` write fail with
-- "no unique or exclusion constraint matching the ON CONFLICT specification".
-- Duplicate rows can only exist for the window in which that constraint was
-- absent; keep the earliest row so the unique index can be built.
DELETE FROM match_reports older
USING match_reports newer
WHERE older.match_token = newer.match_token
  AND older.reporting_player = newer.reporting_player
  AND older.id > newer.id;
