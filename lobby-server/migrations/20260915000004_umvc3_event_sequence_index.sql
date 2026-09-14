-- no-transaction
CREATE UNIQUE INDEX CONCURRENTLY match_events_recipient_sequence_uidx
    ON match_events (recipient_user_id, recipient_sequence)
    WHERE recipient_user_id IS NOT NULL;
