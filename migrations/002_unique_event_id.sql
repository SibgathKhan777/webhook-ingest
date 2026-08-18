-- Add a UNIQUE constraint on events.event_id so that a concurrent pair of
-- inserters cannot both succeed for the same delivery. The plain index in
-- 001_init.sql only speeds up lookups — it does not prevent duplicates.
--
-- Using CREATE UNIQUE INDEX rather than ALTER TABLE ADD CONSTRAINT so that
-- Postgres builds it without a full table lock (CONCURRENTLY would be ideal
-- in production; omitted here because migrations run on an empty DB at start).
CREATE UNIQUE INDEX IF NOT EXISTS idx_events_event_id_unique ON events (event_id);
