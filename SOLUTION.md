# SOLUTION.md

## What was broken and why

**1. `event_id` idempotency race (TOCTOU)**
`Ingest` called `EventExists` (a SELECT) and then `InsertEvent` (a plain INSERT)
as two separate database round-trips. Two concurrent deliveries of the same
`event_id` could both read "not exists" before either wrote, then both insert
successfully — producing duplicate `events` rows and double-counting
`account_stats`. The `events.event_id` column had only a plain index, not a
UNIQUE constraint, so the database had no guard of its own.

**2. Silent recording-processing failure**
The goroutine launched inside `Ingest` captured the HTTP request's `ctx` from
the caller. The handler wrote `200 OK` and returned; net/http then cancelled
that context. The goroutine's `time.Sleep` finished, but `MarkRecordingProcessed`
received a cancelled context and returned an error immediately. The error was
swallowed by a bare `// TODO: handle` comment, so `recording_processed` was
never set to `true` and nothing appeared in the logs.

**3. Graceful shutdown / goroutine loss**
`main.go` called `srv.Shutdown()` and then returned. `Shutdown` drains HTTP
connections but knows nothing about goroutines launched inside handlers. Any
recording goroutine still sleeping (or mid-DB-write) when the process exited
was killed, leaving calls in the database with `recording_processed = false`.

**4. Stats cache data race (bonus)**
`Cache.Record` read and wrote the internal map without holding `c.mu`. Concurrent
calls — which happen on any burst of traffic for the same account — triggered
`concurrent map writes` and could corrupt the in-memory totals.

---

## Why this deduplication strategy over the alternatives

The fix uses `INSERT ... ON CONFLICT (event_id) DO NOTHING` backed by a UNIQUE
index. The key property is **atomicity**: the existence check and the write are
a single statement under Postgres's serialization, so no two concurrent inserts
can both succeed for the same `event_id`. The UNIQUE index enforces the
invariant at the database level regardless of how many application instances are
running.

Alternatives considered:

- **Redis `SET NX` pre-check** — shifts the race from Postgres to Redis and
  introduces a second external dependency on the critical path. If Redis is
  temporarily unavailable, the service would have to reject webhooks or fall back
  to the TOCTOU pattern anyway. It also adds clock-drift concerns if a TTL is set.

- **Postgres advisory locks / `SELECT FOR UPDATE`** — works, but requires a
  transaction and two round-trips (SELECT then INSERT) with an explicit lock.
  `ON CONFLICT DO NOTHING` achieves the same result in one statement, no
  transaction needed.

- **Upsert (`ON CONFLICT DO UPDATE SET ...`)** — appropriate when the second
  delivery may carry updated data. Here the provider guarantees the payload is
  identical across redeliveries, so we want a strict no-op on conflict, not an
  update.

---

## What I would change at 10,000 webhooks/second

**Replace the goroutine-per-recording pattern with a worker pool backed by a
durable queue.**

At high throughput, launching one goroutine per event creates unbounded
concurrency against the recording storage (rate limits, connection exhaustion)
and against Postgres (connection pool saturation). A fixed-size worker pool
(e.g. 20–50 workers) caps concurrency. The jobs need to survive process
restarts, so they should be enqueued durably — the `redis` client is already
wired in and Redis Streams or a simple sorted-set job queue works well here.
On startup the workers drain any queue that built up during the previous
deployment.

Other changes at that scale:

- **Batch `IncrementAccountStats`** — one `UPDATE` per webhook hits Postgres
  hard. Buffer updates in Redis (HINCRBY) and flush to Postgres periodically or
  on a threshold. The stats endpoint reads from Redis first.
- **UNIQUE index becomes critical** — `ON CONFLICT` performance is determined by
  the index. A B-tree UNIQUE index on `event_id` handles this fine; at extreme
  throughput consider partitioning by time (date prefix of event_id) to keep
  index pages hot in cache.
- **Horizontal scale** — because idempotency is enforced at the DB level, any
  number of application replicas can run simultaneously without coordination.
