# Design

PostgreSQL owns both the queue and the attempt history. Keeping them in one transaction avoids a separate database/broker write boundary. Polling and `SKIP LOCKED` let each worker claim independently, without a leader. This is a modest-throughput queue, not an attempt to replace a dedicated workflow engine.

## Delivery and ownership

Execution is at least once. A worker might finish an external side effect and lose its lease before saving the result. Submission idempotency does not make handler side effects exactly once. A real payment, email, or export handler would need its own idempotency key or transactional write, usually based on the stable job ID.

A claim increments the attempt number and sets the worker ID and lease deadline using the database clock. Heartbeats and completion must match the current attempt and worker, while the lease is still valid after acquiring the row lock. Recovery marks an expired attempt and either schedules another one or moves the job to `dead` when its attempt budget is spent. Backoff starts at one second and caps at 64 seconds. Priority orders ready jobs; it does not preempt running work or guarantee fairness for low-priority jobs.

Canceling a running job immediately fences its result. The handler receives cancellation when its next heartbeat is rejected. Handlers must honor the Go context; cancellation cannot undo side effects. Shutdown stops claiming, cancels active handlers, and records those attempts as failures for retry. These attempts count toward the same budget. A hard kill relies on lease expiry instead.

## API and console

The demo opens directly without accounts or API keys. Docker publishes it on localhost, and the standalone API also listens on loopback by default. Payloads and response data are rendered as text. Only the three built-in task kinds are accepted.

The console polls every 1.5 seconds. Cursor pagination uses the insertion sequence so a newly submitted job does not shift an older page. The detail endpoint reads the job and attempt history from one consistent database snapshot. A client generation check discards responses from a previous selection or filter.

## Scope

There is no cron scheduler, workflow DAG, global rate limiter, or automatic dead-letter replay. A failed job stays available for inspection; submit a new job to run it again. History and idempotency keys are retained with the job, with no automatic cleanup policy. The dashboard counts show current persisted state. There are concurrency and failure tests, but no throughput claim or production load benchmark.

The schema setup is an idempotent initial migration guarded by a transaction advisory lock. Future schema changes need versioned migrations rather than edits to `CREATE TABLE IF NOT EXISTS`.
