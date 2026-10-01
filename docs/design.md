# Design

PostgreSQL owns both the queue and the attempt history. Keeping them in one transaction avoids a separate database/broker write boundary. Polling and `SKIP LOCKED` let each worker claim independently, without a leader. This is a modest-throughput queue, not an attempt to replace a dedicated workflow engine.

## Delivery and ownership

Execution is at least once. A worker might finish an external side effect and lose its lease before saving the result. Submission idempotency does not make handler side effects exactly once. A real payment, email, or export handler would need its own idempotency key or transactional write, usually based on the stable job ID.

A claim increments the attempt number and sets the worker ID and lease deadline using the database clock. Heartbeats and completion must match the current attempt and worker, while the lease is still valid after acquiring the row lock. Recovery marks an expired attempt and either schedules another one or moves the job to `dead` when its attempt budget is spent. Backoff starts at one second and caps at 64 seconds. Priority orders ready jobs; it does not preempt running work or guarantee fairness for low-priority jobs.

Canceling a running job immediately fences its result. The handler receives cancellation when its next heartbeat is rejected. Handlers must honor the Go context; cancellation cannot undo side effects. Shutdown stops claiming, cancels active handlers, and records those attempts as failures for retry. These attempts count toward the same budget. A hard kill relies on lease expiry instead.

## API and console

The demo opens directly without accounts or API keys. Docker publishes it on localhost, and the standalone API also listens on loopback by default. Payloads and response data are rendered as text. Only the demo task is accepted; API submissions use the default shared queue.

Job-state transitions append to `job_events` in the same transaction as the state change. A transaction advisory lock serializes event ID allocation through commit, preventing a committed later ID from overtaking an earlier uncommitted event. A single API reader drains this log every 100 ms and fans events out over SSE; viewers do not independently poll the database for activity. This retains transitions of zero-duration jobs instead of sampling current state. Events are pruned after ten minutes; jobs and attempt history remain.

An SSE connection subscribes before taking a repeatable-read snapshot containing counts, active jobs, worker capacity, and a cursor. Buffered events at or below that cursor are discarded. A lost/slow connection reconnects with a fresh snapshot rather than promising replay of every animation during the disconnection. Subscriber queues and write deadlines are bounded. The SSE handler bypasses the normal five-second request deadline, and updates its own write deadline for each message.

One API process owns a mutex-protected 30-second demo lease and generator. Ownership is an opaque HttpOnly, SameSite cookie; the owner ID is never broadcast. All job mutations and workload changes check ownership on the server. A lease cannot be extended by changing workload; stopping or expiry releases it. Controller checks and mutations are serialized so concurrent claims cannot both succeed. Cross-origin mutation requests are rejected. Multiple API replicas are outside this demo's scope.

Cursor pagination uses the insertion sequence so new jobs do not shift an older page. The detail endpoint reads the job and attempts from a consistent snapshot. A client generation check discards responses from previous selections. Each job's visual transitions are briefly sequenced, independently of real processing; reconnect discards pending visual transitions and reconciles actual state.

## Scope

There is no cron scheduler, workflow DAG, global rate limiter, or automatic dead-letter replay. A failed job stays available for inspection; submit a new job to run it again. History and idempotency keys are retained with the job, with no automatic cleanup policy. The dashboard counts show current persisted state. There are concurrency and failure tests, but no throughput claim or production load benchmark.

The schema setup is an idempotent initial migration guarded by a transaction advisory lock. Future schema changes need versioned migrations rather than edits to `CREATE TABLE IF NOT EXISTS`.
