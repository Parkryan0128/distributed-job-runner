# Development and API usage

## Run without Docker

Use Go 1.26+ and PostgreSQL 16. Create a database, then apply the schema and start the API from the repository root:

```bash
export DATABASE_URL=postgres://user:password@localhost/runner?sslmode=disable
go run ./cmd/runner migrate
go run ./cmd/runner api
```

In another terminal with the same `DATABASE_URL`:

```bash
go run ./cmd/runner worker
```

The API serves the console at `http://127.0.0.1:8080`. Use localhost or HTTPS: the console uses the browser's `crypto.randomUUID()` API when submitting jobs. Each worker defaults to two execution slots. Start a second worker process to match the four-slot Compose demo.

| Setting                   | Applies to   | Default                              |
| ------------------------- | ------------ | ------------------------------------ |
| `DATABASE_URL`            | All commands | Required                             |
| `LISTEN_ADDR`             | API          | `127.0.0.1:8080`                     |
| `WEB_DIR`                 | API          | `web`                                |
| `SECURE_COOKIES`          | API          | `false`; production sets `true`      |
| `HISTORY_RETENTION_HOURS` | API          | `0` (disabled); production sets `24` |
| `WORKER_ID`               | Worker       | Generated ID                         |
| `QUEUES`                  | Worker       | `default`                            |
| `CONCURRENCY`             | Worker       | `2`                                  |
| `LEASE_SECONDS`           | Worker       | `10` (local Compose uses `6`)        |
| `POLL_MS`                 | Worker       | `500`                                |

The public API accepts only `demo` tasks on the `default` queue. Worker queue configuration remains available for direct queue-library use.

## Submit through the API

Claim a 30-second turn, keep the session cookie, and submit a task that fails once before succeeding:

```bash
curl -c demo.cookies http://localhost:8080/api/demo \
  -H 'Content-Type: application/json' -d '{"action":"claim"}'
curl -b demo.cookies http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: example-demo' \
  -d '{"kind":"demo","payload":{"work_ms":2000,"fail_until":1}}'
curl -b demo.cookies http://localhost:8080/api/demo \
  -H 'Content-Type: application/json' -d '{"action":"stop"}'
```

Repeating a submission with the same idempotency key and request returns the existing job. A different request with that key returns `409`. Keys expire when their jobs are removed.

| Endpoint                     | Purpose                                                                         |
| ---------------------------- | ------------------------------------------------------------------------------- |
| `GET /api/jobs`              | List jobs; accepts `status`, `limit` (1–100), and `before` cursor               |
| `GET /api/jobs/{id}`         | Job details and attempt history                                                 |
| `POST /api/jobs`             | Submit a job; requires the controlling session                                  |
| `POST /api/jobs/{id}/cancel` | Cancel a queued or running job; requires control                                |
| `GET /api/stats`             | Counts by current job status                                                    |
| `GET /api/pending`           | First 12 queued jobs in claim order                                             |
| `GET /api/workers`           | Worker status, capacity, and running jobs                                       |
| `GET /api/events`            | SSE snapshot, job transitions, worker and control updates                       |
| `GET /api/demo`              | Current control and workload state                                              |
| `POST /api/demo`             | `claim`, `start`, `level`, or `stop`; optional `level`: `low`, `medium`, `high` |
| `GET /healthz`               | HTTP process health                                                             |
| `GET /readyz`                | Database connectivity                                                           |

A visitor without control receives `409` for job changes. Manual submissions allow a global burst of 20, refilling at five per second, with a ceiling of 100 queued/running jobs. Exceeding either limit returns `429` with `Retry-After`.

## Workload and updates

Low, Medium, and High target 0.3, 1.2, and 3 jobs per second. Tasks last 1.5–2.5 seconds; every 20th, 10th, or 5th task respectively fails its first attempt. The generator pauses arrivals while 40 or more jobs are queued. The timer loop and database latency can reduce the actual arrival rate.

SSE streams committed transitions rather than periodically sampling job status. The API reads the event log every 100 ms and broadcasts worker/control updates roughly once a second. It allows up to 200 subscribers with 64 buffered messages each. Slow or disconnected viewers reconnect with a fresh snapshot.

The console briefly sequences task transitions for visibility, including a 400 ms green completion state. These animations do not delay execution. The job table and details refresh in response to events.
