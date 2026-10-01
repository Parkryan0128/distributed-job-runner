# Distributed Job Runner

A job queue built with Go and PostgreSQL. Independent worker processes pick up jobs, renew their leases, and retry failures. A small web console shows the queue, results, and every execution attempt.

## How it works

Workers claim ready jobs with `FOR UPDATE SKIP LOCKED`. A claim and its attempt record commit together. Heartbeats keep the lease alive; if a worker disappears, another worker returns the job to the queue. The attempt number fences off late results from an older execution.

Jobs support priorities, delayed execution, timeouts, cancellation, and capped exponential backoff. Exhausted jobs stay in a dead-letter state with their history. An `Idempotency-Key` prevents duplicate submissions; using the same key with a different request returns a conflict.

The demo task waits for a chosen duration and can fail its first attempts so recovery is easy to inspect. The [design note](docs/design.md) covers delivery guarantees and tradeoffs.

## Run

With Docker Compose:

```sh
docker compose up --build --wait
```

Open **http://localhost:8080**. The console starts immediately; no login or API key is needed. Compose starts PostgreSQL, the API, and two workers with two execution slots each. Try **Retry twice**, then select the job to follow its attempt history. **Long job** gives you time to cancel work while it is running.

This is a local demo. Data stays in the PostgreSQL volume after `docker compose down`. Compose publishes the console on localhost only.

The first visitor to press **Start** or **New job** receives control for 30 seconds. Other visitors watch the same queue but cannot change workloads, create jobs, or cancel them. **Stop** (or **Release** for manual jobs) ends the turn early. Changing the workload does not extend the turn. Closing a browser does not stop the server generator; the deadline does.

```sh
curl -c demo.cookies http://localhost:8080/api/demo \
  -H 'Content-Type: application/json' -d '{"action":"claim"}'
curl -b demo.cookies http://localhost:8080/api/jobs \
  -H 'Content-Type: application/json' -H 'Idempotency-Key: example-demo' \
  -d '{"kind":"demo","payload":{"work_ms":2000,"fail_until":1}}'
curl -b demo.cookies http://localhost:8080/api/demo \
  -H 'Content-Type: application/json' -d '{"action":"stop"}'
```

`GET /api/jobs` lists jobs with `status`, `limit`, and `before` filters. `GET /api/jobs/{id}` includes attempt history; `POST /api/jobs/{id}/cancel` requires the controller's session cookie. `GET /api/events` streams job transitions and worker/control state. `POST /api/demo` accepts `claim`, `start`, `level`, and `stop`, with an optional `level` of `low`, `medium`, or `high`.

## Development

Go 1.26+ and PostgreSQL 16 are required. The binary has `migrate`, `api`, and `worker` commands:

```sh
export DATABASE_URL=postgres://user:password@localhost/runner?sslmode=disable
go run ./cmd/runner migrate
go run ./cmd/runner api
```

In another terminal with the same `DATABASE_URL`, run `go run ./cmd/runner worker`. `WORKER_ID`, `QUEUES`, `CONCURRENCY`, `LEASE_SECONDS`, and `POLL_MS` configure workers. Defaults are a generated ID, `default`, 2 slots, a 10-second lease, and a 500ms polling interval.

Worker settings do not affect the API or migration command.

## Tests

Point the tests at a dedicated PostgreSQL database. Each test creates and removes its own schema:

```sh
TEST_DATABASE_URL=postgres://user:password@localhost/runner_test?sslmode=disable \
  go test -race -count=1 ./...
go vet ./...
```

Without `TEST_DATABASE_URL`, database tests are skipped. Browser and process tests target the running Compose app:

```sh
npm ci
npx playwright install chromium
npm run test:acceptance
npm run test:browser
```

Process tests kill a demo worker, pause PostgreSQL, and restart the API. [CI](https://github.com/Parkryan0128/distributed-job-runner/actions/workflows/ci.yml) runs the full suite against PostgreSQL, builds the Docker image, and checks the console in Chromium. Logs, traces, and screenshots are attached to each run. Node.js is only needed for these development checks; the app itself runs as a Go binary.

## Project structure

- `cmd/runner/` — API, worker, and migration entry points
- `internal/queue/` — persistence, claims, leases, and state transitions
- `internal/worker/` — bounded execution, heartbeats, and recovery
- `internal/task/` — task validation and handlers
- `internal/httpapi/` — HTTP endpoints
- `web/` — console served by the API
- `tests/` — browser and process acceptance tests

## Contact

Ryan Park — [Email](mailto:parkryan0128@gmail.com) · [LinkedIn](https://www.linkedin.com/in/parkryan0128)

### Shared workload demo

Low (0.3 jobs/s), Medium (1.2 jobs/s), and High (3 jobs/s) run in one server generator. Tasks average two seconds; respectively 5%, 10%, or 20% fail their first attempt and recover on retry. The generator pauses arrivals when 40 jobs are queued. Stop and the 30-second deadline end arrivals; already submitted jobs finish normally.

Scenario buttons submit immediately; **Custom task** keeps editable settings for manual submissions.

The activity panel shows one compact ID block per job. The shared queue stays on one row with an overflow count; successful jobs briefly turn green before disappearing. SSE delivers committed creation, claim, retry, cancellation, and completion transitions, including tasks that finish between updates. The UI briefly sequences transitions for visibility without delaying real execution. Reconnecting replaces the display with a consistent current snapshot. The table and selected details refresh in response to events, not on a fixed polling timer.

Run **one API process** for this public demo: it owns the in-memory control lease, one generator, and the event fan-out. An API restart releases control and stops generation while persisted jobs continue on workers. Multiple API replicas would require shared control/generator coordination; this version deliberately does not claim that capability. The session cookie arbitrates turns; it is not account authentication or a defense against a determined visitor repeatedly taking turns.

For public hosting, put the API behind HTTPS and configure the proxy to stream `/api/events` without response buffering or compression buffering. Allow long-lived responses; the server emits periodic messages and disconnects slow readers. It bounds each subscriber's queue at 64 events and allows 200 SSE connections per API instance. Test the actual hosting proxy before publishing. No public deployment is performed by `docker compose up`.

For the shared portfolio VM and automatic releases, see [deployment](docs/deployment.md).
