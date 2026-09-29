# Distributed Job Runner

A job queue built with Go and PostgreSQL. Independent worker processes pick up jobs, renew their leases, and retry failures. A small web console shows the queue, results, and every execution attempt.

## How it works

Workers claim ready jobs with `FOR UPDATE SKIP LOCKED`. A claim and its attempt record commit together. Heartbeats keep the lease alive; if a worker disappears, another worker returns the job to the queue. The attempt number fences off late results from an older execution.

Jobs support priorities, delayed execution, timeouts, cancellation, and capped exponential backoff. Exhausted jobs stay in a dead-letter state with their history. An `Idempotency-Key` prevents duplicate submissions; using the same key with a different request returns a conflict.

The included tasks calculate SHA-256 checksums and numeric statistics. A separate demo task can wait or fail a chosen number of times so recovery is easy to inspect. The [design note](docs/design.md) covers delivery guarantees and tradeoffs.

## Run

With Docker Compose:

```sh
cp .env.example .env
docker compose up --build --wait
```

Open **http://localhost:8080** and connect with `local-runner-token-change-me`. Compose starts PostgreSQL, the API, and two workers with two execution slots each. Try **Retry twice**, then select the job to follow its attempt history. **Long job** gives you time to cancel work while it is running.

Data stays in the PostgreSQL volume after `docker compose down`. The demo binds to localhost; for remote access, use an HTTPS reverse proxy and set a private `API_TOKEN`. The token grants access to the whole queue.

```sh
curl http://localhost:8080/api/jobs \
  -H 'Authorization: Bearer local-runner-token-change-me' \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: example-checksum' \
  -d '{"kind":"checksum","payload":{"text":"hello"}}'
```

`GET /api/jobs` lists jobs with `status`, `queue`, `limit`, and `before` filters. `GET /api/jobs/{id}` includes attempt history; `POST /api/jobs/{id}/cancel` cancels queued or running work. `/api/stats` and `/metrics` expose state counts. Both require the same bearer token.

## Development

Go 1.26+ and PostgreSQL 16 are required. The binary has `migrate`, `api`, and `worker` commands:

```sh
export DATABASE_URL=postgres://user:password@localhost/runner?sslmode=disable
export API_TOKEN=your-local-development-token
go run ./cmd/runner migrate
go run ./cmd/runner api
```

In another terminal with the same `DATABASE_URL`, run `go run ./cmd/runner worker`. `WORKER_ID`, `QUEUES`, `CONCURRENCY`, `LEASE_SECONDS`, and `POLL_MS` configure workers. Defaults are a generated ID, `default,reports`, 2 slots, a 10-second lease, and a 500ms polling interval.

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

Process tests kill a demo worker and restart the API. [CI](https://github.com/Parkryan0128/distributed-job-runner/actions/workflows/ci.yml) runs the full suite against PostgreSQL, builds the Docker image, and checks the console in Chromium. Logs, traces, and screenshots are attached to each run. Node.js is only needed for these development checks; the app itself runs as a Go binary.

## Project structure

- `cmd/runner/` — API, worker, and migration entry points
- `internal/queue/` — persistence, claims, leases, and state transitions
- `internal/worker/` — bounded execution, heartbeats, and recovery
- `internal/task/` — task validation and handlers
- `internal/httpapi/` — authenticated HTTP endpoints
- `web/` — console served by the API
- `tests/` — browser and process acceptance tests

## Contact

Ryan Park — [Email](mailto:parkryan0128@gmail.com) · [LinkedIn](https://www.linkedin.com/in/parkryan0128)
