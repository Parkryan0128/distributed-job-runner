# Distributed Job Runner

A job queue built with Go and PostgreSQL.

Independent workers claim jobs from a shared queue, retry failures, and recover work when a worker stops. A web console shows queued jobs, active workers, and execution history through Server-Sent Events (SSE).

[Live demo](https://distributed-job-runner.ryanparkdev.com/)

## How it works

```text
Web console / API
    │ submit a job
    ▼
PostgreSQL shared queue
    │ claim a ready job
    ▼
Worker A / Worker B (2 execution slots each)
    ├── Success → save result
    ├── Failure → retry with backoff
    └── Attempts exhausted → dead

Committed job transitions → API → SSE → Web console
```

1. The API saves a job in PostgreSQL.
2. A free worker claims it with `FOR UPDATE SKIP LOCKED` and records the attempt in the same transaction.
3. The worker renews its lease while the task runs.
4. Failures return to the queue with exponential backoff. If a worker disappears, another worker recovers the job after its lease expires.
5. The API reads committed state changes and streams them to connected browsers.

Jobs support priority, delayed execution, timeouts, cancellation, and submission idempotency. Attempt numbers prevent an old worker from overwriting a newer attempt's result.

Execution is **at least once**: a recovered job may run again. Handlers with external side effects need their own idempotency. See the [design notes](docs/design.md) for the guarantees and tradeoffs.

## Try the demo

The demo runs two workers with two execution slots each: **four jobs can run at once**.

- Choose **Low**, **Medium**, or **High** and press **Start** to generate jobs.
- Use **New job** to submit a test scenario immediately or configure a custom task.
- Select a job in the table to inspect its attempts, errors, and result.

Everyone observes the same queue. One visitor controls it for 30 seconds; others can watch. Stopping or reaching the deadline ends new arrivals, while existing jobs continue processing. No account or API key is required.

The single **Demo Task** waits for a configured duration and can fail its first attempts. High workload demonstrates queue buildup and retries; it is not a CPU stress test.

## Run locally

Requirements: Docker with Docker Compose.

```bash
git clone https://github.com/Parkryan0128/distributed-job-runner.git
cd distributed-job-runner
docker compose up --build --wait
```

Open **http://localhost:8080**. Compose starts PostgreSQL, applies the schema, and starts the API and both workers.

To stop:

```bash
docker compose down
```

Job history remains in the database volume. The local console binds to localhost only.

## Project structure

```text
cmd/runner/         API, worker, and migration commands
internal/queue/     Jobs, attempts, leases, and event storage
internal/worker/    Concurrent execution and recovery
internal/task/      Demo task validation and execution
internal/httpapi/   HTTP API, demo control, and SSE
web/                HTML, CSS, and JavaScript console
tests/              Browser and process recovery tests
deploy/             Production Compose and deployment scripts
```

## Tests

Go 1.26+ and PostgreSQL 16 are required for the Go tests. Use a dedicated test database; each database test creates and removes its own schema.

```bash
TEST_DATABASE_URL=postgres://user:password@localhost/runner_test?sslmode=disable \
  go test -race -count=1 ./...
go vet ./...
```

Database tests are skipped when `TEST_DATABASE_URL` is unset.

With the local Compose stack running, use Node.js 24+ to run the process and browser tests:

```bash
npm ci
npx playwright install --with-deps chromium
npm run test:acceptance
npm run test:browser
npm run format:check
```

The process tests kill a worker, restart the API, and temporarily pause PostgreSQL to verify recovery. Run them against the local stack. Node.js is only used for development checks; the application runs as a Go binary.

[CI](https://github.com/Parkryan0128/distributed-job-runner/actions/workflows/ci.yml) runs these checks and tests the production configuration through HTTPS, including secure cookies, shared control, retries, and SSE.

## Contact

- **Name:** Ryan Park
- **Email:** [parkryan0128@gmail.com](mailto:parkryan0128@gmail.com)
- **LinkedIn:** [linkedin.com/in/parkryan0128](https://www.linkedin.com/in/parkryan0128)
- **GitHub:** [github.com/Parkryan0128](https://github.com/Parkryan0128)
