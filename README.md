# Distributed Job Runner

A Go job queue backed by PostgreSQL. Separate worker processes claim jobs, renew leases, and retry failures. The dashboard shows job state and attempt history.

The implementation is split into the queue and concurrency tests, worker recovery, HTTP API, and a Docker Compose demo with multiple workers. Each stage is tested before the next is added.
