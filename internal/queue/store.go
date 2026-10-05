package queue

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct{ pool *pgxpool.Pool }

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

func (s *Store) Migrate(ctx context.Context) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7241073)`); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, schema); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

const columns = `id::text, kind, queue, payload, priority, status, attempt, max_attempts,
 timeout_seconds, worker_id, lease_until, available_at, created_at, updated_at, result, error`

func scanJob(row pgx.Row, extra ...any) (Job, error) {
	var j Job
	fields := []any{&j.ID, &j.Kind, &j.Queue, &j.Payload, &j.Priority, &j.Status, &j.Attempt,
		&j.MaxAttempts, &j.TimeoutSeconds, &j.WorkerID, &j.LeaseUntil, &j.AvailableAt,
		&j.CreatedAt, &j.UpdatedAt, &j.Result, &j.Error}
	err := row.Scan(append(fields, extra...)...)
	if errors.Is(err, pgx.ErrNoRows) {
		return j, ErrNotFound
	}
	return j, err
}

type queryRower interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}
