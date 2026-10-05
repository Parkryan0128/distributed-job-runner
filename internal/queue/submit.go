package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
)

func (s *Store) Submit(ctx context.Context, input Submit, key string) (Job, bool, error) {
	return s.submit(ctx, input, key, s.pool)
}

// SubmitLimited serializes admission while preserving idempotent replays at capacity.
func (s *Store) SubmitLimited(ctx context.Context, input Submit, key string, limit int) (Job, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, false, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(7241075)`); err != nil {
		return Job{}, false, err
	}
	var count int
	var replay bool
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE status IN ('queued','running')`).Scan(&count); err != nil {
		return Job{}, false, err
	}
	if key != "" {
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM jobs WHERE idempotency_key=$1)`, key).Scan(&replay); err != nil {
			return Job{}, false, err
		}
	}
	if count >= limit && !replay {
		return Job{}, false, ErrCapacity
	}
	j, created, err := s.submit(ctx, input, key, tx)
	if err != nil {
		return Job{}, false, err
	}
	return j, created, tx.Commit(ctx)
}

func (s *Store) submit(ctx context.Context, input Submit, key string, q queryRower) (Job, bool, error) {
	if err := input.Normalize(); err != nil {
		return Job{}, false, err
	}
	if len(key) > 128 {
		return Job{}, false, errors.New("idempotency key exceeds 128 characters")
	}
	data, err := json.Marshal(input)
	if err != nil {
		return Job{}, false, err
	}
	digest := sha256.Sum256(data)
	hash := hex.EncodeToString(digest[:])
	var storedKey *string
	if key != "" {
		storedKey = &key
	}
	j, err := scanJob(q.QueryRow(ctx, `INSERT INTO jobs
	 (id,kind,queue,payload,request_hash,idempotency_key,priority,max_attempts,timeout_seconds,available_at)
	 VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,clock_timestamp()+make_interval(secs=>$10))
	 ON CONFLICT (idempotency_key) DO NOTHING RETURNING `+columns,
		NewID(), input.Kind, input.Queue, input.Payload, hash, storedKey, input.Priority,
		input.MaxAttempts, input.TimeoutSeconds, input.DelaySeconds))
	if err == nil {
		return j, true, nil
	}
	if !errors.Is(err, ErrNotFound) {
		return Job{}, false, err
	}
	var id, previous string
	if err = q.QueryRow(ctx, `SELECT id::text,request_hash FROM jobs WHERE idempotency_key=$1`, key).Scan(&id, &previous); err != nil {
		return Job{}, false, err
	}
	if previous != hash {
		return Job{}, false, ErrIdempotency
	}
	j, err = scanJob(q.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1`, id))
	return j, false, err
}
