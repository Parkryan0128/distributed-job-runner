package queue

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"

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

func (s *Store) get(ctx context.Context, id string) (Job, error) {
	return scanJob(s.pool.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1`, id))
}

func (s *Store) Get(ctx context.Context, id string) (Detail, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Detail{}, err
	}
	defer tx.Rollback(ctx)
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1`, id))
	if err != nil {
		return Detail{}, err
	}
	rows, err := tx.Query(ctx, `SELECT number,worker_id,status,started_at,finished_at,error FROM attempts WHERE job_id=$1 ORDER BY number`, id)
	if err != nil {
		return Detail{}, err
	}
	attempts := make([]Attempt, 0)
	for rows.Next() {
		var a Attempt
		if err := rows.Scan(&a.Number, &a.WorkerID, &a.Status, &a.StartedAt, &a.FinishedAt, &a.Error); err != nil {
			rows.Close()
			return Detail{}, err
		}
		attempts = append(attempts, a)
	}
	if err := rows.Err(); err != nil {
		return Detail{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Detail{}, err
	}
	return Detail{Job: j, Attempts: attempts}, nil
}

func (s *Store) List(ctx context.Context, f Filter) (Page, error) {
	if err := f.Validate(); err != nil {
		return Page{}, err
	}
	rows, err := s.pool.Query(ctx, `SELECT `+columns+`,sequence FROM jobs
	 WHERE ($1='' OR status=$1) AND ($2='' OR queue=$2) AND ($3::bigint=0 OR sequence<$3)
	 ORDER BY sequence DESC LIMIT $4`, f.Status, f.Queue, f.Before, f.Limit+1)
	if err != nil {
		return Page{}, err
	}
	defer rows.Close()
	page := Page{Jobs: make([]Job, 0)}
	var last int64
	for rows.Next() {
		var seq int64
		j, err := scanJob(rows, &seq)
		if err != nil {
			return Page{}, err
		}
		if len(page.Jobs) == f.Limit {
			page.NextCursor = last
			break
		}
		page.Jobs = append(page.Jobs, j)
		last = seq
	}
	return page, rows.Err()
}

func (s *Store) Stats(ctx context.Context) (Stats, error) {
	var st Stats
	err := s.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='queued'),count(*) FILTER(WHERE status='running'),
	 count(*) FILTER(WHERE status='succeeded'),count(*) FILTER(WHERE status='dead'),count(*) FILTER(WHERE status='canceled') FROM jobs`).Scan(&st.Queued, &st.Running, &st.Succeeded, &st.Dead, &st.Canceled)
	return st, err
}

func (s *Store) Claim(ctx context.Context, worker string, queues []string, lease time.Duration) (Job, error) {
	if worker == "" || len(queues) == 0 || lease < time.Millisecond {
		return Job{}, errors.New("worker, queues and a positive lease are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback(ctx)
	j, err := scanJob(tx.QueryRow(ctx, `UPDATE jobs SET status='running',attempt=attempt+1,worker_id=$1,
	 lease_until=clock_timestamp()+make_interval(secs=>$3),updated_at=clock_timestamp(),error=''
	 WHERE id=(SELECT id FROM jobs WHERE status='queued' AND queue=ANY($2) AND available_at<=clock_timestamp()
	 ORDER BY priority DESC,available_at,sequence FOR UPDATE SKIP LOCKED LIMIT 1) RETURNING `+columns,
		worker, queues, lease.Seconds()))
	if err != nil {
		return Job{}, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO attempts(job_id,number,worker_id,status) VALUES($1,$2,$3,'running')`, j.ID, j.Attempt, worker)
	if err != nil {
		return Job{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Job{}, err
	}
	return j, nil
}

func lockAttempt(ctx context.Context, tx pgx.Tx, j Job) (int, error) {
	var maxAttempts int
	err := tx.QueryRow(ctx, `SELECT max_attempts FROM jobs WHERE id=$1 AND status='running'
	 AND attempt=$2 AND worker_id=$3 FOR UPDATE`, j.ID, j.Attempt, j.WorkerID).Scan(&maxAttempts)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, ErrLeaseLost
	}
	return maxAttempts, err
}

func (s *Store) Heartbeat(ctx context.Context, j Job, lease time.Duration) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := lockAttempt(ctx, tx, j); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET lease_until=clock_timestamp()+make_interval(secs=>$2)
	 WHERE id=$1 AND lease_until>clock_timestamp()`, j.ID, lease.Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return tx.Commit(ctx)
}

func (s *Store) Finish(ctx context.Context, j Job, result json.RawMessage, failure string, permanent bool) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	maxAttempts, err := lockAttempt(ctx, tx, j)
	if err != nil {
		return err
	}
	status, attemptStatus := "succeeded", "succeeded"
	if failure != "" {
		status, attemptStatus = "queued", "failed"
		result = nil
		if permanent || j.Attempt >= maxAttempts {
			status = "dead"
		}
	}
	tag, err := tx.Exec(ctx, `UPDATE jobs SET status=$2,result=$3,error=$4,worker_id=NULL,lease_until=NULL,
	 available_at=CASE WHEN $2='queued' THEN clock_timestamp()+make_interval(secs=>$5) ELSE available_at END,
	 updated_at=clock_timestamp() WHERE id=$1 AND lease_until>clock_timestamp()`, j.ID, status, result, failure, RetryDelay(j.Attempt).Seconds())
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	_, err = tx.Exec(ctx, `UPDATE attempts SET status=$3,error=$4,finished_at=clock_timestamp() WHERE job_id=$1 AND number=$2`, j.ID, j.Attempt, attemptStatus, failure)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Recover(ctx context.Context) (int, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	rows, err := tx.Query(ctx, `SELECT id::text,attempt,max_attempts FROM jobs WHERE status='running' AND lease_until<=clock_timestamp()
	 ORDER BY lease_until FOR UPDATE SKIP LOCKED LIMIT 100`)
	if err != nil {
		return 0, err
	}
	type expired struct {
		id           string
		attempt, max int
	}
	jobs := make([]expired, 0)
	for rows.Next() {
		var j expired
		if err := rows.Scan(&j.id, &j.attempt, &j.max); err != nil {
			rows.Close()
			return 0, err
		}
		jobs = append(jobs, j)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	for _, j := range jobs {
		status := "queued"
		if j.attempt >= j.max {
			status = "dead"
		}
		_, err = tx.Exec(ctx, `UPDATE jobs SET status=$2,worker_id=NULL,lease_until=NULL,error='worker lease expired',
		 available_at=CASE WHEN $2='queued' THEN clock_timestamp()+make_interval(secs=>$3) ELSE available_at END,
		 updated_at=clock_timestamp() WHERE id=$1`, j.id, status, RetryDelay(j.attempt).Seconds())
		if err != nil {
			return 0, err
		}
		_, err = tx.Exec(ctx, `UPDATE attempts SET status='expired',error='worker lease expired',finished_at=clock_timestamp() WHERE job_id=$1 AND number=$2`, j.id, j.attempt)
		if err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, err
	}
	return len(jobs), nil
}

func (s *Store) Cancel(ctx context.Context, id string) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	j, err := scanJob(tx.QueryRow(ctx, `SELECT `+columns+` FROM jobs WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return err
	}
	if j.Status == "canceled" {
		return tx.Commit(ctx)
	}
	if j.Status != "queued" && j.Status != "running" {
		return ErrConflict
	}
	_, err = tx.Exec(ctx, `UPDATE jobs SET status='canceled',worker_id=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1`, id)
	if err != nil {
		return err
	}
	if j.Status == "running" {
		_, err = tx.Exec(ctx, `UPDATE attempts SET status='canceled',finished_at=clock_timestamp() WHERE job_id=$1 AND number=$2`, id, j.Attempt)
		if err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
