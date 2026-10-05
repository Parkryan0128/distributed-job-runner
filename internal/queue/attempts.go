package queue

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

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
