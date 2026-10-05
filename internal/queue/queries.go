package queue

import (
	"context"

	"github.com/jackc/pgx/v5"
)

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
