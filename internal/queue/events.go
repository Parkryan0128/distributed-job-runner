package queue

import (
	"context"
	"encoding/json"
	"github.com/jackc/pgx/v5"
)

type Event struct {
	ID       int64  `json:"id"`
	Job      Job    `json:"job"`
	Previous string `json:"previous"`
}
type Snapshot struct {
	Workers []WorkerState `json:"workers"`
	Cursor  int64         `json:"cursor"`
	Stats   Stats         `json:"stats"`
	Jobs    []Job         `json:"jobs"`
}

func (s *Store) Snapshot(ctx context.Context) (Snapshot, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return Snapshot{}, err
	}
	defer tx.Rollback(ctx)
	var snap Snapshot
	if err = tx.QueryRow(ctx, `SELECT COALESCE(max(id),0) FROM job_events`).Scan(&snap.Cursor); err != nil {
		return snap, err
	}
	if err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE status='queued'),count(*) FILTER(WHERE status='running'),count(*) FILTER(WHERE status='succeeded'),count(*) FILTER(WHERE status='dead'),count(*) FILTER(WHERE status='canceled') FROM jobs`).Scan(&snap.Stats.Queued, &snap.Stats.Running, &snap.Stats.Succeeded, &snap.Stats.Dead, &snap.Stats.Canceled); err != nil {
		return snap, err
	}
	rows, err := tx.Query(ctx, `SELECT `+columns+` FROM jobs WHERE status IN ('queued','running') ORDER BY priority DESC,available_at,sequence`)
	if err != nil {
		return snap, err
	}
	snap.Jobs = make([]Job, 0)
	for rows.Next() {
		j, e := scanJob(rows)
		if e != nil {
			rows.Close()
			return snap, e
		}
		snap.Jobs = append(snap.Jobs, j)
	}
	if err = rows.Err(); err != nil {
		return snap, err
	}
	workerRows, err := tx.Query(ctx, `SELECT id,concurrency,seen_at>clock_timestamp()-interval '5 seconds' FROM workers WHERE seen_at>clock_timestamp()-interval '5 minutes' OR EXISTS(SELECT 1 FROM jobs WHERE worker_id=workers.id AND status='running') ORDER BY id`)
	if err != nil {
		return snap, err
	}
	snap.Workers = make([]WorkerState, 0)
	for workerRows.Next() {
		var w WorkerState
		if err := workerRows.Scan(&w.ID, &w.Concurrency, &w.Online); err != nil {
			workerRows.Close()
			return snap, err
		}
		w.Jobs = make([]WorkerJob, 0)
		snap.Workers = append(snap.Workers, w)
	}
	if err := workerRows.Err(); err != nil {
		return snap, err
	}
	return snap, tx.Commit(ctx)
}
func (s *Store) Events(ctx context.Context, after int64) ([]Event, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,job,previous FROM job_events WHERE id>$1 ORDER BY id LIMIT 256`, after)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]Event, 0)
	for rows.Next() {
		var e Event
		var raw json.RawMessage
		if err = rows.Scan(&e.ID, &raw, &e.Previous); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(raw, &e.Job); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}
func (s *Store) PruneEvents(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `DELETE FROM job_events WHERE created_at<clock_timestamp()-interval '10 minutes'`)
	return err
}
