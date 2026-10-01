package queue

import "context"

// WorkerState is a recent worker heartbeat and its currently leased jobs.
type WorkerState struct {
	ID          string      `json:"id"`
	Concurrency int         `json:"concurrency"`
	Online      bool        `json:"online"`
	Jobs        []WorkerJob `json:"jobs"`
}
type WorkerJob struct {
	ID          string `json:"id"`
	Attempt     int    `json:"attempt"`
	Kind        string `json:"kind"`
	Queue       string `json:"queue"`
	MaxAttempts int    `json:"max_attempts"`
}

func (s *Store) AnnounceWorker(ctx context.Context, id string, concurrency int) error {
	_, err := s.pool.Exec(ctx, `INSERT INTO workers(id,concurrency,seen_at) VALUES($1,$2,clock_timestamp())
 ON CONFLICT(id) DO UPDATE SET concurrency=EXCLUDED.concurrency,seen_at=EXCLUDED.seen_at`, id, concurrency)
	return err
}

func (s *Store) Workers(ctx context.Context) ([]WorkerState, error) {
	rows, err := s.pool.Query(ctx, `SELECT w.id,w.concurrency,w.seen_at>clock_timestamp()-interval '5 seconds',j.id::text,j.attempt,j.kind,j.queue,j.max_attempts
 FROM workers w LEFT JOIN jobs j ON j.worker_id=w.id AND j.status='running'
 WHERE w.seen_at>clock_timestamp()-interval '5 minutes' OR j.id IS NOT NULL ORDER BY w.id,j.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	workers := make([]WorkerState, 0)
	for rows.Next() {
		var w WorkerState
		var id *string
		var attempt, maxAttempts *int
		var kind, queueName *string
		if err := rows.Scan(&w.ID, &w.Concurrency, &w.Online, &id, &attempt, &kind, &queueName, &maxAttempts); err != nil {
			return nil, err
		}
		if len(workers) == 0 || workers[len(workers)-1].ID != w.ID {
			w.Jobs = make([]WorkerJob, 0)
			workers = append(workers, w)
		}
		if id != nil {
			i := len(workers) - 1
			workers[i].Jobs = append(workers[i].Jobs, WorkerJob{ID: *id, Attempt: *attempt, Kind: *kind, Queue: *queueName, MaxAttempts: *maxAttempts})
		}
	}
	return workers, rows.Err()
}

// Pending returns the first jobs in claim order, followed by delayed retries.
func (s *Store) Pending(ctx context.Context) ([]Job, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+columns+` FROM jobs WHERE status='queued'
 ORDER BY (available_at>clock_timestamp()),priority DESC,available_at,sequence LIMIT 12`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	jobs := make([]Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}
