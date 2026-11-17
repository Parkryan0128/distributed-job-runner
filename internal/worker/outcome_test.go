package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestWorkerPersistsHandlerResult(t *testing.T) {
	s, _ := testdb.New(t)
	j := submit(t, s, "demo", `{}`, 3, 30)
	start(t, s, "result-worker", func(ctx context.Context, job queue.Job) (json.RawMessage, error) {
		return json.Marshal(map[string]any{"job_id": job.ID, "attempt": job.Attempt, "value": 42})
	})
	got := await(t, s, j.ID, "succeeded")
	var result struct {
		JobID   string `json:"job_id"`
		Attempt int    `json:"attempt"`
		Value   int    `json:"value"`
	}
	if err := json.Unmarshal(got.Result, &result); err != nil || result.JobID != j.ID || result.Attempt != 1 || result.Value != 42 {
		t.Fatalf("result: %s (%v)", got.Result, err)
	}
	if got.Error != "" || got.WorkerID != nil || got.LeaseUntil != nil || len(got.Attempts) != 1 || got.Attempts[0].FinishedAt == nil || got.Attempts[0].WorkerID != "result-worker" {
		t.Fatalf("completion: %+v", got)
	}
}

func TestWorkerRecognizesWrappedPermanentFailure(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(fmt.Sprint(wrapped), func(t *testing.T) {
			s, _ := testdb.New(t)
			j := submit(t, s, "demo", `{}`, 10, 30)
			var failure error = task.PermanentError{Err: errors.New("invalid input")}
			if wrapped {
				failure = fmt.Errorf("handler: %w", failure)
			}
			start(t, s, "permanent-worker", func(ctx context.Context, job queue.Job) (json.RawMessage, error) {
				if job.ID == j.ID {
					return json.RawMessage(`{"partial":true}`), failure
				}
				return task.Run(ctx, job)
			})
			got := await(t, s, j.ID, "dead")
			if got.Attempt != 1 || got.Result != nil || got.Error != failure.Error() || len(got.Attempts) != 1 || got.Attempts[0].Status != "failed" || got.Attempts[0].Error != failure.Error() {
				t.Fatalf("permanent outcome: %+v", got)
			}
			next := submit(t, s, "demo", `{}`, 1, 30)
			await(t, s, next.ID, "succeeded")
		})
	}
}

func TestWorkerRejectsSuccessReturnedAfterDeadline(t *testing.T) {
	s, _ := testdb.New(t)
	j := submit(t, s, "demo", `{}`, 1, 1)
	start(t, s, "deadline-worker", func(ctx context.Context, job queue.Job) (json.RawMessage, error) {
		<-ctx.Done()
		return json.RawMessage(`{"late":true}`), nil
	})
	got := await(t, s, j.ID, "dead")
	if got.Result != nil || !strings.Contains(got.Error, "deadline exceeded") || got.Attempt != 1 || len(got.Attempts) != 1 || got.Attempts[0].Status != "failed" {
		t.Fatalf("late success was persisted: %+v", got)
	}
}

func TestWorkerRetriesPanicAndSucceedsOnLastAttempt(t *testing.T) {
	s, _ := testdb.New(t)
	j := submit(t, s, "demo", "{}", 2, 30)
	start(t, s, "retry-worker", func(_ context.Context, job queue.Job) (json.RawMessage, error) {
		if job.Attempt == 1 {
			panic("interrupted")
		}
		return json.RawMessage("42"), nil
	})

	got := await(t, s, j.ID, "succeeded")
	if string(got.Result) != "42" || got.Error != "" || len(got.Attempts) != 2 {
		t.Fatalf("result after retry: %+v", got)
	}
	first, last := got.Attempts[0], got.Attempts[1]
	if first.Status != "failed" || first.Error != "handler panic: interrupted" || last.Status != "succeeded" || last.Error != "" {
		t.Fatalf("attempt history: %+v", got.Attempts)
	}
}

func TestShutdownDiscardsLateSuccessAndAnotherWorkerCanRetry(t *testing.T) {
	s, _ := testdb.New(t)
	j := submit(t, s, "demo", "{}", 2, 30)
	stop := start(t, s, "leaving", func(ctx context.Context, _ queue.Job) (json.RawMessage, error) {
		<-ctx.Done()
		return json.RawMessage("1"), nil
	})
	await(t, s, j.ID, "running")
	stop()

	got, err := s.Get(t.Context(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "queued" || got.Result != nil || len(got.Attempts) != 1 || got.Attempts[0].Status != "failed" || got.Attempts[0].Error != context.Canceled.Error() {
		t.Fatalf("job after shutdown: %+v", got)
	}

	start(t, s, "replacement", func(context.Context, queue.Job) (json.RawMessage, error) {
		return json.RawMessage("2"), nil
	})
	got = await(t, s, j.ID, "succeeded")
	if string(got.Result) != "2" || len(got.Attempts) != 2 || got.Attempts[1].WorkerID != "replacement" {
		t.Fatalf("replacement result: %+v", got)
	}
}

func TestExpiredHandlerCannotReplaceRetriedResult(t *testing.T) {
	s, pool := testdb.New(t)
	j := submit(t, s, "demo", "{}", 2, 30)
	started := make(chan struct{})
	lostLease := make(chan struct{})
	release := make(chan struct{})
	releaseFirst := sync.OnceFunc(func() { close(release) })
	stop := start(t, s, "worker", func(ctx context.Context, job queue.Job) (json.RawMessage, error) {
		if job.Attempt == 1 {
			close(started)
			<-ctx.Done()
			close(lostLease)
			<-release
			return nil, task.PermanentError{Err: errors.New("stale failure")}
		}
		return json.RawMessage("42"), nil
	})
	t.Cleanup(releaseFirst)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("first attempt did not start")
	}
	if _, err := pool.Exec(t.Context(), "UPDATE jobs SET lease_until = clock_timestamp() WHERE id = $1", j.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-lostLease:
	case <-time.After(5 * time.Second):
		t.Fatal("losing its lease did not cancel the handler")
	}
	await(t, s, j.ID, "succeeded")
	snap, err := s.Snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	releaseFirst()
	stop()

	got, err := s.Get(t.Context(), j.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != "succeeded" || string(got.Result) != "42" || got.Error != "" || len(got.Attempts) != 2 {
		t.Fatalf("late failure changed the job: %+v", got)
	}
	first, second := got.Attempts[0], got.Attempts[1]
	if first.Status != "expired" || second.Status != "succeeded" || first.WorkerID != second.WorkerID {
		t.Fatalf("attempt history: %+v", got.Attempts)
	}
	events, err := s.Events(t.Context(), snap.Cursor)
	if err != nil || len(events) != 0 {
		t.Fatalf("events after late failure: %+v, err=%v", events, err)
	}
}
