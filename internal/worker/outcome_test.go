package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

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
