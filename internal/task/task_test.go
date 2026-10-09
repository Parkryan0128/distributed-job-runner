package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

func TestTaskPayloadValidation(t *testing.T) {
	for _, c := range []struct{ kind, payload string }{
		{"shell", `{}`}, {"demo", `{"work_ms":120001}`}, {"demo", `{"fail_until":-1}`}, {"demo", `{"unknown":1}`},
		{"demo", `null`}, {"demo", `[]`},
	} {
		t.Run(c.kind+c.payload, func(t *testing.T) {
			_, err := task.Run(context.Background(), queue.Job{Kind: c.kind, Payload: json.RawMessage(c.payload)})
			var pe task.PermanentError
			if !errors.As(err, &pe) {
				t.Fatalf("expected permanent error: %v", err)
			}
		})
	}
}

func TestDemoFailureRecoveryAndCancellation(t *testing.T) {
	j := queue.Job{Kind: "demo", Payload: json.RawMessage(`{"fail_until":1,"work_ms":0}`), Attempt: 1}
	if _, err := task.Run(context.Background(), j); err == nil {
		t.Fatal("expected simulated failure")
	}
	j.Attempt = 2
	if _, err := task.Run(context.Background(), j); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	j.Payload = json.RawMessage(`{"work_ms":10000}`)
	if _, err := task.Run(ctx, j); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation: %v", err)
	}
}
