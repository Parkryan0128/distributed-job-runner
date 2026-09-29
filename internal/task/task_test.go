package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

func TestChecksumAndStatisticsResults(t *testing.T) {
	result, err := task.Run(context.Background(), queue.Job{Kind: "checksum", Payload: json.RawMessage(`{"text":"abc"}`)})
	if err != nil {
		t.Fatal(err)
	}
	var hash struct {
		SHA256 string `json:"sha256"`
		Bytes  int    `json:"bytes"`
	}
	if err = json.Unmarshal(result, &hash); err != nil {
		t.Fatal(err)
	}
	if hash.SHA256 != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" || hash.Bytes != 3 {
		t.Fatalf("checksum: %s", result)
	}
	result, err = task.Run(context.Background(), queue.Job{Kind: "statistics", Payload: json.RawMessage(`{"values":[-2,4,7]}`)})
	if err != nil {
		t.Fatal(err)
	}
	var stats map[string]float64
	if err = json.Unmarshal(result, &stats); err != nil {
		t.Fatal(err)
	}
	if stats["sum"] != 9 || stats["mean"] != 3 || stats["min"] != -2 || stats["max"] != 7 || stats["count"] != 3 {
		t.Fatalf("statistics: %s", result)
	}
}

func TestTaskPayloadValidation(t *testing.T) {
	for _, c := range []struct{ kind, payload string }{
		{"shell", `{}`}, {"demo", `{"work_ms":120001}`}, {"demo", `{"fail_until":-1}`}, {"demo", `{"unknown":1}`},
		{"demo", `null`}, {"demo", `[]`}, {"statistics", `{"values":[]}`}, {"statistics", `{"values":[1e13]}`}, {"checksum", `{"text":7}`},
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
