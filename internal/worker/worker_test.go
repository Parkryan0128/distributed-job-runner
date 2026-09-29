package worker_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
	"github.com/Parkryan0128/distributed-job-runner/internal/worker"
)

func start(t *testing.T, s *queue.Store, id string, h worker.Handler) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	w := worker.Worker{Store: s, ID: id, Queues: []string{"default"}, Concurrency: 2, Lease: 600 * time.Millisecond, Poll: 20 * time.Millisecond, Handler: h, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(6 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return cancel, done
}

func submit(t *testing.T, s *queue.Store, kind, payload string, attempts, timeout int) queue.Job {
	t.Helper()
	j, _, err := s.Submit(context.Background(), queue.Submit{Kind: kind, Payload: json.RawMessage(payload), MaxAttempts: attempts, TimeoutSeconds: timeout}, "")
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func await(t *testing.T, s *queue.Store, id, status string) queue.Detail {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		j, err := s.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == status {
			return j
		}
		time.Sleep(20 * time.Millisecond)
	}
	j, err := s.Get(context.Background(), id)
	t.Fatalf("waiting for %s, got %+v (%v)", status, j, err)
	return queue.Detail{}
}

func TestMultipleWorkersRespectConcurrencyAndExecuteEveryJob(t *testing.T) {
	s, _ := testdb.New(t)
	var active, peak atomic.Int32
	var mu sync.Mutex
	seen := map[string]int{}
	h := func(ctx context.Context, j queue.Job) (json.RawMessage, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for p := peak.Load(); n > p; p = peak.Load() {
			if peak.CompareAndSwap(p, n) {
				break
			}
		}
		mu.Lock()
		seen[j.ID]++
		mu.Unlock()
		return task.Run(ctx, j)
	}
	jobs := make([]queue.Job, 0)
	for range 12 {
		jobs = append(jobs, submit(t, s, "demo", `{"work_ms":80}`, 3, 30))
	}
	start(t, s, "worker-a", h)
	start(t, s, "worker-b", h)
	for _, j := range jobs {
		got := await(t, s, j.ID, "succeeded")
		if got.Attempt != 1 {
			t.Fatal("unexpected retry")
		}
	}
	if peak.Load() < 2 || peak.Load() > 4 {
		t.Fatalf("concurrent handlers: %d", peak.Load())
	}
	mu.Lock()
	defer mu.Unlock()
	for _, j := range jobs {
		if seen[j.ID] != 1 {
			t.Fatalf("job executed %d times", seen[j.ID])
		}
	}
}

func TestWorkerRetriesTransientFailureAndKeepsHeartbeat(t *testing.T) {
	s, _ := testdb.New(t)
	j := submit(t, s, "demo", `{"work_ms":800,"fail_until":1}`, 3, 30)
	start(t, s, "worker", nil)
	got := await(t, s, j.ID, "succeeded")
	if got.Attempt != 2 || len(got.Attempts) != 2 || got.Attempts[0].Status != "failed" || got.Attempts[1].Status != "succeeded" {
		t.Fatalf("retry history: %+v", got)
	}
}

func TestTimeoutAndPanicReachRetryLimit(t *testing.T) {
	for _, kind := range []string{"timeout", "panic"} {
		t.Run(kind, func(t *testing.T) {
			s, _ := testdb.New(t)
			j := submit(t, s, "demo", `{"work_ms":10000}`, 1, 1)
			var h worker.Handler
			if kind == "panic" {
				h = func(ctx context.Context, job queue.Job) (json.RawMessage, error) {
					if job.ID == j.ID {
						panic("broken handler")
					}
					return task.Run(ctx, job)
				}
			}
			start(t, s, "worker", h)
			got := await(t, s, j.ID, "dead")
			if got.Attempt != 1 || got.Attempts[0].Status != "failed" {
				t.Fatalf("failure: %+v", got)
			}
			want := "deadline exceeded"
			if kind == "panic" {
				want = "handler panic"
			}
			if !strings.Contains(got.Error, want) {
				t.Fatalf("error: %s", got.Error)
			}
			next := submit(t, s, "demo", `{}`, 1, 30)
			await(t, s, next.ID, "succeeded")
		})
	}
}

func TestRunningCancellationStopsHandler(t *testing.T) {
	s, _ := testdb.New(t)
	j := submit(t, s, "demo", `{"work_ms":10000}`, 3, 30)
	stopped := make(chan struct{})
	h := func(ctx context.Context, j queue.Job) (json.RawMessage, error) {
		defer close(stopped)
		return task.Run(ctx, j)
	}
	start(t, s, "worker", h)
	await(t, s, j.ID, "running")
	if err := s.Cancel(context.Background(), j.ID); err != nil {
		t.Fatal(err)
	}
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("canceled handler kept running")
	}
	got := await(t, s, j.ID, "canceled")
	if got.Attempts[0].Status != "canceled" || got.Result != nil {
		t.Fatalf("cancellation overwritten: %+v", got)
	}
}

func TestShutdownReturnsInFlightWorkForRetry(t *testing.T) {
	s, _ := testdb.New(t)
	j := submit(t, s, "demo", `{"work_ms":10000}`, 3, 30)
	cancel, _ := start(t, s, "worker", nil)
	await(t, s, j.ID, "running")
	cancel()
	got := await(t, s, j.ID, "queued")
	if got.Attempts[0].Status != "failed" || !strings.Contains(got.Error, "canceled") {
		t.Fatalf("shutdown: %+v", got)
	}
}

func TestWorkerRecoversJobLeftByCrashedProcess(t *testing.T) {
	s, p := testdb.New(t)
	j := submit(t, s, "demo", `{}`, 3, 30)
	if _, err := s.Claim(context.Background(), "crashed", []string{"default"}, time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Exec(context.Background(), `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, j.ID); err != nil {
		t.Fatal(err)
	}
	start(t, s, "survivor", nil)
	got := await(t, s, j.ID, "succeeded")
	if got.Attempt != 2 || got.Attempts[0].Status != "expired" || got.Attempts[1].WorkerID != "survivor" {
		t.Fatalf("recovery: %+v", got)
	}
}
