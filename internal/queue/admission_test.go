package queue_test

import (
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestConcurrentAdmissionStopsAtCapacity(t *testing.T) {
	s, _ := testdb.New(t)
	var accepted, rejected atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range 20 {
		wg.Go(func() {
			<-start
			_, created, err := s.SubmitLimited(ctx, queue.Submit{Kind: "demo", Payload: json.RawMessage(`{}`)}, "", 5)
			switch {
			case err == nil && created:
				accepted.Add(1)
			case errors.Is(err, queue.ErrCapacity):
				rejected.Add(1)
			default:
				t.Errorf("submission: created=%v err=%v", created, err)
			}
		})
	}
	close(start)
	wg.Wait()
	stats, err := s.Stats(ctx)
	if err != nil || accepted.Load() != 5 || rejected.Load() != 15 || stats.Queued != 5 {
		t.Fatalf("accepted=%d rejected=%d stats=%+v err=%v", accepted.Load(), rejected.Load(), stats, err)
	}
}

func TestRunningJobsCountTowardCapacityAndCompletionReleasesIt(t *testing.T) {
	s, _ := testdb.New(t)
	input := queue.Submit{Kind: "demo", Payload: json.RawMessage(`{}`)}
	j, created, err := s.SubmitLimited(ctx, input, "original", 1)
	if err != nil || !created {
		t.Fatalf("first submission: %v %v", created, err)
	}
	claimed := claim(t, s, "worker")
	if _, _, err := s.SubmitLimited(ctx, input, "new", 1); !errors.Is(err, queue.ErrCapacity) {
		t.Fatalf("running job did not occupy capacity: %v", err)
	}
	changed := input
	changed.Priority = 1
	if _, _, err := s.SubmitLimited(ctx, changed, "original", 1); !errors.Is(err, queue.ErrIdempotency) {
		t.Fatalf("conflicting replay at capacity: %v", err)
	}
	if err := s.Finish(ctx, claimed, json.RawMessage(`{"ok":true}`), "", false); err != nil {
		t.Fatal(err)
	}
	replayed, created, err := s.SubmitLimited(ctx, input, "original", 1)
	if err != nil || created || replayed.ID != j.ID || replayed.Status != "succeeded" {
		t.Fatalf("completed replay: %+v %v %v", replayed, created, err)
	}
	if _, created, err := s.SubmitLimited(ctx, input, "new", 1); err != nil || !created {
		t.Fatalf("capacity was not released: %v %v", created, err)
	}
}

func TestSubmissionKeysAndValidationDoNotCreateExtraJobs(t *testing.T) {
	s, _ := testdb.New(t)
	input := queue.Submit{Kind: "demo", Payload: json.RawMessage(`{}`)}
	first := submit(t, s, input)
	second := submit(t, s, input)
	if first.ID == second.ID {
		t.Fatal("requests without keys were deduplicated")
	}
	key := strings.Repeat("a", 128)
	j, created, err := s.Submit(ctx, input, key)
	if err != nil || !created {
		t.Fatalf("128-byte key: %v %v", created, err)
	}
	replay, created, err := s.Submit(ctx, input, key)
	if err != nil || created || replay.ID != j.ID {
		t.Fatalf("replay: %+v %v %v", replay, created, err)
	}
	if _, _, err := s.Submit(ctx, input, key+"a"); err == nil {
		t.Fatal("oversized key accepted")
	}
	invalid := input
	invalid.Priority = -1
	if _, _, err := s.SubmitLimited(ctx, invalid, "invalid", 10); err == nil {
		t.Fatal("invalid request accepted")
	}
	stats, err := s.Stats(ctx)
	if err != nil || stats.Queued != 3 {
		t.Fatalf("unexpected stored jobs: %+v %v", stats, err)
	}
}

func TestDifferentWorkerCannotRenewOrCompleteAnAttempt(t *testing.T) {
	s, _ := testdb.New(t)
	submit(t, s, queue.Submit{})
	j := claim(t, s, "owner")
	wrong := j
	other := "other"
	wrong.WorkerID = &other
	if err := s.Heartbeat(ctx, wrong, time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("wrong worker heartbeat: %v", err)
	}
	if err := s.Finish(ctx, wrong, json.RawMessage(`{}`), "", false); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("wrong worker completion: %v", err)
	}
	got := detail(t, s, j.ID)
	if got.Status != "running" || *got.WorkerID != "owner" || !got.LeaseUntil.Equal(*j.LeaseUntil) || got.Attempts[0].FinishedAt != nil {
		t.Fatalf("another worker changed the attempt: %+v", got)
	}
	if err := s.Finish(ctx, j, json.RawMessage(`{}`), "", false); err != nil {
		t.Fatal(err)
	}
}
