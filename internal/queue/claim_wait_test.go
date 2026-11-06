package queue_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestClaimLeaseStartsAfterWaitingForEventPublication(t *testing.T) {
	s, p := testdb.New(t)
	submit(t, s, queue.Submit{MaxAttempts: 1})
	tx, pid := holdLock(t, p, `SELECT pg_advisory_xact_lock(7241074)`)
	type outcome struct {
		job queue.Job
		err error
	}
	result := make(chan outcome, 1)
	const lease = 200 * time.Millisecond
	go func() {
		j, err := s.Claim(ctx, "worker", []string{"default"}, lease)
		result <- outcome{j, err}
	}()
	waitForBlockedQuery(t, p, pid)
	time.Sleep(lease + 50*time.Millisecond)
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	got := <-result
	if got.err != nil {
		t.Fatal(got.err)
	}
	if remaining := time.Until(*got.job.LeaseUntil); remaining < lease/2 {
		t.Fatalf("lock wait consumed the lease: remaining=%s", remaining)
	}
	if err := s.Finish(ctx, got.job, json.RawMessage(`{"ok":true}`), "", false); err != nil {
		t.Fatalf("freshly claimed job could not finish: %v", err)
	}
	if got := detail(t, s, got.job.ID); got.Status != "succeeded" || got.Attempts[0].Status != "succeeded" {
		t.Fatalf("successful work was lost: %+v", got)
	}
}
