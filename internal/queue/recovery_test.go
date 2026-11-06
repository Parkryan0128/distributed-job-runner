package queue_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestRecoveryRollsBackTheWholeBatchWhenALaterAttemptFails(t *testing.T) {
	s, p := testdb.New(t)
	submit(t, s, queue.Submit{MaxAttempts: 3})
	first := claim(t, s, "recoverable-worker")
	submit(t, s, queue.Submit{MaxAttempts: 1})
	second := claim(t, s, "blocked-worker")
	execute(t, p, `UPDATE jobs SET lease_until=clock_timestamp()-interval '2 seconds' WHERE id=$1`, first.ID)
	execute(t, p, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, second.ID)
	before := []queue.Detail{detail(t, s, first.ID), detail(t, s, second.ID)}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	execute(t, p, `ALTER TABLE attempts ADD CONSTRAINT reject_second_recovery CHECK(worker_id <> 'blocked-worker' OR status='running')`)
	if n, err := s.Recover(ctx); err == nil || n != 0 {
		t.Fatalf("failed batch: recovered=%d err=%v", n, err)
	}
	for _, original := range before {
		if after := detail(t, s, original.ID); !reflect.DeepEqual(original, after) {
			t.Fatalf("partial recovery committed: before=%+v after=%+v", original, after)
		}
	}
	events, err := s.Events(ctx, snapshot.Cursor)
	if err != nil || len(events) != 0 {
		t.Fatalf("failed batch published events: %+v %v", events, err)
	}

	execute(t, p, `ALTER TABLE attempts DROP CONSTRAINT reject_second_recovery`)
	if n, err := s.Recover(ctx); err != nil || n != 2 {
		t.Fatalf("retry batch: recovered=%d err=%v", n, err)
	}
	for i, want := range []string{"queued", "dead"} {
		got := detail(t, s, before[i].ID)
		if got.Status != want || got.Attempt != 1 || got.WorkerID != nil || got.LeaseUntil != nil || got.Result != nil || got.Error != "worker lease expired" {
			t.Fatalf("recovered job: %+v", got)
		}
		if len(got.Attempts) != 1 || got.Attempts[0].Status != "expired" || got.Attempts[0].FinishedAt == nil || got.Attempts[0].Error != "worker lease expired" {
			t.Fatalf("recovered history: %+v", got.Attempts)
		}
	}
	events, err = s.Events(ctx, snapshot.Cursor)
	if err != nil || len(events) != 2 || events[0].Job.ID != first.ID || events[0].Job.Status != "queued" || events[1].Job.ID != second.ID || events[1].Job.Status != "dead" {
		t.Fatalf("recovery events: %+v %v", events, err)
	}
	if n, err := s.Recover(ctx); err != nil || n != 0 {
		t.Fatalf("completed batch recovered twice: %d %v", n, err)
	}
}

func TestRecoverySkipsLockedExpiredJobsAndRevisitsThem(t *testing.T) {
	s, p := testdb.New(t)
	submit(t, s, queue.Submit{})
	first := claim(t, s, "first-worker")
	submit(t, s, queue.Submit{})
	second := claim(t, s, "second-worker")
	execute(t, p, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second'`)
	before := detail(t, s, first.ID)
	locked, _ := holdLock(t, p, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, first.ID)
	op, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	if n, err := s.Recover(op); err != nil || n != 1 {
		t.Fatalf("locked job blocked recovery: recovered=%d err=%v", n, err)
	}
	if got := detail(t, s, second.ID); got.Status != "queued" || got.Attempts[0].Status != "expired" {
		t.Fatalf("unlocked job was not recovered: %+v", got)
	}
	if got := detail(t, s, first.ID); !reflect.DeepEqual(before, got) {
		t.Fatalf("locked job changed: before=%+v after=%+v", before, got)
	}
	if err := locked.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if n, err := s.Recover(ctx); err != nil || n != 1 {
		t.Fatalf("next recovery missed unlocked job: recovered=%d err=%v", n, err)
	}
	if got := detail(t, s, first.ID); got.Status != "queued" || got.Attempts[0].Status != "expired" {
		t.Fatalf("previously locked job was not recovered: %+v", got)
	}
	if n, err := s.Recover(ctx); err != nil || n != 0 {
		t.Fatalf("repeated recovery changed completed attempts: %d %v", n, err)
	}
}
