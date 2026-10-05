package queue_test

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestEmptySnapshotAndEventLog(t *testing.T) {
	s, _ := testdb.New(t)
	snap, err := s.Snapshot(ctx)
	if err != nil || snap.Cursor != 0 || snap.Stats != (queue.Stats{}) || snap.Jobs == nil || len(snap.Jobs) != 0 || snap.Workers == nil || len(snap.Workers) != 0 {
		t.Fatalf("empty snapshot: %+v %v", snap, err)
	}
	events, err := s.Events(ctx, 0)
	if err != nil || events == nil || len(events) != 0 {
		t.Fatalf("empty events: %+v %v", events, err)
	}
}

func TestSnapshotAndEventsDescribeCommittedTransitions(t *testing.T) {
	s, _ := testdb.New(t)
	if err := s.AnnounceWorker(ctx, "worker", 2); err != nil {
		t.Fatal(err)
	}
	submit(t, s, queue.Submit{})
	j := claim(t, s, "worker")
	if err := s.Heartbeat(ctx, j, time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(ctx, j, json.RawMessage(`{"ok":true}`), "", false); err != nil {
		t.Fatal(err)
	}
	queued := submit(t, s, queue.Submit{Priority: 9})
	snap, err := s.Snapshot(ctx)
	if err != nil || snap.Stats != (queue.Stats{Queued: 1, Succeeded: 1}) || len(snap.Jobs) != 1 || snap.Jobs[0].ID != queued.ID || len(snap.Workers) != 1 || !snap.Workers[0].Online {
		t.Fatalf("snapshot: %+v %v", snap, err)
	}
	events, err := s.Events(ctx, 0)
	if err != nil || len(events) != 4 {
		t.Fatalf("transitions (heartbeat must not add one): %+v %v", events, err)
	}
	for i, want := range []struct{ id, status, previous string }{
		{j.ID, "queued", ""}, {j.ID, "running", "queued"}, {j.ID, "succeeded", "running"}, {queued.ID, "queued", ""},
	} {
		e := events[i]
		if e.Job.ID != want.id || e.Job.Status != want.status || e.Previous != want.previous || (i > 0 && e.ID <= events[i-1].ID) {
			t.Fatalf("event %d: %+v", i, e)
		}
	}
	if snap.Cursor != events[3].ID {
		t.Fatalf("snapshot cursor=%d last event=%d", snap.Cursor, events[3].ID)
	}
	if err := s.Cancel(ctx, queued.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(ctx, queued.ID); err != nil {
		t.Fatal(err)
	}
	after, err := s.Events(ctx, snap.Cursor)
	if err != nil || len(after) != 1 || after[0].Job.Status != "canceled" || after[0].Previous != "queued" {
		t.Fatalf("events after snapshot: %+v %v", after, err)
	}
}

func TestEventPaginationAndRetention(t *testing.T) {
	s, p := testdb.New(t)
	for range 260 {
		submit(t, s, queue.Submit{})
	}
	first, err := s.Events(ctx, 0)
	if err != nil || len(first) != 256 {
		t.Fatalf("first batch: len=%d err=%v", len(first), err)
	}
	last := first[len(first)-1].ID
	second, err := s.Events(ctx, last)
	if err != nil || len(second) != 4 || second[0].ID <= last {
		t.Fatalf("second batch: %+v %v", second, err)
	}
	execute(t, p, `UPDATE job_events SET created_at=clock_timestamp()-interval '11 minutes' WHERE id<=$1`, last)
	if err := s.PruneEvents(ctx); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.Events(ctx, 0)
	if err != nil || len(remaining) != 4 || remaining[0].ID != second[0].ID {
		t.Fatalf("retained events: %+v %v", remaining, err)
	}
	stats, err := s.Stats(ctx)
	if err != nil || stats.Queued != 260 {
		t.Fatalf("event pruning changed jobs: %+v %v", stats, err)
	}
}

func TestFailedTransitionDoesNotPublishAnEvent(t *testing.T) {
	s, p := testdb.New(t)
	j := submit(t, s, queue.Submit{})
	snap, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	execute(t, p, `ALTER TABLE attempts ADD CONSTRAINT reject_worker CHECK(worker_id <> 'broken')`)
	if _, err := s.Claim(ctx, "broken", []string{"default"}, time.Minute); err == nil {
		t.Fatal("expected attempt insert failure")
	}
	events, err := s.Events(ctx, snap.Cursor)
	if err != nil || len(events) != 0 {
		t.Fatalf("rolled back event was visible: %+v %v", events, err)
	}
	if got := detail(t, s, j.ID); got.Status != "queued" || len(got.Attempts) != 0 {
		t.Fatalf("failed claim changed job: %+v", got)
	}
}

func TestHistoryRetentionPreservesActiveAndRecentJobsAndCascadesAttempts(t *testing.T) {
	s, p := testdb.New(t)
	old := make([]string, 0)
	for _, state := range []string{"succeeded", "dead", "canceled"} {
		submit(t, s, queue.Submit{})
		j := claim(t, s, "worker")
		var err error
		if state == "canceled" {
			err = s.Cancel(ctx, j.ID)
		} else {
			failure := ""
			if state == "dead" {
				failure = "permanent failure"
			}
			err = s.Finish(ctx, j, json.RawMessage(`{}`), failure, true)
		}
		if err != nil {
			t.Fatal(err)
		}
		old = append(old, j.ID)
	}
	submit(t, s, queue.Submit{})
	running := claim(t, s, "worker")
	queued := submit(t, s, queue.Submit{})
	execute(t, p, `UPDATE jobs SET updated_at=clock_timestamp()-interval '25 hours'`)
	recent := submit(t, s, queue.Submit{})
	if err := s.Cancel(ctx, recent.ID); err != nil {
		t.Fatal(err)
	}
	for _, retention := range []time.Duration{0, -time.Hour} {
		if n, err := s.PruneHistory(ctx, retention); err != nil || n != 0 {
			t.Fatalf("disabled retention: %d %v", n, err)
		}
	}
	if n, err := s.PruneHistory(ctx, 24*time.Hour); err != nil || n != 3 {
		t.Fatalf("pruned=%d err=%v", n, err)
	}
	for _, id := range old {
		if _, err := s.Get(ctx, id); !errors.Is(err, queue.ErrNotFound) {
			t.Fatalf("old terminal job retained: %v", err)
		}
		var attempts int
		if err := p.QueryRow(ctx, `SELECT count(*) FROM attempts WHERE job_id=$1`, id).Scan(&attempts); err != nil || attempts != 0 {
			t.Fatalf("orphan attempts=%d err=%v", attempts, err)
		}
	}
	for _, id := range []string{running.ID, queued.ID, recent.ID} {
		if _, err := s.Get(ctx, id); err != nil {
			t.Fatalf("retained job disappeared: %v", err)
		}
	}
}
