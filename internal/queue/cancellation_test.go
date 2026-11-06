package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestCanceledSubmissionsLeaveNoJobsOrEventsAndCanReuseTheirKey(t *testing.T) {
	for _, tc := range []struct {
		name    string
		limited bool
		lock    string
	}{
		{"submit publication", false, `SELECT pg_advisory_xact_lock(7241074)`},
		{"limited publication", true, `SELECT pg_advisory_xact_lock(7241074)`},
		{"limited admission", true, `SELECT pg_advisory_xact_lock(7241075)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p := testdb.New(t)
			input := queue.Submit{Kind: "demo", Payload: json.RawMessage(`{}`)}
			place := func(op context.Context) (queue.Job, bool, error) {
				if tc.limited {
					return s.SubmitLimited(op, input, "canceled-request", 1)
				}
				return s.Submit(op, input, "canceled-request")
			}
			locked, pid := holdLock(t, p, tc.lock)
			pending, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, _, err := place(pending)
				done <- err
			}()
			waitForBlockedQuery(t, p, pid)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled submission: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("submission ignored cancellation")
			}
			if err := locked.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			job, created, err := place(ctx)
			if err != nil || !created || job.Attempt != 0 || job.Status != "queued" {
				t.Fatalf("retry did not create a fresh job: %+v created=%v err=%v", job, created, err)
			}
			if replay, created, err := place(ctx); err != nil || created || replay.ID != job.ID {
				t.Fatalf("retry replay: %+v created=%v err=%v", replay, created, err)
			}
			stats, err := s.Stats(ctx)
			if err != nil || stats != (queue.Stats{Queued: 1}) {
				t.Fatalf("canceled submission leaked a job: %+v %v", stats, err)
			}
			events, err := s.Events(ctx, 0)
			if err != nil || len(events) != 1 || events[0].Job.ID != job.ID || events[0].Job.Status != "queued" {
				t.Fatalf("canceled submission leaked an event: %+v %v", events, err)
			}
		})
	}
}

func TestCanceledClaimLeavesJobAvailableWithoutAnAttempt(t *testing.T) {
	s, p := testdb.New(t)
	job := submit(t, s, queue.Submit{})
	before := detail(t, s, job.ID)
	locked, pid := holdLock(t, p, `SELECT pg_advisory_xact_lock(7241074)`)
	pending, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := s.Claim(pending, "canceled-worker", []string{"default"}, time.Minute)
		done <- err
	}()
	waitForBlockedQuery(t, p, pid)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled claim: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("claim ignored cancellation")
	}
	if err := locked.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if after := detail(t, s, job.ID); !reflect.DeepEqual(before, after) {
		t.Fatalf("canceled claim changed job: before=%+v after=%+v", before, after)
	}
	events, err := s.Events(ctx, 0)
	if err != nil || len(events) != 1 {
		t.Fatalf("canceled claim published an event: %+v %v", events, err)
	}
	claimed := claim(t, s, "replacement-worker")
	if claimed.ID != job.ID || claimed.Attempt != 1 {
		t.Fatalf("claim after cancellation: %+v", claimed)
	}
	if err := s.Finish(ctx, claimed, json.RawMessage(`{"ok":true}`), "", false); err != nil {
		t.Fatal(err)
	}
}

func TestCanceledAttemptOperationsPreserveLeaseAndHistory(t *testing.T) {
	for _, operation := range []string{"heartbeat", "finish"} {
		t.Run(operation, func(t *testing.T) {
			s, p := testdb.New(t)
			submit(t, s, queue.Submit{})
			job, err := s.Claim(ctx, "worker", []string{"default"}, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			before := detail(t, s, job.ID)
			locked, pid := holdLock(t, p, `SELECT id FROM jobs WHERE id=$1 FOR UPDATE`, job.ID)
			pending, cancel := context.WithCancel(ctx)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if operation == "heartbeat" {
					done <- s.Heartbeat(pending, job, 2*time.Minute)
				} else {
					done <- s.Finish(pending, job, json.RawMessage(`{"canceled":true}`), "", false)
				}
			}()
			waitForBlockedQuery(t, p, pid)
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled %s: %v", operation, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("%s ignored cancellation", operation)
			}
			if err := locked.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if after := detail(t, s, job.ID); !reflect.DeepEqual(before, after) {
				t.Fatalf("canceled operation changed attempt: before=%+v after=%+v", before, after)
			}
			events, err := s.Events(ctx, 0)
			if err != nil || len(events) != 2 {
				t.Fatalf("canceled operation published an event: %+v %v", events, err)
			}
			if err := s.Finish(ctx, job, json.RawMessage(`{"ok":true}`), "", false); err != nil {
				t.Fatalf("original lease no longer usable: %v", err)
			}
		})
	}
}
