package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ctx = context.Background()

func submit(t *testing.T, s *queue.Store, input queue.Submit) queue.Job {
	t.Helper()
	if input.Kind == "" {
		input.Kind = "demo"
	}
	if input.Payload == nil {
		input.Payload = json.RawMessage(`{}`)
	}
	j, created, err := s.Submit(ctx, input, "")
	if err != nil || !created {
		t.Fatalf("submit: created=%v err=%v", created, err)
	}
	return j
}
func claim(t *testing.T, s *queue.Store, worker string) queue.Job {
	t.Helper()
	j, err := s.Claim(ctx, worker, []string{"default"}, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func detail(t *testing.T, s *queue.Store, id string) queue.Detail {
	t.Helper()
	j, err := s.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return j
}
func execute(t *testing.T, p *pgxpool.Pool, sql string, args ...any) {
	t.Helper()
	if _, err := p.Exec(ctx, sql, args...); err != nil {
		t.Fatal(err)
	}
}
func ready(t *testing.T, p *pgxpool.Pool, id string) {
	t.Helper()
	execute(t, p, `UPDATE jobs SET available_at=clock_timestamp()-interval '1 second' WHERE id=$1`, id)
}

func TestConcurrentSubmissionsShareOneIdempotencyKey(t *testing.T) {
	s, _ := testdb.New(t)
	var wg sync.WaitGroup
	ids := make(chan string, 20)
	var created atomic.Int32
	for range 20 {
		wg.Go(func() {
			j, fresh, err := s.Submit(ctx, queue.Submit{Kind: "demo", Payload: json.RawMessage(`{"nested":{"b":2,"a":1}}`)}, "request-1")
			if err != nil {
				t.Error(err)
				return
			}
			if fresh {
				created.Add(1)
			}
			ids <- j.ID
		})
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("duplicate jobs")
		}
	}
	if created.Load() != 1 {
		t.Fatalf("created %d jobs", created.Load())
	}
	j, fresh, err := s.Submit(ctx, queue.Submit{Kind: "demo", Queue: "default", MaxAttempts: 3, TimeoutSeconds: 30, Payload: json.RawMessage(`{ "nested": {"a": 1, "b": 2} }`)}, "request-1")
	if err != nil || fresh || j.ID != first {
		t.Fatalf("equivalent request: %v %v", fresh, err)
	}
	_, _, err = s.Submit(ctx, queue.Submit{Kind: "demo", Payload: json.RawMessage(`{"nested":{"a":9}}`)}, "request-1")
	if !errors.Is(err, queue.ErrIdempotency) {
		t.Fatalf("expected conflict: %v", err)
	}
}

func TestConcurrentWorkersClaimEachJobOnce(t *testing.T) {
	s, _ := testdb.New(t)
	for range 40 {
		submit(t, s, queue.Submit{})
	}
	var wg sync.WaitGroup
	claimed := make(chan string, 40)
	for n := range 8 {
		wg.Go(func() {
			for {
				j, err := s.Claim(ctx, fmt.Sprint("worker-", n), []string{"default"}, time.Minute)
				if errors.Is(err, queue.ErrNotFound) {
					return
				}
				if err != nil {
					t.Error(err)
					return
				}
				claimed <- j.ID
			}
		})
	}
	wg.Wait()
	close(claimed)
	seen := map[string]bool{}
	for id := range claimed {
		if seen[id] {
			t.Fatal("job claimed twice")
		}
		seen[id] = true
		j := detail(t, s, id)
		if len(j.Attempts) != 1 || j.Attempt != 1 {
			t.Fatal("claim and attempt were not committed together")
		}
	}
	if len(seen) != 40 {
		t.Fatalf("claimed %d jobs", len(seen))
	}
}

func TestClaimHonorsQueuePriorityAndSchedule(t *testing.T) {
	s, p := testdb.New(t)
	low := submit(t, s, queue.Submit{Priority: 1})
	high := submit(t, s, queue.Submit{Priority: 9})
	delayed := submit(t, s, queue.Submit{Priority: 9, DelaySeconds: 3600})
	other := submit(t, s, queue.Submit{Queue: "reports", Priority: 9})
	if j := claim(t, s, "w"); j.ID != high.ID {
		t.Fatal("highest ready priority was not selected")
	}
	if j := claim(t, s, "w"); j.ID != low.ID {
		t.Fatal("scheduled or foreign queue job was selected")
	}
	if _, err := s.Claim(ctx, "w", []string{"default"}, time.Second); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("expected empty queue: %v", err)
	}
	if j, err := s.Claim(ctx, "w", []string{"reports"}, time.Second); err != nil || j.ID != other.ID {
		t.Fatalf("other queue: %v", err)
	}
	ready(t, p, delayed.ID)
	if j := claim(t, s, "w"); j.ID != delayed.ID {
		t.Fatal("scheduled job was not claimed after becoming due")
	}
}

func TestSuccessfulCompletionPersistsResultAndHistory(t *testing.T) {
	s, _ := testdb.New(t)
	submit(t, s, queue.Submit{})
	j := claim(t, s, "worker-a")
	if err := s.Finish(ctx, j, json.RawMessage(`{"answer":42}`), "", false); err != nil {
		t.Fatal(err)
	}
	got := detail(t, s, j.ID)
	if got.Status != "succeeded" || got.WorkerID != nil || got.LeaseUntil != nil || len(got.Attempts) != 1 || got.Attempts[0].Status != "succeeded" || got.Attempts[0].FinishedAt == nil {
		t.Fatalf("bad completion: %+v", got)
	}
	if !got.AvailableAt.Equal(j.AvailableAt) {
		t.Fatal("completed job was given another scheduled execution time")
	}
	var result map[string]int
	if err := json.Unmarshal(got.Result, &result); err != nil || result["answer"] != 42 {
		t.Fatalf("result: %s", got.Result)
	}
	if err := s.Finish(ctx, j, json.RawMessage(`{}`), "", false); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatalf("duplicate completion: %v", err)
	}
	if err := s.Cancel(ctx, j.ID); !errors.Is(err, queue.ErrConflict) {
		t.Fatalf("cancel completed: %v", err)
	}
}

func TestFailuresBackOffThenReachDeadLetterState(t *testing.T) {
	s, p := testdb.New(t)
	created := submit(t, s, queue.Submit{MaxAttempts: 3})
	for n := 1; n <= 3; n++ {
		j := claim(t, s, "worker")
		if j.Attempt != n {
			t.Fatal("wrong attempt")
		}
		if err := s.Finish(ctx, j, nil, "temporary failure", false); err != nil {
			t.Fatal(err)
		}
		got := detail(t, s, j.ID)
		if n < 3 {
			if got.Status != "queued" || time.Until(got.AvailableAt) < queue.RetryDelay(n)-500*time.Millisecond {
				t.Fatalf("missing backoff: %+v", got)
			}
			if _, err := s.Claim(ctx, "w", []string{"default"}, time.Second); !errors.Is(err, queue.ErrNotFound) {
				t.Fatal("retried before delay")
			}
			ready(t, p, j.ID)
		} else if got.Status != "dead" {
			t.Fatal("retry limit not enforced")
		}
	}
	got := detail(t, s, created.ID)
	if len(got.Attempts) != 3 || got.Error != "temporary failure" {
		t.Fatalf("history: %+v", got)
	}
	if _, err := s.Claim(ctx, "w", []string{"default"}, time.Second); !errors.Is(err, queue.ErrNotFound) {
		t.Fatal("dead job was claimed")
	}
}

func TestPermanentFailureDoesNotRetry(t *testing.T) {
	s, _ := testdb.New(t)
	submit(t, s, queue.Submit{})
	j := claim(t, s, "w")
	if err := s.Finish(ctx, j, nil, "unsupported task", true); err != nil {
		t.Fatal(err)
	}
	if got := detail(t, s, j.ID); got.Status != "dead" || got.Attempt != 1 {
		t.Fatalf("permanent failure: %+v", got)
	}
}

func TestExpiredLeaseIsRecoveredAndOldAttemptIsFenced(t *testing.T) {
	s, p := testdb.New(t)
	submit(t, s, queue.Submit{})
	old := claim(t, s, "same-worker")
	execute(t, p, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, old.ID)
	if err := s.Heartbeat(ctx, old, time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatal("expired lease was resurrected")
	}
	if err := s.Finish(ctx, old, json.RawMessage(`{}`), "", false); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatal("expired completion accepted")
	}
	if n, err := s.Recover(ctx); err != nil || n != 1 {
		t.Fatalf("recover: %d %v", n, err)
	}
	ready(t, p, old.ID)
	current := claim(t, s, "same-worker")
	if current.Attempt != 2 {
		t.Fatal("attempt token was reused")
	}
	if err := s.Finish(ctx, old, json.RawMessage(`{"stale":true}`), "", false); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatal("old attempt overwrote current one")
	}
	if err := s.Heartbeat(ctx, old, time.Minute); !errors.Is(err, queue.ErrLeaseLost) {
		t.Fatal("old attempt extended new lease")
	}
	if err := s.Finish(ctx, current, json.RawMessage(`{"ok":true}`), "", false); err != nil {
		t.Fatal(err)
	}
	got := detail(t, s, old.ID)
	if got.Attempts[0].Status != "expired" || got.Attempts[1].Status != "succeeded" {
		t.Fatalf("history: %+v", got.Attempts)
	}
}

func TestHeartbeatKeepsActiveJobOutOfRecovery(t *testing.T) {
	s, _ := testdb.New(t)
	submit(t, s, queue.Submit{})
	j := claim(t, s, "w")
	before := *j.LeaseUntil
	if err := s.Heartbeat(ctx, j, time.Minute); err != nil {
		t.Fatal(err)
	}
	if got := detail(t, s, j.ID); !got.LeaseUntil.After(before) {
		t.Fatal("lease was not extended")
	}
	if n, err := s.Recover(ctx); err != nil || n != 0 {
		t.Fatalf("active job recovered: %d %v", n, err)
	}
}

func TestExpiredLastAttemptBecomesDead(t *testing.T) {
	s, p := testdb.New(t)
	submit(t, s, queue.Submit{MaxAttempts: 1})
	j := claim(t, s, "w")
	execute(t, p, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, j.ID)
	if n, err := s.Recover(ctx); err != nil || n != 1 {
		t.Fatalf("recover: %d %v", n, err)
	}
	if got := detail(t, s, j.ID); got.Status != "dead" || got.Attempts[0].Status != "expired" {
		t.Fatalf("last attempt: %+v", got)
	}
}

func TestCancelQueuedAndRunningJobs(t *testing.T) {
	for _, running := range []bool{false, true} {
		t.Run(fmt.Sprint(running), func(t *testing.T) {
			s, _ := testdb.New(t)
			j := submit(t, s, queue.Submit{})
			if running {
				j = claim(t, s, "w")
			}
			for range 2 {
				if err := s.Cancel(ctx, j.ID); err != nil {
					t.Fatal(err)
				}
			}
			got := detail(t, s, j.ID)
			if got.Status != "canceled" || got.WorkerID != nil {
				t.Fatalf("canceled: %+v", got)
			}
			if running {
				if got.Attempts[0].Status != "canceled" {
					t.Fatal("attempt not canceled")
				}
				if err := s.Finish(ctx, j, json.RawMessage(`{}`), "", false); !errors.Is(err, queue.ErrLeaseLost) {
					t.Fatal("canceled attempt completed")
				}
			}
			if _, err := s.Claim(ctx, "w", []string{"default"}, time.Second); !errors.Is(err, queue.ErrNotFound) {
				t.Fatal("canceled job claimed")
			}
		})
	}
}

func TestCancelAndFinishHaveOneConsistentWinner(t *testing.T) {
	s, _ := testdb.New(t)
	for range 10 {
		submit(t, s, queue.Submit{})
		j := claim(t, s, "w")
		var cancelErr, finishErr error
		var wg sync.WaitGroup
		wg.Go(func() { cancelErr = s.Cancel(ctx, j.ID) })
		wg.Go(func() { finishErr = s.Finish(ctx, j, json.RawMessage(`{}`), "", false) })
		wg.Wait()
		got := detail(t, s, j.ID)
		if got.Status == "canceled" {
			if cancelErr != nil || !errors.Is(finishErr, queue.ErrLeaseLost) || got.Attempts[0].Status != "canceled" {
				t.Fatalf("cancel won: %v %v %+v", cancelErr, finishErr, got)
			}
		} else if got.Status == "succeeded" {
			if finishErr != nil || !errors.Is(cancelErr, queue.ErrConflict) || got.Attempts[0].Status != "succeeded" {
				t.Fatalf("finish won: %v %v %+v", cancelErr, finishErr, got)
			}
		} else {
			t.Fatalf("unexpected state: %s", got.Status)
		}
	}
}

func TestClaimRollsBackIfAttemptInsertFails(t *testing.T) {
	s, p := testdb.New(t)
	created := submit(t, s, queue.Submit{})
	execute(t, p, `ALTER TABLE attempts ADD CONSTRAINT reject_worker CHECK(worker_id <> 'broken')`)
	if _, err := s.Claim(ctx, "broken", []string{"default"}, time.Minute); err == nil {
		t.Fatal("expected insert failure")
	}
	got := detail(t, s, created.ID)
	if got.Status != "queued" || got.Attempt != 0 || len(got.Attempts) != 0 {
		t.Fatalf("partial claim persisted: %+v", got)
	}
	if j := claim(t, s, "healthy"); j.Attempt != 1 {
		t.Fatal("failed transaction consumed attempt")
	}
}

func TestListUsesStableCursorAndFilters(t *testing.T) {
	s, _ := testdb.New(t)
	for range 5 {
		submit(t, s, queue.Submit{})
	}
	submit(t, s, queue.Submit{Queue: "reports"})
	first, err := s.List(ctx, queue.Filter{Queue: "default", Status: "queued", Limit: 3})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Jobs) != 3 || first.NextCursor == 0 {
		t.Fatalf("first page: %+v", first)
	}
	fresh := submit(t, s, queue.Submit{})
	second, err := s.List(ctx, queue.Filter{Queue: "default", Status: "queued", Limit: 3, Before: first.NextCursor})
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Jobs) != 2 || second.NextCursor != 0 {
		t.Fatalf("second page: %+v", second)
	}
	seen := map[string]bool{fresh.ID: true}
	for _, j := range append(first.Jobs, second.Jobs...) {
		if seen[j.ID] || j.Queue != "default" {
			t.Fatal("unstable pagination")
		}
		seen[j.ID] = true
	}
	empty, err := s.List(ctx, queue.Filter{Status: "dead", Limit: 10})
	if err != nil || len(empty.Jobs) != 0 {
		t.Fatalf("empty: %+v %v", empty, err)
	}
	st, err := s.Stats(ctx)
	if err != nil || st.Queued != 7 || st.Running != 0 {
		t.Fatalf("stats: %+v %v", st, err)
	}
	if _, err := s.Get(ctx, queue.NewID()); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}

func TestLeaseExpiryWhileWaitingForRowLock(t *testing.T) {
	for _, operation := range []string{"heartbeat", "finish"} {
		t.Run(operation, func(t *testing.T) {
			s, p := testdb.New(t)
			submit(t, s, queue.Submit{})
			j, err := s.Claim(ctx, "worker", []string{"default"}, 2*time.Second)
			if err != nil {
				t.Fatal(err)
			}
			locked, err := p.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer locked.Rollback(ctx)
			var pid int
			if err := locked.QueryRow(ctx, `SELECT pg_backend_pid() FROM jobs WHERE id=$1 FOR UPDATE`, j.ID).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			waiting, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if operation == "heartbeat" {
					result <- s.Heartbeat(waiting, j, time.Minute)
				} else {
					result <- s.Finish(waiting, j, json.RawMessage(`{}`), "", false)
				}
			}()
			blocked := false
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if !blocked {
				t.Fatal("operation did not wait on the row lock")
			}
			execute(t, p, `SELECT pg_sleep(GREATEST(0, EXTRACT(EPOCH FROM lease_until-clock_timestamp()))+0.05) FROM jobs WHERE id=$1`, j.ID)
			if err := locked.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			if err := <-result; !errors.Is(err, queue.ErrLeaseLost) {
				t.Fatalf("accepted expired lease after lock wait: %v", err)
			}
			got := detail(t, s, j.ID)
			if got.Status != "running" || got.Result != nil || !got.LeaseUntil.Equal(*j.LeaseUntil) {
				t.Fatalf("expired attempt was changed: %+v", got)
			}
		})
	}
}

func TestConcurrentRecoveryProcessesEachExpiredAttemptOnce(t *testing.T) {
	s, p := testdb.New(t)
	ids := make([]string, 0, 105)
	for range 105 {
		j := submit(t, s, queue.Submit{})
		claim(t, s, "crashed")
		ids = append(ids, j.ID)
	}
	execute(t, p, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second'`)
	var recovered atomic.Int32
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			n, err := s.Recover(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			if n > 100 {
				t.Errorf("recovery exceeded batch limit: %d", n)
			}
			recovered.Add(int32(n))
		})
	}
	wg.Wait()
	if recovered.Load() != 105 {
		t.Fatalf("recovered %d attempts, expected 105", recovered.Load())
	}
	for _, id := range ids {
		j := detail(t, s, id)
		if j.Status != "queued" || j.Attempt != 1 || len(j.Attempts) != 1 || j.Attempts[0].Status != "expired" {
			t.Fatalf("recovery duplicated or lost history: %+v", j)
		}
	}
	if n, err := s.Recover(ctx); err != nil || n != 0 {
		t.Fatalf("recovered attempts twice: %d %v", n, err)
	}
}

func TestStateChangesRollBackWhenAttemptUpdateFails(t *testing.T) {
	for _, operation := range []string{"finish", "recover", "cancel"} {
		t.Run(operation, func(t *testing.T) {
			s, p := testdb.New(t)
			submit(t, s, queue.Submit{})
			j := claim(t, s, "worker")
			if operation == "recover" {
				execute(t, p, `UPDATE jobs SET lease_until=clock_timestamp()-interval '1 second' WHERE id=$1`, j.ID)
			}
			execute(t, p, `ALTER TABLE attempts ADD CONSTRAINT reject_terminal CHECK(status='running')`)
			apply := func() error {
				switch operation {
				case "finish":
					return s.Finish(ctx, j, json.RawMessage(`{"saved":true}`), "", false)
				case "cancel":
					return s.Cancel(ctx, j.ID)
				default:
					_, err := s.Recover(ctx)
					return err
				}
			}
			if err := apply(); err == nil {
				t.Fatal("expected attempt update failure")
			}
			got := detail(t, s, j.ID)
			if got.Status != "running" || got.WorkerID == nil || got.Result != nil || got.Attempts[0].Status != "running" || got.Attempts[0].FinishedAt != nil {
				t.Fatalf("partial state transition persisted: %+v", got)
			}
			execute(t, p, `ALTER TABLE attempts DROP CONSTRAINT reject_terminal`)
			if err := apply(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestWorkerPresenceAndRunningJobs(t *testing.T) {
	s, p := testdb.New(t)
	if err := s.AnnounceWorker(ctx, "worker-a", 2); err != nil {
		t.Fatal(err)
	}
	submit(t, s, queue.Submit{})
	j := claim(t, s, "worker-a")
	workers, err := s.Workers(ctx)
	if err != nil || len(workers) != 1 || !workers[0].Online || workers[0].Concurrency != 2 || len(workers[0].Jobs) != 1 || workers[0].Jobs[0].ID != j.ID {
		t.Fatalf("snapshot: %+v %v", workers, err)
	}
	execute(t, p, `UPDATE workers SET seen_at=clock_timestamp()-interval '6 seconds'`)
	workers, err = s.Workers(ctx)
	if err != nil || len(workers) != 1 || workers[0].Online || len(workers[0].Jobs) != 1 {
		t.Fatalf("offline worker must retain leased job: %+v %v", workers, err)
	}
	if err := s.Cancel(ctx, j.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.AnnounceWorker(ctx, "worker-a", 3); err != nil {
		t.Fatal(err)
	}
	workers, err = s.Workers(ctx)
	if err != nil || !workers[0].Online || workers[0].Concurrency != 3 || len(workers[0].Jobs) != 0 {
		t.Fatalf("returning worker: %+v %v", workers, err)
	}
	execute(t, p, `UPDATE workers SET seen_at=clock_timestamp()-interval '6 minutes'`)
	workers, err = s.Workers(ctx)
	if err != nil || len(workers) != 0 {
		t.Fatalf("stale workers: %+v %v", workers, err)
	}
}

func TestPendingShowsClaimOrderAndScheduledJobs(t *testing.T) {
	s, _ := testdb.New(t)
	delayed := submit(t, s, queue.Submit{Priority: 9, DelaySeconds: 60})
	ordinary := submit(t, s, queue.Submit{})
	urgent := submit(t, s, queue.Submit{Priority: 9})
	pending, err := s.Pending(ctx)
	if err != nil || len(pending) != 3 || pending[0].ID != urgent.ID || pending[1].ID != ordinary.ID || pending[2].ID != delayed.ID {
		t.Fatalf("pending order: %+v %v", pending, err)
	}
	claim(t, s, "worker-a")
	pending, err = s.Pending(ctx)
	if err != nil || len(pending) != 2 || pending[0].ID != ordinary.ID {
		t.Fatalf("claimed job still pending: %+v %v", pending, err)
	}
}
