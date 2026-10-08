package queue_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestIdempotencyReplayPreservesTerminalJobs(t *testing.T) {
	for _, terminal := range []string{"succeeded", "dead", "canceled"} {
		t.Run(terminal, func(t *testing.T) {
			s, p := testdb.New(t)
			input := queue.Submit{Kind: "demo", Payload: json.RawMessage(`{"work_ms":0}`)}
			created, fresh, err := s.Submit(ctx, input, "terminal-replay")
			if err != nil || !fresh {
				t.Fatalf("initial submission: %v %v", fresh, err)
			}
			j := claim(t, s, "worker")
			switch terminal {
			case "succeeded":
				err = s.Finish(ctx, j, json.RawMessage(`{"completed":true}`), "", false)
			case "dead":
				err = s.Finish(ctx, j, nil, "permanent failure", true)
			case "canceled":
				err = s.Cancel(ctx, j.ID)
			}
			if err != nil {
				t.Fatal(err)
			}
			before := detail(t, s, created.ID)
			var eventsBefore int
			if err := p.QueryRow(ctx, `SELECT count(*) FROM job_events`).Scan(&eventsBefore); err != nil {
				t.Fatal(err)
			}
			for range 2 {
				replay, fresh, err := s.SubmitLimited(ctx, input, "terminal-replay", 0)
				if err != nil || fresh || replay.ID != created.ID || replay.Status != terminal {
					t.Fatalf("terminal replay: %+v fresh=%v err=%v", replay, fresh, err)
				}
			}
			input.Priority = 1
			if _, _, err := s.Submit(ctx, input, "terminal-replay"); !errors.Is(err, queue.ErrIdempotency) {
				t.Fatalf("changed replay: %v", err)
			}
			if after := detail(t, s, created.ID); !reflect.DeepEqual(before, after) {
				t.Fatalf("replay mutated job: before=%+v after=%+v", before, after)
			}
			var jobs, events int
			if err := p.QueryRow(ctx, `SELECT (SELECT count(*) FROM jobs), (SELECT count(*) FROM job_events)`).Scan(&jobs, &events); err != nil {
				t.Fatal(err)
			}
			if jobs != 1 || events != eventsBefore {
				t.Fatalf("replay created jobs or events: jobs=%d events=%d (was %d)", jobs, events, eventsBefore)
			}
		})
	}
}

func TestRetryDelayBoundaries(t *testing.T) {
	for _, c := range []struct {
		attempt int
		seconds int
	}{{-1, 1}, {0, 1}, {1, 1}, {2, 2}, {3, 4}, {4, 8}, {5, 16}, {6, 32}, {7, 64}, {8, 64}, {10, 64}, {1000, 64}} {
		t.Run(fmt.Sprint(c.attempt), func(t *testing.T) {
			if got := queue.RetryDelay(c.attempt); got != time.Duration(c.seconds)*time.Second {
				t.Fatalf("delay=%s, want %ds", got, c.seconds)
			}
		})
	}
}
