package httpapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestSlowGeneratorDoesNotBlockControlOrSubmitForAnOldTurn(t *testing.T) {
	for _, tc := range []struct{ name, lock string }{
		{"stats", `LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE`},
		{"submit", `SELECT pg_advisory_xact_lock(7241074)`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, p := testdb.New(t)
			ctx := context.Background()
			d := NewDemo(s)
			d.owner, d.expires, d.running = queue.NewID(), time.Now().Add(time.Minute), true
			h, err := (&Server{Store: s, Demo: d}).Handler(nil)
			if err != nil {
				t.Fatal(err)
			}
			control := func(owner, action string) {
				t.Helper()
				r := httptest.NewRequest("POST", "/api/demo", strings.NewReader(`{"action":"`+action+`"}`))
				r.AddCookie(&http.Cookie{Name: "demo_session", Value: owner})
				w := httptest.NewRecorder()
				start := time.Now()
				h.ServeHTTP(w, r)
				if w.Code != 200 || time.Since(start) > 500*time.Millisecond {
					t.Fatalf("control blocked by generator: status=%d elapsed=%s", w.Code, time.Since(start))
				}
			}
			tx, err := p.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err := tx.Exec(ctx, tc.lock); err != nil {
				t.Fatal(err)
			}
			var pid int
			if err := tx.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			owner := d.owner
			done := make(chan struct{})
			go func() { d.generate(ctx, time.Now()); close(done) }()
			deadline := time.Now().Add(time.Second)
			for {
				var blocked bool
				if err := p.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)))`, pid).Scan(&blocked); err != nil {
					t.Fatal(err)
				}
				if blocked {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("generator did not wait for the database")
				}
				time.Sleep(10 * time.Millisecond)
			}
			control(owner, "stop")
			control(queue.NewID(), "start")
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("stopping did not cancel the blocked database operation")
			}
			if err := tx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			stats, err := s.Stats(ctx)
			if err != nil || stats.Queued != 0 || d.sent != 0 {
				t.Fatalf("old turn submitted work: stats=%+v sent=%d err=%v", stats, d.sent, err)
			}
			d.generate(ctx, time.Now())
			stats, err = s.Stats(ctx)
			if err != nil || stats.Queued != 1 || d.sent != 1 {
				t.Fatalf("new turn did not generate work: stats=%+v sent=%d err=%v", stats, d.sent, err)
			}
		})
	}
}

func TestGeneratorRatesDoNotDriftWithTickerRounding(t *testing.T) {
	for _, tc := range []struct {
		level string
		want  int
	}{{"low", 3}, {"medium", 12}, {"high", 30}} {
		t.Run(tc.level, func(t *testing.T) {
			s, _ := testdb.New(t)
			d := NewDemo(s)
			start := time.Now()
			d.owner, d.expires, d.running, d.level, d.next = queue.NewID(), start.Add(time.Minute), true, tc.level, start
			// Exercise actual submissions with the same 100 ms ticks as Run.
			for elapsed := time.Duration(0); elapsed < 10*time.Second; elapsed += 100 * time.Millisecond {
				d.generate(context.Background(), start.Add(elapsed))
			}
			stats, err := s.Stats(context.Background())
			if err != nil || stats.Queued != tc.want || d.sent != tc.want {
				t.Fatalf("10-second arrivals: stats=%+v sent=%d want=%d err=%v", stats, d.sent, tc.want, err)
			}
			// A long pause permits one arrival, not a backlog of missed ticks.
			d.generate(context.Background(), start.Add(30*time.Second))
			d.generate(context.Background(), start.Add(30*time.Second+100*time.Millisecond))
			if d.sent != tc.want+1 {
				t.Fatalf("catch-up burst after stall: %d", d.sent-tc.want)
			}
		})
	}
}
