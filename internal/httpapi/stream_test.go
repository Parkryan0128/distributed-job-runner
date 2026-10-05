package httpapi

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestBroadcastDisconnectsSlowSubscribersWithoutDroppingFastOnes(t *testing.T) {
	d := NewDemo(nil)
	slow, fast := make(chan message, 1), make(chan message, 3)
	d.subscribers[slow] = struct{}{}
	d.subscribers[fast] = struct{}{}
	d.broadcast(message{kind: "job", id: 1})
	d.broadcast(message{kind: "job", id: 2})
	if len(d.subscribers) != 1 {
		t.Fatalf("subscribers=%d", len(d.subscribers))
	}
	if _, ok := d.subscribers[fast]; !ok {
		t.Fatal("fast subscriber removed")
	}
	if m := <-slow; m.id != 1 {
		t.Fatalf("buffered event lost: %+v", m)
	}
	if _, open := <-slow; open {
		t.Fatal("slow subscriber was not closed")
	}
	for _, id := range []int64{1, 2} {
		if m := <-fast; m.id != id {
			t.Fatalf("fast subscriber event: %+v", m)
		}
	}
}

func TestStreamRejectsViewersAtCapacity(t *testing.T) {
	d := NewDemo(nil)
	for range 200 {
		d.subscribers[make(chan message, 1)] = struct{}{}
	}
	w := httptest.NewRecorder()
	d.stream(w, httptest.NewRequest("GET", "/api/events", nil))
	if w.Code != 503 || len(d.subscribers) != 200 {
		t.Fatalf("stream limit: status=%d subscribers=%d", w.Code, len(d.subscribers))
	}
}

func TestStreamSnapshotFailureRemovesSubscription(t *testing.T) {
	s, p := testdb.New(t)
	p.Close()
	d := NewDemo(s)
	w := httptest.NewRecorder()
	d.stream(w, httptest.NewRequest("GET", "/api/events", nil))
	if w.Code != 503 || len(d.subscribers) != 0 {
		t.Fatalf("snapshot failure: status=%d subscribers=%d", w.Code, len(d.subscribers))
	}
}

func TestStreamDisconnectAndResetReleaseSubscriptions(t *testing.T) {
	for _, reason := range []string{"cancel", "reset", "unavailable"} {
		t.Run(reason, func(t *testing.T) {
			s, _ := testdb.New(t)
			d := NewDemo(s)
			done := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(done)
				d.stream(w, r)
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			r, err := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
			if err != nil {
				t.Fatal(err)
			}
			resp, err := server.Client().Do(r)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 || resp.Header.Get("Content-Type") != "text/event-stream" || resp.Header.Get("X-Accel-Buffering") != "no" {
				t.Fatalf("stream response: %d %v", resp.StatusCode, resp.Header)
			}
			if reason == "cancel" {
				cancel()
			} else {
				d.broadcast(message{kind: reason})
				body, err := io.ReadAll(resp.Body)
				if err != nil || !strings.Contains(string(body), "event: snapshot") || !strings.Contains(string(body), "event: demo") {
					t.Fatalf("stream body: %s %v", body, err)
				}
				if reason == "reset" && !strings.Contains(string(body), "event: reset\ndata: {}") {
					t.Fatalf("missing reset: %s", body)
				}
			}
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("stream did not stop")
			}
			d.mu.Lock()
			remaining := len(d.subscribers)
			d.mu.Unlock()
			if remaining != 0 {
				t.Fatalf("leaked subscriptions: %d", remaining)
			}
		})
	}
}
