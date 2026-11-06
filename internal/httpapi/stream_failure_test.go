package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

type streamWriter struct {
	*httptest.ResponseRecorder
	writes, flushes int
	failWrite       int
	failFlush       int
	failed          bool
	onFlush         func(int)
}

func (w *streamWriter) Write(data []byte) (int, error) {
	w.writes++
	if w.writes == w.failWrite {
		w.failed = true
		return 0, io.ErrClosedPipe
	}
	return w.ResponseRecorder.Write(data)
}

func (w *streamWriter) FlushError() error {
	w.flushes++
	if w.flushes == w.failFlush {
		w.failed = true
		return io.ErrClosedPipe
	}
	w.ResponseRecorder.Flush()
	if w.onFlush != nil {
		w.onFlush(w.flushes)
	}
	return nil
}

func TestStreamWriteAndFlushFailuresReleaseSubscriptions(t *testing.T) {
	for _, tc := range []struct {
		name             string
		writeAt, flushAt int
	}{
		{"snapshot write", 1, 0},
		{"snapshot flush", 0, 1},
		{"demo write", 2, 0},
		{"demo flush", 0, 2},
		{"event id write", 3, 0},
		{"event data write", 4, 0},
		{"event flush", 0, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _ := testdb.New(t)
			d := NewDemo(s)
			w := &streamWriter{ResponseRecorder: httptest.NewRecorder(), failWrite: tc.writeAt, failFlush: tc.flushAt}
			w.onFlush = func(count int) {
				if count == 1 {
					d.broadcast(message{kind: "job", id: 1, data: map[string]string{"status": "queued"}})
					d.broadcast(message{kind: "unavailable"})
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			d.stream(w, httptest.NewRequest("GET", "/api/events", nil).WithContext(ctx))
			if !w.failed || len(d.subscribers) != 0 || ctx.Err() != nil {
				t.Fatalf("failure cleanup: reached=%v subscribers=%d context=%v", w.failed, len(d.subscribers), ctx.Err())
			}
			d.broadcast(message{kind: "job", id: 2})
		})
	}
}

func TestStreamEncodingFailureReleasesSubscription(t *testing.T) {
	s, _ := testdb.New(t)
	d := NewDemo(s)
	w := &streamWriter{ResponseRecorder: httptest.NewRecorder()}
	w.onFlush = func(count int) {
		if count == 1 {
			d.broadcast(message{kind: "job", id: 1, data: make(chan int)})
			d.broadcast(message{kind: "unavailable"})
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	d.stream(w, httptest.NewRequest("GET", "/api/events", nil).WithContext(ctx))
	if len(d.subscribers) != 0 || w.flushes != 2 || strings.Contains(w.Body.String(), "event: job") || ctx.Err() != nil {
		t.Fatalf("encoding failure cleanup: subscribers=%d flushes=%d body=%s context=%v", len(d.subscribers), w.flushes, w.Body.String(), ctx.Err())
	}
}

func TestStreamSkipsEventsAlreadyIncludedInSnapshot(t *testing.T) {
	s, _ := testdb.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 2 {
		if _, _, err := s.Submit(ctx, queue.Submit{Kind: "demo", Payload: json.RawMessage(`{}`)}, ""); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := s.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	d := NewDemo(s)
	w := &streamWriter{ResponseRecorder: httptest.NewRecorder()}
	w.onFlush = func(count int) {
		if count == 1 {
			for _, id := range []int64{snapshot.Cursor - 1, snapshot.Cursor, snapshot.Cursor + 1} {
				d.broadcast(message{kind: "job", id: id, data: map[string]int64{"cursor": id}})
			}
			d.broadcast(message{kind: "unavailable"})
		}
	}
	d.stream(w, httptest.NewRequest("GET", "/api/events", nil).WithContext(ctx))
	body := w.Body.String()
	if strings.Count(body, "event: job\n") != 1 || strings.Count(body, "event: snapshot\n") != 1 || w.flushes != 3 {
		t.Fatalf("snapshot overlap was replayed: %s", body)
	}
	for _, block := range strings.Split(body, "\n\n") {
		if strings.Contains(block, "event: job\n") {
			var event struct {
				Cursor int64 `json:"cursor"`
			}
			_, data, found := strings.Cut(block, "data: ")
			if !found || json.Unmarshal([]byte(data), &event) != nil || event.Cursor != snapshot.Cursor+1 {
				t.Fatalf("wrong event after snapshot: %s", block)
			}
		}
	}
	if len(d.subscribers) != 0 || ctx.Err() != nil {
		t.Fatalf("stream cleanup: subscribers=%d context=%v", len(d.subscribers), ctx.Err())
	}
}
