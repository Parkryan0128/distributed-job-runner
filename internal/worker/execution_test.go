package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

func TestWorkerRejectsInvalidConfigurationBeforeStarting(t *testing.T) {
	cases := []struct {
		name   string
		change func(*Worker)
		want   string
	}{
		{"missing store", func(w *Worker) { w.Store = nil }, "invalid worker configuration"},
		{"missing id", func(w *Worker) { w.ID = "" }, "invalid worker configuration"},
		{"missing queues", func(w *Worker) { w.Queues = nil }, "invalid worker configuration"},
		{"zero concurrency", func(w *Worker) { w.Concurrency = 0 }, "invalid worker configuration"},
		{"excess concurrency", func(w *Worker) { w.Concurrency = 33 }, "invalid worker configuration"},
		{"short lease", func(w *Worker) { w.Lease = 300*time.Millisecond - time.Nanosecond }, "invalid worker configuration"},
		{"short poll", func(w *Worker) { w.Poll = 10*time.Millisecond - time.Nanosecond }, "invalid worker configuration"},
		{"empty queue", func(w *Worker) { w.Queues = []string{"default", ""} }, "invalid worker queue"},
		{"invalid queue", func(w *Worker) { w.Queues = []string{"Bad Queue"} }, "invalid worker queue"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			w := Worker{Store: &queue.Store{}, ID: "worker", Queues: []string{"default"}, Concurrency: 1, Lease: time.Second, Poll: time.Second}
			c.change(&w)
			if err := w.Run(context.Background()); err == nil || err.Error() != c.want {
				t.Fatalf("configuration error = %v, want %q", err, c.want)
			}
			if w.Handler != nil || w.Logger != nil {
				t.Fatal("invalid configuration initialized the worker")
			}
		})
	}
}

func TestCanceledWorkerAcceptsConfigurationBoundaries(t *testing.T) {
	for _, concurrency := range []int{1, 32} {
		t.Run(fmt.Sprint(concurrency), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			w := Worker{Store: &queue.Store{}, ID: "worker", Queues: []string{"default"}, Concurrency: concurrency, Lease: 300 * time.Millisecond, Poll: 10 * time.Millisecond}
			if err := w.Run(ctx); err != nil {
				t.Fatal(err)
			}
			if w.Handler == nil || w.Logger == nil {
				t.Fatal("worker defaults were not initialized")
			}
		})
	}
}

func TestInvokePreservesArgumentsResultAndError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	job := queue.Job{ID: "job", Attempt: 3, Payload: json.RawMessage(`{"work_ms":0}`)}
	payload := json.RawMessage(`{"value":42}`)
	failure := errors.New("handler error")
	for _, wantErr := range []error{nil, failure, fmt.Errorf("wrapped: %w", task.PermanentError{Err: failure})} {
		result, err := invoke(ctx, func(gotCtx context.Context, gotJob queue.Job) (json.RawMessage, error) {
			if gotCtx != ctx || !reflect.DeepEqual(gotJob, job) {
				t.Fatal("handler received different arguments")
			}
			return payload, wantErr
		}, job)
		if string(result) != string(payload) || err != wantErr {
			t.Fatalf("result=%s error=%v, want %s %v", result, err, payload, wantErr)
		}
	}
}

func TestInvokeConvertsPanicsToErrors(t *testing.T) {
	for _, value := range []any{"broken handler", errors.New("broken dependency"), 7, nil} {
		t.Run(fmt.Sprint(value), func(t *testing.T) {
			result, err := invoke(context.Background(), func(context.Context, queue.Job) (json.RawMessage, error) {
				panic(value)
			}, queue.Job{})
			if result != nil || err == nil {
				t.Fatalf("panic produced result=%s error=%v", result, err)
			}
			if value != nil && err.Error() != fmt.Sprintf("handler panic: %v", value) {
				t.Fatalf("unexpected panic error: %v", err)
			}
		})
	}
}

func TestPauseHandlesCompletionAndCancellation(t *testing.T) {
	if !pause(context.Background(), 0) {
		t.Fatal("completed timer reported cancellation")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if pause(ctx, time.Hour) {
		t.Fatal("canceled wait reported completion")
	}
}
