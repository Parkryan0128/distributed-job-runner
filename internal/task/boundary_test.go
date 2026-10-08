package task_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

func TestDemoPayloadBoundaries(t *testing.T) {
	for _, payload := range []string{`{}`, `{"work_ms":0,"fail_until":0}`, `{"work_ms":120000,"fail_until":10}`, " \n {\"work_ms\":1} \t "} {
		t.Run(payload, func(t *testing.T) {
			if err := task.Validate("demo", json.RawMessage(payload)); err != nil {
				t.Fatalf("valid payload: %v", err)
			}
		})
	}
	for _, payload := range []string{`{"work_ms":-1}`, `{"fail_until":11}`, `{"work_ms":1.5}`, `{"work_ms":"1"}`, `{"work_ms":true}`, `{"fail_until":[]}`, `{"work_ms":9223372036854775808}`, `{} {}`, `true`, `"demo"`, ``, `{`} {
		kind := "demo"
		t.Run(kind+payload, func(t *testing.T) {
			if err := task.Validate(kind, json.RawMessage(payload)); err == nil {
				t.Fatal("invalid payload accepted")
			}
			result, err := task.Run(context.Background(), queue.Job{Kind: kind, Payload: json.RawMessage(payload), Attempt: 1})
			var permanent task.PermanentError
			if result != nil || !errors.As(err, &permanent) {
				t.Fatalf("invalid task result=%s error=%v", result, err)
			}
		})
	}
}

func TestDemoResultAndFailureThreshold(t *testing.T) {
	for _, failUntil := range []int{0, 1, 10} {
		t.Run(fmt.Sprint(failUntil), func(t *testing.T) {
			payload := json.RawMessage(fmt.Sprintf(`{"work_ms":0,"fail_until":%d}`, failUntil))
			if failUntil > 0 {
				result, err := task.Run(context.Background(), queue.Job{Kind: "demo", Payload: payload, Attempt: failUntil})
				var permanent task.PermanentError
				if result != nil || err == nil || errors.As(err, &permanent) || err.Error() != fmt.Sprintf("simulated failure on attempt %d", failUntil) {
					t.Fatalf("threshold failure: %s %v", result, err)
				}
			}
			result, err := task.Run(context.Background(), queue.Job{Kind: "demo", Payload: payload, Attempt: failUntil + 1})
			if err != nil {
				t.Fatal(err)
			}
			var got struct {
				Completed bool `json:"completed"`
				Attempt   int  `json:"attempt"`
				WorkMS    int  `json:"work_ms"`
			}
			if err := json.Unmarshal(result, &got); err != nil || !got.Completed || got.Attempt != failUntil+1 || got.WorkMS != 0 {
				t.Fatalf("success payload: %s (%v)", result, err)
			}
		})
	}
}

type observedContext struct {
	context.Context
	waiting chan struct{}
}

func (c observedContext) Done() <-chan struct{} {
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	return c.Context.Done()
}

func TestCancellationDuringTaskWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waiting := make(chan struct{}, 1)
	done := make(chan error, 1)
	go func() {
		result, err := task.Run(observedContext{ctx, waiting}, queue.Job{Kind: "demo", Payload: json.RawMessage(`{"work_ms":120000}`), Attempt: 1})
		if result != nil {
			done <- fmt.Errorf("canceled task returned %s", result)
			return
		}
		done <- err
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("task did not start waiting")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("task did not stop after cancellation")
	}
}

func TestExpiredDeadlineAndPermanentErrorUnwrap(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	result, err := task.Run(ctx, queue.Job{Kind: "demo", Payload: json.RawMessage(`{}`), Attempt: 1})
	if result != nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired context: %s %v", result, err)
	}
	cause := errors.New("invalid data")
	wrapped := fmt.Errorf("task: %w", task.PermanentError{Err: cause})
	var permanent task.PermanentError
	if !errors.Is(wrapped, cause) || !errors.As(wrapped, &permanent) || permanent.Error() != cause.Error() {
		t.Fatalf("permanent error chain: %v", wrapped)
	}
}
