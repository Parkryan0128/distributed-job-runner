package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

func Run(ctx context.Context, j queue.Job) (json.RawMessage, error) {
	p, err := parse(j.Kind, j.Payload)
	if err != nil {
		return nil, PermanentError{err}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	timer := time.NewTimer(time.Duration(p.WorkMS) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-timer.C:
	}
	if j.Attempt <= p.FailUntil {
		return nil, fmt.Errorf("simulated failure on attempt %d", j.Attempt)
	}
	return json.Marshal(map[string]any{"completed": true, "attempt": j.Attempt, "work_ms": p.WorkMS})
}
