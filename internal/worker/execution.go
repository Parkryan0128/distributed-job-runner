package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

func (w *Worker) execute(parent context.Context, j queue.Job) {
	ctx, cancel := context.WithTimeout(parent, time.Duration(j.TimeoutSeconds)*time.Second)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(w.Lease / 3)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				op, stop := context.WithTimeout(ctx, w.Lease/3)
				err := w.Store.Heartbeat(op, j, w.Lease)
				stop()
				if err != nil {
					if ctx.Err() == nil {
						w.Logger.Warn("lost job lease", "job_id", j.ID, "error", err)
					}
					cancel()
					return
				}
			}
		}
	}()
	w.Logger.Info("started job", "job_id", j.ID, "attempt", j.Attempt, "worker_id", w.ID)
	result, err := invoke(ctx, w.Handler, j)
	if err == nil {
		err = ctx.Err()
	}
	cancel()
	<-done
	failure := ""
	permanent := false
	if err != nil {
		failure = err.Error()
		var pe task.PermanentError
		permanent = errors.As(err, &pe)
	}
	op, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if finishErr := w.Store.Finish(op, j, result, failure, permanent); finishErr != nil {
		if !errors.Is(finishErr, queue.ErrLeaseLost) {
			w.Logger.Error("persist job outcome", "job_id", j.ID, "error", finishErr)
		}
		return
	}
	w.Logger.Info("finished attempt", "job_id", j.ID, "attempt", j.Attempt, "error", failure)
}

func invoke(ctx context.Context, handler Handler, j queue.Job) (result json.RawMessage, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nil
			err = fmt.Errorf("handler panic: %v", recovered)
		}
	}()
	return handler(ctx, j)
}
