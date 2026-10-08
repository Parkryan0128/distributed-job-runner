package worker

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

type Handler func(context.Context, queue.Job) (json.RawMessage, error)

type Worker struct {
	Store       *queue.Store
	ID          string
	Queues      []string
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
	Handler     Handler
	Logger      *slog.Logger
}

func (w *Worker) Run(ctx context.Context) error {
	if w.Store == nil || w.ID == "" || len(w.Queues) == 0 || w.Concurrency < 1 || w.Concurrency > 32 || w.Lease < 300*time.Millisecond || w.Poll < 10*time.Millisecond {
		return errors.New("invalid worker configuration")
	}
	for _, name := range w.Queues {
		if !queue.ValidQueue(name) {
			return errors.New("invalid worker queue")
		}
	}
	if w.Handler == nil {
		w.Handler = task.Run
	}
	if w.Logger == nil {
		w.Logger = slog.Default()
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for ctx.Err() == nil {
			op, cancel := context.WithTimeout(ctx, 2*time.Second)
			err := w.Store.AnnounceWorker(op, w.ID, w.Concurrency)
			cancel()
			if err != nil && ctx.Err() == nil {
				w.Logger.Error("worker heartbeat", "error", err)
			}
			if !pause(ctx, time.Second) {
				return
			}
		}
	})
	wg.Go(func() { w.recover(ctx) })
	for range w.Concurrency {
		wg.Go(func() { w.consume(ctx) })
	}
	wg.Wait()
	return nil
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func (w *Worker) recover(ctx context.Context) {
	for ctx.Err() == nil {
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		n, err := w.Store.Recover(op)
		cancel()
		if err != nil && ctx.Err() == nil {
			w.Logger.Error("recover leases", "error", err)
		} else if n > 0 {
			w.Logger.Info("recovered expired jobs", "count", n)
		}
		if !pause(ctx, min(w.Lease/2, time.Second)) {
			return
		}
	}
}

func (w *Worker) consume(ctx context.Context) {
	for ctx.Err() == nil {
		op, cancel := context.WithTimeout(ctx, 5*time.Second)
		j, err := w.Store.Claim(op, w.ID, w.Queues, w.Lease)
		cancel()
		if err != nil {
			if !errors.Is(err, queue.ErrNotFound) && ctx.Err() == nil {
				w.Logger.Error("claim job", "error", err)
			}
			if !pause(ctx, w.Poll) {
				return
			}
			continue
		}
		w.execute(ctx, j)
	}
}
