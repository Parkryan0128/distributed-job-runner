package httpapi

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

func (d *Demo) generate(ctx context.Context, now time.Time) {
	d.mu.Lock()
	d.expire()
	if !d.running || now.Before(d.next) {
		d.mu.Unlock()
		return
	}
	rate, every := 0.3, 20
	if d.level == "medium" {
		rate, every = 1.2, 10
	}
	if d.level == "high" {
		rate, every = 3, 5
	}
	interval := time.Duration(float64(time.Second) / rate)
	d.next = d.next.Add(interval)
	if !d.next.After(now) {
		// Skip missed arrivals after a stall instead of generating a catch-up burst.
		d.next = now.Add(interval)
	}
	owner, expires, level, n := d.owner, d.expires, d.level, d.sent+1
	op, cancel := context.WithTimeout(ctx, min(2*time.Second, time.Until(expires)))
	d.generationCancel = cancel
	d.mu.Unlock()
	defer func() {
		cancel()
		d.mu.Lock()
		d.generationCancel = nil
		d.mu.Unlock()
	}()
	stats, err := d.store.Stats(op)
	if err != nil || stats.Queued >= 40 {
		return
	}
	// Recheck the turn after the database read, just before admitting a submission.
	d.mu.Lock()
	d.expire()
	allowed := d.running && d.owner == owner && d.expires.Equal(expires) && d.level == level
	d.mu.Unlock()
	if !allowed {
		return
	}
	fail := 0
	if n%every == 0 {
		fail = 1
	}
	payload, _ := json.Marshal(map[string]int{"work_ms": []int{1500, 2000, 2500}[(n-1)%3], "fail_until": fail})
	if _, _, err = d.store.Submit(op, queue.Submit{Kind: "demo", Payload: payload}, ""); err == nil {
		d.mu.Lock()
		if d.owner == owner && d.expires.Equal(expires) {
			d.sent = n
		}
		d.mu.Unlock()
	}
}

func (d *Demo) Run(ctx context.Context) {
	// A single reader drains committed transitions; no per-viewer database polling.
	cursor := int64(0)
	for {
		snap, err := d.store.Snapshot(ctx)
		if err == nil {
			cursor = snap.Cursor
			break
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(time.Second):
		}
	}
	close(d.Ready)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	var lastWorkers, lastPrune time.Time
	for {
		select {
		case <-ctx.Done():
			d.mu.Lock()
			for ch := range d.subscribers {
				close(ch)
				delete(d.subscribers, ch)
			}
			d.mu.Unlock()
			return
		case <-ticker.C:
			d.generate(ctx, time.Now())
			op, cancel := context.WithTimeout(ctx, 2*time.Second)
			events, err := d.store.Events(op, cursor)
			if err != nil {
				d.broadcast(message{kind: "unavailable"})
			}
			if err == nil {
				for _, e := range events {
					d.broadcast(message{kind: "job", id: e.ID, data: e})
					cursor = e.ID
				}
			}
			if time.Since(lastWorkers) >= time.Second {
				if workers, err := d.store.Workers(op); err == nil {
					d.broadcast(message{kind: "workers", data: workers})
				}
				d.broadcast(message{kind: "demo"})
				lastWorkers = time.Now()
			}
			if time.Since(lastPrune) >= time.Minute {
				_ = d.store.PruneEvents(op)
				if n, err := d.store.PruneHistory(op, d.HistoryRetention); err == nil && n > 0 {
					// Counts/history changed without a job transition; reconnect to a fresh snapshot.
					d.broadcast(message{kind: "reset"})
				}
				lastPrune = time.Now()
			}
			cancel()
		}
	}
}
