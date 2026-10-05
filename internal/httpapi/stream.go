package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type message struct {
	kind string
	id   int64
	data any
}

func (d *Demo) broadcast(m message) { d.mu.Lock(); defer d.mu.Unlock(); d.broadcastLocked(m) }
func (d *Demo) broadcastLocked(m message) {
	for ch := range d.subscribers {
		select {
		case ch <- m:
		default:
			close(ch)
			delete(d.subscribers, ch)
		}
	}
}
func (d *Demo) stream(w http.ResponseWriter, r *http.Request) {
	who := d.session(w, r)
	ch := make(chan message, 64)
	d.mu.Lock()
	if len(d.subscribers) >= 200 {
		d.mu.Unlock()
		writeError(w, 503, "Demo is busy. Try again shortly.")
		return
	}
	d.subscribers[ch] = struct{}{}
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.subscribers, ch); d.mu.Unlock() }()
	op, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	snap, err := d.store.Snapshot(op)
	cancel()
	if err != nil {
		writeError(w, 503, "Snapshot unavailable")
		return
	}
	rc := http.NewResponseController(w)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("X-Accel-Buffering", "no")
	send := func(kind string, id int64, data any) error {
		_ = rc.SetWriteDeadline(time.Now().Add(10 * time.Second))
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		if id > 0 {
			if _, err = fmt.Fprintf(w, "id: %d\n", id); err != nil {
				return err
			}
		}
		if _, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", kind, raw); err != nil {
			return err
		}
		return rc.Flush()
	}
	if send("snapshot", snap.Cursor, snap) != nil {
		return
	}
	if send("demo", 0, d.view(who)) != nil {
		return
	}
	// A heartbeat also detects disconnected viewers while the queue is idle.
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if send("heartbeat", 0, struct{}{}) != nil {
				return
			}
		case m, ok := <-ch:
			if !ok || m.kind == "unavailable" {
				return
			}
			if m.kind == "reset" {
				_ = send("reset", 0, struct{}{})
				return
			}
			if m.id > 0 && m.id <= snap.Cursor {
				continue
			}
			if m.kind == "demo" {
				m.data = d.view(who)
			}
			if send(m.kind, m.id, m.data) != nil {
				return
			}
		}
	}
}
