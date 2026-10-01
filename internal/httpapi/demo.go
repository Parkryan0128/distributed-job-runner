package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

type message struct {
	kind string
	id   int64
	data any
}

// Demo owns the one public demo controller and broadcasts database events.
// Run one API instance; workers remain independent processes.
type Demo struct {
	store            *queue.Store
	Ready            chan struct{}
	mu               sync.Mutex
	owner            string
	expires          time.Time
	level            string
	running          bool
	next             time.Time
	sent             int
	Duration         time.Duration
	SecureCookies    bool
	HistoryRetention time.Duration
	submitTokens     float64
	tokensUpdated    time.Time
	subscribers      map[chan message]struct{}
}
type demoView struct {
	Available   bool   `json:"available"`
	Mine        bool   `json:"mine"`
	Running     bool   `json:"running"`
	Level       string `json:"level"`
	RemainingMS int64  `json:"remaining_ms"`
}

func NewDemo(store *queue.Store) *Demo {
	return &Demo{store: store, Ready: make(chan struct{}), level: "low", Duration: 30 * time.Second, submitTokens: 20, tokensUpdated: time.Now(), subscribers: make(map[chan message]struct{})}
}
func (d *Demo) expire() {
	if d.owner != "" && !time.Now().Before(d.expires) {
		d.owner = ""
		d.running = false
	}
}
func (d *Demo) view(session string) demoView {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.expire()
	return demoView{d.owner == "", d.owner != "" && d.owner == session, d.running, d.level, max(0, time.Until(d.expires).Milliseconds())}
}
func (d *Demo) session(w http.ResponseWriter, r *http.Request) string {
	if c, err := r.Cookie("demo_session"); err == nil && queue.ValidID(c.Value) {
		return c.Value
	}
	id := queue.NewID()
	http.SetCookie(w, &http.Cookie{Name: "demo_session", Value: id, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode, Secure: d.SecureCookies || r.TLS != nil, MaxAge: 86400})
	return id
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
func (d *Demo) control(w http.ResponseWriter, r *http.Request) {
	who := d.session(w, r)
	if r.Method == "GET" {
		write(w, 200, d.view(who))
		return
	}
	var in struct {
		Action string `json:"action"`
		Level  string `json:"level"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&in); err != nil {
		writeError(w, 400, "invalid control request")
		return
	}
	if in.Action != "claim" && in.Action != "start" && in.Action != "stop" && in.Action != "level" {
		writeError(w, 400, "invalid action")
		return
	}
	if in.Level != "" && in.Level != "low" && in.Level != "medium" && in.Level != "high" {
		writeError(w, 400, "invalid workload")
		return
	}
	d.mu.Lock()
	d.expire()
	if d.owner != "" && d.owner != who {
		d.mu.Unlock()
		writeError(w, 409, "Another visitor controls this demo.")
		return
	}
	if (in.Action == "level" || in.Action == "stop") && d.owner != who {
		d.mu.Unlock()
		writeError(w, 409, "Take control first.")
		return
	}
	if in.Action == "claim" || in.Action == "start" {
		if d.owner == "" {
			d.owner = who
			d.expires = time.Now().Add(d.Duration)
			d.sent = 0
		}
		if in.Action == "start" {
			d.running = true
			d.next = time.Now()
		}
	}
	if in.Level != "" {
		d.level = in.Level
	}
	if in.Action == "stop" {
		d.owner = ""
		d.running = false
	}
	d.broadcastLocked(message{kind: "demo"})
	d.mu.Unlock()
	write(w, 200, d.view(who))
}
func (d *Demo) authorize(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		who := d.session(w, r)
		d.mu.Lock()
		defer d.mu.Unlock()
		d.expire()
		if d.owner == "" || d.owner != who {
			writeError(w, 409, "Take control before changing jobs.")
			return
		}
		next(w, r)
	}
}
func (d *Demo) generate(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.expire()
	if !d.running || time.Now().Before(d.next) {
		return
	}
	rate, every := 0.3, 20
	if d.level == "medium" {
		rate, every = 1.2, 10
	}
	if d.level == "high" {
		rate, every = 3, 5
	}
	d.next = time.Now().Add(time.Duration(float64(time.Second) / rate))
	op, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	stats, err := d.store.Stats(op)
	if err != nil || stats.Queued >= 40 {
		return
	}
	n := d.sent + 1
	fail := 0
	if n%every == 0 {
		fail = 1
	}
	payload, _ := json.Marshal(map[string]int{"work_ms": []int{1500, 2000, 2500}[(n-1)%3], "fail_until": fail})
	if _, _, err = d.store.Submit(op, queue.Submit{Kind: "demo", Payload: payload}, ""); err == nil {
		d.sent = n
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
			d.generate(ctx)
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
func sameOrigin(w http.ResponseWriter, r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin != "" && origin != "http://"+r.Host && origin != "https://"+r.Host {
		writeError(w, 403, "Cross-origin changes are not allowed.")
		return false
	}
	if strings.EqualFold(r.Header.Get("Sec-Fetch-Site"), "cross-site") {
		writeError(w, 403, "Cross-site changes are not allowed.")
		return false
	}
	return true
}

// Called under the controller mutex. A global burst survives ownership changes.
func (d *Demo) allowSubmit() bool {
	now := time.Now()
	d.submitTokens = min(20, d.submitTokens+now.Sub(d.tokensUpdated).Seconds()*5)
	d.tokensUpdated = now
	if d.submitTokens < 1 {
		return false
	}
	d.submitTokens--
	return true
}
