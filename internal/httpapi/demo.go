package httpapi

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

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

// Check ownership when admitting a mutation, after reading its input. An admitted
// database operation may finish after the turn ends without blocking the next one.
func (d *Demo) authorize(w http.ResponseWriter, r *http.Request) bool {
	who := d.session(w, r)
	d.mu.Lock()
	d.expire()
	allowed := d.owner != "" && d.owner == who
	d.mu.Unlock()
	if !allowed {
		writeError(w, 409, "Take control before changing jobs.")
	}
	return allowed
}

// A global burst survives ownership changes.
func (d *Demo) allowSubmit() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	d.submitTokens = min(20, d.submitTokens+now.Sub(d.tokensUpdated).Seconds()*5)
	d.tokensUpdated = now
	if d.submitTokens < 1 {
		return false
	}
	d.submitTokens--
	return true
}
