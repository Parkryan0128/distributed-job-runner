package queue

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"time"
)

var (
	ErrCapacity    = errors.New("shared queue is full; wait for existing jobs to finish")
	ErrNotFound    = errors.New("job not found")
	ErrConflict    = errors.New("job cannot be changed in its current state")
	ErrIdempotency = errors.New("idempotency key was used with a different request")
	ErrLeaseLost   = errors.New("job lease is no longer owned")
	namePattern    = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
	idPattern      = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
)

type Submit struct {
	Kind           string          `json:"kind"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	DelaySeconds   int             `json:"delay_seconds"`
}

func (s *Submit) Normalize() error {
	if s.Queue == "" {
		s.Queue = "default"
	}
	if s.MaxAttempts == 0 {
		s.MaxAttempts = 3
	}
	if s.TimeoutSeconds == 0 {
		s.TimeoutSeconds = 30
	}
	if !namePattern.MatchString(s.Kind) || !ValidQueue(s.Queue) {
		return errors.New("kind and queue must be lowercase names of up to 32 characters")
	}
	if s.Priority < 0 || s.Priority > 9 {
		return errors.New("priority must be between 0 and 9")
	}
	if s.MaxAttempts < 1 || s.MaxAttempts > 10 {
		return errors.New("max_attempts must be between 1 and 10")
	}
	if s.TimeoutSeconds < 1 || s.TimeoutSeconds > 300 {
		return errors.New("timeout_seconds must be between 1 and 300")
	}
	if s.DelaySeconds < 0 || s.DelaySeconds > 86400 {
		return errors.New("delay_seconds must be between 0 and 86400")
	}
	var payload map[string]any
	decoder := json.NewDecoder(bytes.NewReader(s.Payload))
	decoder.UseNumber()
	if len(s.Payload) > 16384 || !json.Valid(s.Payload) || decoder.Decode(&payload) != nil || payload == nil {
		return errors.New("payload must be a JSON object of at most 16 KiB")
	}
	var canonical bytes.Buffer
	encoder := json.NewEncoder(&canonical)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(payload); err != nil {
		return err
	}
	s.Payload = bytes.TrimSpace(canonical.Bytes())
	if len(s.Payload) > 16384 {
		return errors.New("normalized payload exceeds 16 KiB")
	}
	return nil
}

func ValidID(id string) bool      { return idPattern.MatchString(id) }
func ValidQueue(name string) bool { return namePattern.MatchString(name) }

func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	s := hex.EncodeToString(b[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", s[:8], s[8:12], s[12:16], s[16:20], s[20:])
}

type Job struct {
	ID             string          `json:"id"`
	Kind           string          `json:"kind"`
	Queue          string          `json:"queue"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	Status         string          `json:"status"`
	Attempt        int             `json:"attempt"`
	MaxAttempts    int             `json:"max_attempts"`
	TimeoutSeconds int             `json:"timeout_seconds"`
	WorkerID       *string         `json:"worker_id"`
	LeaseUntil     *time.Time      `json:"lease_until"`
	AvailableAt    time.Time       `json:"available_at"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
	Result         json.RawMessage `json:"result"`
	Error          string          `json:"error"`
}

type Attempt struct {
	Number     int        `json:"number"`
	WorkerID   string     `json:"worker_id"`
	Status     string     `json:"status"`
	StartedAt  time.Time  `json:"started_at"`
	FinishedAt *time.Time `json:"finished_at"`
	Error      string     `json:"error"`
}

type Detail struct {
	Job
	Attempts []Attempt `json:"attempts"`
}

type Filter struct {
	Status, Queue string
	Before        int64
	Limit         int
}

func (f Filter) Validate() error {
	if f.Limit < 1 || f.Limit > 100 {
		return errors.New("limit must be between 1 and 100")
	}
	if f.Status != "" && !ValidStatus(f.Status) {
		return errors.New("invalid status")
	}
	if f.Queue != "" && !ValidQueue(f.Queue) {
		return errors.New("invalid queue")
	}
	if f.Before < 0 {
		return errors.New("invalid cursor")
	}
	return nil
}

type Page struct {
	Jobs       []Job `json:"jobs"`
	NextCursor int64 `json:"next_cursor,omitempty"`
}

type Stats struct {
	Queued    int `json:"queued"`
	Running   int `json:"running"`
	Succeeded int `json:"succeeded"`
	Dead      int `json:"dead"`
	Canceled  int `json:"canceled"`
}

func ValidStatus(status string) bool {
	switch status {
	case "queued", "running", "succeeded", "dead", "canceled":
		return true
	}
	return false
}

func RetryDelay(attempt int) time.Duration {
	return time.Second * time.Duration(1<<min(max(attempt-1, 0), 6))
}
