package task

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

type PermanentError struct{ Err error }

func (e PermanentError) Error() string { return e.Err.Error() }
func (e PermanentError) Unwrap() error { return e.Err }

type Demo struct {
	WorkMS    int `json:"work_ms"`
	FailUntil int `json:"fail_until"`
}

type Checksum struct {
	Text string `json:"text"`
}
type Statistics struct {
	Values []float64 `json:"values"`
}

func decode(raw json.RawMessage, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if !json.Valid(raw) || string(raw) == "null" {
		return errors.New("invalid task payload")
	}
	return d.Decode(value)
}

func Validate(kind string, raw json.RawMessage) error {
	switch kind {
	case "demo":
		var p Demo
		if err := decode(raw, &p); err != nil {
			return err
		}
		if p.WorkMS < 0 || p.WorkMS > 120000 || p.FailUntil < 0 || p.FailUntil > 10 {
			return errors.New("work_ms must be 0..120000 and fail_until must be 0..10")
		}
	case "checksum":
		var p Checksum
		if err := decode(raw, &p); err != nil {
			return err
		}
		if len(p.Text) > 12000 {
			return errors.New("text exceeds 12000 bytes")
		}
	case "statistics":
		var p Statistics
		if err := decode(raw, &p); err != nil {
			return err
		}
		if len(p.Values) < 1 || len(p.Values) > 1000 {
			return errors.New("values must contain 1..1000 numbers")
		}
		for _, v := range p.Values {
			if math.IsNaN(v) || math.IsInf(v, 0) || math.Abs(v) > 1e12 {
				return errors.New("values must be finite and within +/-1e12")
			}
		}
	default:
		return errors.New("kind must be demo, checksum or statistics")
	}
	return nil
}

func Run(ctx context.Context, j queue.Job) (json.RawMessage, error) {
	if err := Validate(j.Kind, j.Payload); err != nil {
		return nil, PermanentError{err}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch j.Kind {
	case "demo":
		var p Demo
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return nil, PermanentError{err}
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
	case "checksum":
		var p Checksum
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return nil, PermanentError{err}
		}
		sum := sha256.Sum256([]byte(p.Text))
		return json.Marshal(map[string]any{"sha256": hex.EncodeToString(sum[:]), "bytes": len(p.Text)})
	case "statistics":
		var p Statistics
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return nil, PermanentError{err}
		}
		lo, hi, total := p.Values[0], p.Values[0], 0.0
		for _, v := range p.Values {
			lo = min(lo, v)
			hi = max(hi, v)
			total += v
		}
		return json.Marshal(map[string]any{"count": len(p.Values), "sum": total, "mean": total / float64(len(p.Values)), "min": lo, "max": hi})
	}
	return nil, PermanentError{errors.New("unknown task")}
}
