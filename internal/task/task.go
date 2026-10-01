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
	"strings"
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
	Text *string `json:"text"`
}
type Statistics struct {
	Values []*float64 `json:"values"`
}

func decode(raw json.RawMessage, value any) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	trimmed := bytes.TrimSpace(raw)
	if !json.Valid(raw) || len(trimmed) == 0 || trimmed[0] != '{' {
		return errors.New("invalid task payload")
	}
	return d.Decode(value)
}

func Validate(kind string, raw json.RawMessage) error {
	_, err := parse(kind, raw)
	return err
}

func parse(kind string, raw json.RawMessage) (any, error) {
	switch kind {
	case "demo":
		var p Demo
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if p.WorkMS < 0 || p.WorkMS > 120000 || p.FailUntil < 0 || p.FailUntil > 10 {
			return nil, errors.New("work_ms must be 0..120000 and fail_until must be 0..10")
		}
		return p, nil
	case "checksum":
		var p Checksum
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if p.Text == nil {
			return nil, errors.New("text must be a string")
		}
		if len(*p.Text) > 12000 {
			return nil, errors.New("text exceeds 12000 bytes")
		}
		if strings.ContainsRune(*p.Text, 0) {
			return nil, errors.New("text cannot contain a null character")
		}
		return p, nil
	case "statistics":
		var p Statistics
		if err := decode(raw, &p); err != nil {
			return nil, err
		}
		if len(p.Values) < 1 || len(p.Values) > 1000 {
			return nil, errors.New("values must contain 1..1000 numbers")
		}
		for _, v := range p.Values {
			if v == nil || math.IsNaN(*v) || math.IsInf(*v, 0) || math.Abs(*v) > 1e12 {
				return nil, errors.New("values must be non-null finite numbers within +/-1e12")
			}
		}
		return p, nil
	default:
		return nil, errors.New("kind must be demo, checksum or statistics")
	}
}

func Run(ctx context.Context, j queue.Job) (json.RawMessage, error) {
	payload, err := parse(j.Kind, j.Payload)
	if err != nil {
		return nil, PermanentError{err}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	switch p := payload.(type) {
	case Demo:
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
	case Checksum:
		sum := sha256.Sum256([]byte(*p.Text))
		return json.Marshal(map[string]any{"sha256": hex.EncodeToString(sum[:]), "bytes": len(*p.Text)})
	case Statistics:
		lo, hi, total := *p.Values[0], *p.Values[0], 0.0
		for _, v := range p.Values {
			lo = min(lo, *v)
			hi = max(hi, *v)
			total += *v
		}
		return json.Marshal(map[string]any{"count": len(p.Values), "sum": total, "mean": total / float64(len(p.Values)), "min": lo, "max": hi})
	}
	return nil, PermanentError{errors.New("unknown task")}
}
