package task

import (
	"bytes"
	"encoding/json"
	"errors"
)

type Demo struct {
	WorkMS    int `json:"work_ms"`
	FailUntil int `json:"fail_until"`
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

	default:
		return nil, errors.New("kind must be demo")
	}
}
