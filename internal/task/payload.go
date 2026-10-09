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

func Validate(kind string, raw json.RawMessage) error {
	_, err := parse(kind, raw)
	return err
}

func parse(kind string, raw json.RawMessage) (Demo, error) {
	var p Demo
	if kind != "demo" {
		return p, errors.New("kind must be demo")
	}
	trimmed := bytes.TrimSpace(raw)
	if !json.Valid(raw) || len(trimmed) == 0 || trimmed[0] != '{' {
		return p, errors.New("invalid task payload")
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if err := d.Decode(&p); err != nil {
		return p, err
	}
	if p.WorkMS < 0 || p.WorkMS > 120000 || p.FailUntil < 0 || p.FailUntil > 10 {
		return p, errors.New("work_ms must be 0..120000 and fail_until must be 0..10")
	}
	return p, nil
}
