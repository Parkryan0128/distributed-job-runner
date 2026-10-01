package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

type Config struct {
	DatabaseURL string
	Address     string
	WebDir      string
	WorkerID    string
	Queues      []string
	Concurrency int
	Lease       time.Duration
	Poll        time.Duration
}

func Load(mode string) (Config, error) {
	c := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
	}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required")
	}
	switch mode {
	case "migrate":
		return c, nil
	case "api":
		c.Address = env("LISTEN_ADDR", "127.0.0.1:8080")
		c.WebDir = env("WEB_DIR", "web")
		return c, nil
	case "worker":
		c.WorkerID = os.Getenv("WORKER_ID")
		if c.WorkerID == "" {
			c.WorkerID = queue.NewID()
		}
		c.Queues = strings.Split(env("QUEUES", "default,reports"), ",")
	default:
		return c, errors.New("unknown command")
	}
	var err error
	c.Concurrency, err = integer("CONCURRENCY", 2, 1, 32)
	if err != nil {
		return c, err
	}
	seconds, err := integer("LEASE_SECONDS", 10, 3, 60)
	if err != nil {
		return c, err
	}
	c.Lease = time.Duration(seconds) * time.Second
	ms, err := integer("POLL_MS", 500, 50, 5000)
	if err != nil {
		return c, err
	}
	c.Poll = time.Duration(ms) * time.Millisecond
	for i, name := range c.Queues {
		c.Queues[i] = strings.TrimSpace(name)
		if !queue.ValidQueue(c.Queues[i]) {
			return c, fmt.Errorf("invalid queue %q", name)
		}
	}
	if len(c.WorkerID) > 128 {
		return c, errors.New("WORKER_ID exceeds 128 bytes")
	}
	return c, nil
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func integer(key string, fallback, lo, hi int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < lo || n > hi {
		return 0, fmt.Errorf("%s must be %d..%d", key, lo, hi)
	}
	return n, nil
}
