package config_test

import (
	"testing"

	"github.com/Parkryan0128/distributed-job-runner/internal/config"
)

func TestCommandsOnlyReadTheirOwnSettings(t *testing.T) {
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("CONCURRENCY", "invalid")
	t.Setenv("QUEUES", "invalid queue")
	for _, mode := range []string{"api", "migrate"} {
		if _, err := config.Load(mode); err != nil {
			t.Fatalf("%s was blocked by worker settings: %v", mode, err)
		}
	}
	if _, err := config.Load("unknown"); err == nil {
		t.Fatal("unknown command accepted")
	}
}

func TestConfigurationRejectsMissingDatabaseAndUnsafeWorkerLimits(t *testing.T) {
	t.Setenv("DATABASE_URL", "")
	if _, err := config.Load("api"); err == nil {
		t.Fatal("missing database accepted")
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
	t.Setenv("CONCURRENCY", "0")
	if _, err := config.Load("worker"); err == nil {
		t.Fatal("zero concurrency accepted")
	}
	t.Setenv("CONCURRENCY", "2")
	t.Setenv("LEASE_SECONDS", "1")
	if _, err := config.Load("worker"); err == nil {
		t.Fatal("short lease accepted")
	}
	t.Setenv("LEASE_SECONDS", "10")
	t.Setenv("QUEUES", "default, reports")
	c, err := config.Load("worker")
	if err != nil || c.Queues[1] != "reports" {
		t.Fatalf("configuration: %+v %v", c, err)
	}
}
