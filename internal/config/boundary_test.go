package config_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/config"
	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

func cleanEnvironment(t *testing.T) {
	t.Helper()
	for _, key := range []string{"LISTEN_ADDR", "WEB_DIR", "WORKER_ID", "QUEUES", "CONCURRENCY", "LEASE_SECONDS", "POLL_MS", "SECURE_COOKIES", "HISTORY_RETENTION_HOURS"} {
		t.Setenv(key, "")
	}
	t.Setenv("DATABASE_URL", "postgres://localhost/test")
}

func TestConfigurationDefaults(t *testing.T) {
	cleanEnvironment(t)
	api, err := config.Load("api")
	if err != nil || api.Address != "127.0.0.1:8080" || api.WebDir != "web" || api.SecureCookies || api.HistoryRetention != 0 {
		t.Fatalf("API defaults: %+v %v", api, err)
	}
	worker, err := config.Load("worker")
	if err != nil || !queue.ValidID(worker.WorkerID) || worker.Concurrency != 2 || worker.Lease != 10*time.Second || worker.Poll != 500*time.Millisecond || len(worker.Queues) != 1 || worker.Queues[0] != "default" {
		t.Fatalf("worker defaults: %+v %v", worker, err)
	}
}

func TestNumericConfigurationBoundaries(t *testing.T) {
	for _, setting := range []struct {
		key, mode string
		low, high int
	}{
		{"CONCURRENCY", "worker", 1, 32},
		{"LEASE_SECONDS", "worker", 3, 60},
		{"POLL_MS", "worker", 50, 5000},
		{"HISTORY_RETENTION_HOURS", "api", 0, 720},
	} {
		for _, value := range []string{fmt.Sprint(setting.low), fmt.Sprint(setting.high), fmt.Sprint(setting.low - 1), fmt.Sprint(setting.high + 1), "abc", "1.5", "999999999999999999999999"} {
			t.Run(setting.key+"="+value, func(t *testing.T) {
				cleanEnvironment(t)
				t.Setenv(setting.key, value)
				c, err := config.Load(setting.mode)
				valid := value == fmt.Sprint(setting.low) || value == fmt.Sprint(setting.high)
				if (err == nil) != valid {
					t.Fatalf("config %+v error=%v, valid=%v", c, err, valid)
				}
				if !valid {
					return
				}
				var got int
				switch setting.key {
				case "CONCURRENCY":
					got = c.Concurrency
				case "LEASE_SECONDS":
					got = int(c.Lease / time.Second)
				case "POLL_MS":
					got = int(c.Poll / time.Millisecond)
				case "HISTORY_RETENTION_HOURS":
					got = int(c.HistoryRetention / time.Hour)
				}
				if fmt.Sprint(got) != value {
					t.Fatalf("converted value=%d, want %s", got, value)
				}
			})
		}
	}
}

func TestWorkerIdentityAndQueueBoundaries(t *testing.T) {
	for _, length := range []int{128, 129} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			cleanEnvironment(t)
			t.Setenv("WORKER_ID", strings.Repeat("w", length))
			c, err := config.Load("worker")
			if (err == nil) != (length == 128) || (err == nil && len(c.WorkerID) != length) {
				t.Fatalf("worker ID: %+v %v", c, err)
			}
		})
	}
	for _, queues := range []string{",default", "default,", "default,,reports", "UPPER", strings.Repeat("q", 33)} {
		t.Run(queues, func(t *testing.T) {
			cleanEnvironment(t)
			t.Setenv("QUEUES", queues)
			if _, err := config.Load("worker"); err == nil {
				t.Fatal("invalid queues accepted")
			}
		})
	}
	cleanEnvironment(t)
	t.Setenv("QUEUES", " default, "+strings.Repeat("q", 32)+" ")
	c, err := config.Load("worker")
	if err != nil || len(c.Queues) != 2 || c.Queues[0] != "default" || len(c.Queues[1]) != 32 {
		t.Fatalf("trimmed queues: %+v %v", c, err)
	}
}

func TestAPIOverridesAndInvalidCookieSetting(t *testing.T) {
	cleanEnvironment(t)
	t.Setenv("LISTEN_ADDR", "0.0.0.0:9090")
	t.Setenv("WEB_DIR", "custom-web")
	t.Setenv("SECURE_COOKIES", "true")
	c, err := config.Load("api")
	if err != nil || c.Address != "0.0.0.0:9090" || c.WebDir != "custom-web" || !c.SecureCookies {
		t.Fatalf("API overrides: %+v %v", c, err)
	}
	t.Setenv("SECURE_COOKIES", "maybe")
	if _, err := config.Load("api"); err == nil {
		t.Fatal("invalid cookie setting accepted")
	}
	for _, mode := range []string{"worker", "migrate"} {
		if _, err := config.Load(mode); err != nil {
			t.Fatalf("%s read API-only settings: %v", mode, err)
		}
	}
}
