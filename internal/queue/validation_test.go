package queue_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

func TestNormalizationPreservesLargeNumbersAndPayloadLimit(t *testing.T) {
	input := queue.Submit{Kind: "demo", Payload: json.RawMessage(`{ "number": 9007199254740993, "nested": {"z": 2,"a": 1} }`)}
	if err := input.Normalize(); err != nil {
		t.Fatal(err)
	}
	if string(input.Payload) != `{"nested":{"a":1,"z":2},"number":9007199254740993}` || input.Queue != "default" || input.MaxAttempts != 3 || input.TimeoutSeconds != 30 {
		t.Fatalf("normalization changed values: %+v", input)
	}
	for _, size := range []int{16384, 16385} {
		input.Payload = json.RawMessage(`{"x":"` + strings.Repeat("a", size-8) + `"}`)
		err := input.Normalize()
		if (err == nil) != (size == 16384) {
			t.Fatalf("payload size %d: %v", size, err)
		}
	}
}

func TestFilterValidationBoundaries(t *testing.T) {
	for _, filter := range []queue.Filter{
		{Limit: 1}, {Limit: 100, Before: 1},
		{Limit: 10, Queue: "a" + strings.Repeat("0", 31)},
	} {
		if err := filter.Validate(); err != nil {
			t.Fatalf("valid filter %+v: %v", filter, err)
		}
	}
	for _, status := range []string{"queued", "running", "succeeded", "dead", "canceled"} {
		if err := (queue.Filter{Limit: 10, Status: status}).Validate(); err != nil {
			t.Fatalf("valid status %s: %v", status, err)
		}
	}
	for _, filter := range []queue.Filter{
		{Limit: 0}, {Limit: 101}, {Limit: 1, Before: -1},
		{Limit: 1, Status: "RUNNING"}, {Limit: 1, Queue: "a" + strings.Repeat("0", 32)},
		{Limit: 1, Queue: "../default"},
	} {
		if err := filter.Validate(); err == nil {
			t.Fatalf("invalid filter accepted: %+v", filter)
		}
	}
}
