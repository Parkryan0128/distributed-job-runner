package queue_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

func TestSubmissionValidation(t *testing.T) {
	cases := []queue.Submit{
		{Kind: "Bad name", Payload: json.RawMessage(`{}`)},
		{Kind: "demo", Queue: "../queue", Payload: json.RawMessage(`{}`)},
		{Kind: "demo", Priority: 10, Payload: json.RawMessage(`{}`)},
		{Kind: "demo", MaxAttempts: 11, Payload: json.RawMessage(`{}`)},
		{Kind: "demo", MaxAttempts: -1, Payload: json.RawMessage(`{}`)},
		{Kind: "demo", TimeoutSeconds: 301, Payload: json.RawMessage(`{}`)},
		{Kind: "demo", DelaySeconds: -1, Payload: json.RawMessage(`{}`)},
		{Kind: "demo", Payload: json.RawMessage(`null`)},
		{Kind: "demo", Payload: json.RawMessage(`[]`)},
		{Kind: "demo", Payload: json.RawMessage(`{} {}`)},
		{Kind: "demo", Payload: json.RawMessage(`{"text":"` + strings.Repeat("x", 16384) + `"}`)},
	}
	for _, c := range cases {
		if err := c.Normalize(); err == nil {
			t.Fatalf("accepted invalid request: %+v", c)
		}
	}
	valid := queue.Submit{Kind: "demo", Priority: 9, MaxAttempts: 10, TimeoutSeconds: 300, DelaySeconds: 86400, Payload: json.RawMessage(`{}`)}
	if err := valid.Normalize(); err != nil {
		t.Fatal(err)
	}
	if queue.RetryDelay(10) != 64*time.Second {
		t.Fatal("backoff is not capped")
	}
}

func TestNormalizationPreservesLargeTextAcrossRepeatedValidation(t *testing.T) {
	input := queue.Submit{Kind: "checksum", Payload: json.RawMessage(`{"text":"` + strings.Repeat("<>&", 3000) + `"}`)}
	if err := input.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := string(input.Payload)
	if err := input.Normalize(); err != nil {
		t.Fatalf("valid payload failed its second validation: %v", err)
	}
	if string(input.Payload) != first {
		t.Fatal("normalization changed an already normalized payload")
	}
}
