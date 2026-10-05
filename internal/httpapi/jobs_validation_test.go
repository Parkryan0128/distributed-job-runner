package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/Parkryan0128/distributed-job-runner/internal/httpapi"
	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func TestSubmissionMediaTypesAndKeyCharacters(t *testing.T) {
	s, h := setup(t)
	for _, tc := range []struct {
		name, media, key string
		status           int
	}{
		{"JSON with charset", "application/json; charset=utf-8", strings.Repeat("x", 128), 201},
		{"missing content type", "", "", 415},
		{"trailing separator", "application/json;", "", 201},
		{"invalid parameter", "application/json; charset", "", 415},
		{"different JSON type", "application/problem+json", "", 415},
		{"space in key", "application/json", "two words", 400},
		{"control character in key", "application/json", "a\tb", 400},
		{"non ASCII key", "application/json", "café", 400},
		{"oversized key", "application/json", strings.Repeat("x", 129), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(`{"kind":"demo","payload":{}}`))
			r.Header.Set("Content-Type", tc.media)
			r.Header.Set("Idempotency-Key", tc.key)
			r.AddCookie(&http.Cookie{Name: "demo_session", Value: "11111111-1111-4111-8111-111111111111"})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			expect(t, w, tc.status)
		})
	}
	stats, err := s.Stats(context.Background())
	if err != nil || stats.Queued != 2 {
		t.Fatalf("rejected requests created jobs: %+v %v", stats, err)
	}
}

func TestSubmissionBodyAndFieldBoundaries(t *testing.T) {
	s, h := setup(t)
	for _, tc := range []struct {
		name, body string
		status     int
	}{
		{"empty body", "", 400},
		{"array body", `[]`, 400},
		{"second JSON value", `{"kind":"demo","payload":{}} true`, 400},
		{"foreign queue", `{"kind":"demo","queue":"reports","payload":{}}`, 400},
		{"unknown payload field", `{"kind":"demo","payload":{"command":"run"}}`, 400},
		{"fractional work", `{"kind":"demo","payload":{"work_ms":0.5}}`, 400},
		{"negative delay", `{"kind":"demo","payload":{},"delay_seconds":-1}`, 400},
		{"excessive delay", `{"kind":"demo","payload":{},"delay_seconds":86401}`, 400},
		{"excessive timeout", `{"kind":"demo","payload":{},"timeout_seconds":301}`, 400},
		{"excessive attempts", `{"kind":"demo","payload":{},"max_attempts":11}`, 400},
		{"oversized trailing body", `{"kind":"demo","payload":{}}` + strings.Repeat(" ", 32768), 413},
		{"upper bounds", `{"kind":"demo","queue":"default","payload":{"work_ms":120000,"fail_until":10},"priority":9,"max_attempts":10,"timeout_seconds":300,"delay_seconds":86400}`, 201},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expect(t, request(h, "POST", "/api/jobs", tc.body, ""), tc.status)
		})
	}
	stats, err := s.Stats(context.Background())
	if err != nil || stats.Queued != 1 {
		t.Fatalf("validation changed queue unexpectedly: %+v %v", stats, err)
	}
}

func TestCapacityResponseAndReplay(t *testing.T) {
	s, h := setup(t)
	input := queue.Submit{Kind: "demo", Payload: json.RawMessage(`{}`)}
	for i := range 100 {
		key := ""
		if i == 0 {
			key = "replay"
		}
		if _, _, err := s.Submit(context.Background(), input, key); err != nil {
			t.Fatal(err)
		}
	}
	body := `{"kind":"demo","payload":{}}`
	w := request(h, "POST", "/api/jobs", body, "new")
	expect(t, w, 429)
	if w.Header().Get("Retry-After") != "1" {
		t.Fatal("capacity response omitted retry delay")
	}
	expect(t, request(h, "POST", "/api/jobs", body, "replay"), 200)
	expect(t, request(h, "POST", "/api/jobs", `{"kind":"demo","payload":{},"priority":1}`, "replay"), 409)
}

func TestEmptyAPICollectionsAndQueryBoundaries(t *testing.T) {
	_, h := setup(t)
	for _, path := range []string{"/api/pending", "/api/workers"} {
		w := request(h, "GET", path, "", "")
		expect(t, w, 200)
		if strings.TrimSpace(w.Body.String()) != "[]" {
			t.Fatalf("%s: %s", path, w.Body)
		}
	}
	for _, path := range []string{"/api/jobs?limit=1", "/api/jobs?limit=100", "/api/jobs?before=9223372036854775807"} {
		w := request(h, "GET", path, "", "")
		expect(t, w, 200)
		var page queue.Page
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || page.Jobs == nil || len(page.Jobs) != 0 || page.NextCursor != 0 {
			t.Fatalf("empty page: %+v %v", page, err)
		}
	}
	for _, query := range []string{"before=0", "before=9223372036854775808", "limit=9999999999999999999999"} {
		expect(t, request(h, "GET", "/api/jobs?"+query, "", ""), 400)
	}
	expect(t, request(h, "POST", "/api/jobs/not-an-id/cancel", "", ""), 400)
}

func TestDatabaseOutagePreservesLivenessAndReturnsServiceErrors(t *testing.T) {
	s, p := testdb.New(t)
	api := httpapi.Server{Store: s}
	h, err := api.Handler(nil)
	if err != nil {
		t.Fatal(err)
	}
	expect(t, request(h, "POST", "/api/demo", `{"action":"claim"}`, ""), 200)
	p.Close()
	expect(t, request(h, "GET", "/healthz", "", ""), 200)
	for _, path := range []string{"/readyz", "/api/jobs", "/api/jobs/" + queue.NewID(), "/api/stats", "/api/pending", "/api/workers", "/api/events"} {
		w := request(h, "GET", path, "", "")
		expect(t, w, 503)
		if strings.Contains(w.Body.String(), "closed pool") {
			t.Fatalf("database details exposed: %s", w.Body)
		}
	}
	expect(t, request(h, "POST", "/api/jobs", `{"kind":"demo","payload":{}}`, ""), 503)
}

func TestOriginChecksAndSecurityHeaders(t *testing.T) {
	api := httpapi.Server{Store: queue.New(nil)}
	h, err := api.Handler(fstest.MapFS{"index.html": {Data: []byte("runner")}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, origin, site string
		status             int
	}{
		{"no browser headers", "", "", 200},
		{"same HTTP origin", "http://example.com", "same-origin", 200},
		{"same HTTPS origin", "https://example.com", "same-origin", 200},
		{"foreign origin", "https://other.example", "", 403},
		{"different port", "https://example.com:444", "", 403},
		{"opaque origin", "null", "", 403},
		{"cross site without origin", "", "cross-site", 403},
		{"cross site with matching origin", "https://example.com", "CrOsS-SiTe", 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", "/api/demo", strings.NewReader(`{"action":"claim"}`))
			r.Header.Set("Origin", tc.origin)
			r.Header.Set("Sec-Fetch-Site", tc.site)
			r.AddCookie(&http.Cookie{Name: "demo_session", Value: "11111111-1111-4111-8111-111111111111"})
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			expect(t, w, tc.status)
			if w.Header().Get("X-Content-Type-Options") != "nosniff" || w.Header().Get("Referrer-Policy") != "no-referrer" || !strings.Contains(w.Header().Get("Content-Security-Policy"), "frame-ancestors 'none'") {
				t.Fatalf("missing security headers: %v", w.Header())
			}
		})
	}
	expect(t, request(h, "GET", "/api/missing", "", ""), 404)
	w := request(h, "POST", "/", "", "")
	expect(t, w, 405)
	if w.Header().Get("Allow") != "GET, HEAD" {
		t.Fatalf("static methods: %v", w.Header())
	}
	if _, err := (&httpapi.Server{}).Handler(nil); err == nil {
		t.Fatal("missing store accepted")
	}
}
