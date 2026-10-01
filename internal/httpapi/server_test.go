package httpapi_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/httpapi"
	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
)

func setup(t *testing.T) (*queue.Store, http.Handler) {
	t.Helper()
	s, _ := testdb.New(t)
	api := httpapi.Server{Store: s, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	h, err := api.Handler(fstest.MapFS{"index.html": {Data: []byte("<h1>Job runner</h1>")}})
	if err != nil {
		t.Fatal(err)
	}
	expect(t, request(h, "POST", "/api/demo", `{"action":"claim"}`, ""), 200)
	return s, h
}
func request(h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(&http.Cookie{Name: "demo_session", Value: "11111111-1111-4111-8111-111111111111"})
	if key != "" {
		r.Header.Set("Idempotency-Key", key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func expect(t *testing.T, w *httptest.ResponseRecorder, status int) {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status=%d want=%d body=%s", w.Code, status, w.Body)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("API response can be cached")
	}
}
func submitted(t *testing.T, h http.Handler, body string) queue.Job {
	t.Helper()
	w := request(h, "POST", "/api/jobs", body, "")
	expect(t, w, 201)
	var j queue.Job
	if err := json.Unmarshal(w.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	return j
}

func TestConsoleAndAPIWorkWithoutCredentials(t *testing.T) {
	_, h := setup(t)
	for _, path := range []string{"/", "/healthz", "/readyz", "/api/jobs", "/api/stats"} {
		expect(t, request(h, "GET", path, "", ""), 200)
	}
	submitted(t, h, `{"kind":"demo","payload":{}}`)
}

func TestAPISubmissionReplayConflictAndDetail(t *testing.T) {
	_, h := setup(t)
	body := `{"kind":"demo","payload":{"work_ms":1}}`
	first := request(h, "POST", "/api/jobs", body, "same-request")
	expect(t, first, 201)
	var j queue.Job
	if err := json.Unmarshal(first.Body.Bytes(), &j); err != nil {
		t.Fatal(err)
	}
	replay := request(h, "POST", "/api/jobs", body, "same-request")
	expect(t, replay, 200)
	var again queue.Job
	if err := json.Unmarshal(replay.Body.Bytes(), &again); err != nil {
		t.Fatal(err)
	}
	if j.ID != again.ID || first.Header().Get("Location") != "/api/jobs/"+j.ID {
		t.Fatal("replay created another job")
	}
	expect(t, request(h, "POST", "/api/jobs", `{"kind":"demo","payload":{}}`, "same-request"), 409)
	got := request(h, "GET", "/api/jobs/"+j.ID, "", "")
	expect(t, got, 200)
	var d queue.Detail
	if err := json.Unmarshal(got.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.Status != "queued" || d.Attempts == nil || len(d.Attempts) != 0 {
		t.Fatalf("new job: %+v", d)
	}
}

func TestInvalidTaskValuesDoNotEnterTheQueue(t *testing.T) {
	s, h := setup(t)
	for _, body := range []string{
		`{"kind":"statistics","payload":{"values":[null,10]}}`,
		`{"kind":"statistics","payload":{"values":[0,null]}}`,
		`{"kind":"checksum","payload":{"text":null}}`,
		`{"kind":"checksum","payload":{}}`,
	} {
		expect(t, request(h, "POST", "/api/jobs", body, ""), 400)
	}
	stats, err := s.Stats(context.Background())
	if err != nil || stats.Queued != 0 {
		t.Fatalf("invalid input entered the queue: %+v %v", stats, err)
	}
	submitted(t, h, `{"kind":"demo","payload":{"work_ms":0}}`)
	submitted(t, h, `{"kind":"demo","payload":{"fail_until":1}}`)
}

func TestAPIRejectsInvalidAndOversizedRequests(t *testing.T) {
	s, h := setup(t)
	for _, body := range []string{`{`, `null`, `{}`, `{"kind":"demo","payload":{},"owner":"fake"}`, `{"kind":"shell","payload":{}}`, `{"kind":"demo","payload":{"work_ms":-1}}`, `{"kind":"demo","payload":{},"priority":10}`, `{"kind":"demo","payload":{},"max_attempts":-1}`, `{"kind":"statistics","payload":{"values":[]}}`, `{"kind":"demo","payload":{}} {}`} {
		expect(t, request(h, "POST", "/api/jobs", body, ""), 400)
	}
	expect(t, request(h, "POST", "/api/jobs", `{"kind":"checksum","payload":{"text":"`+strings.Repeat("a", 33000)+`"}}`, ""), 413)
	expect(t, request(h, "POST", "/api/jobs", `{"kind":"demo","payload":{}}`, strings.Repeat("x", 129)), 400)
	r := httptest.NewRequest("POST", "/api/jobs", strings.NewReader(`{}`))

	r.Header.Set("Content-Type", "text/plain")
	r.AddCookie(&http.Cookie{Name: "demo_session", Value: "11111111-1111-4111-8111-111111111111"})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	expect(t, w, 415)
	st, err := s.Stats(context.Background())
	if err != nil || st.Queued != 0 {
		t.Fatalf("invalid job stored: %+v %v", st, err)
	}
}

func TestAPIListFiltersPaginationAndBadQueries(t *testing.T) {
	_, h := setup(t)
	for range 3 {
		submitted(t, h, `{"kind":"demo","payload":{},"delay_seconds":60}`)
	}
	submitted(t, h, `{"kind":"demo","payload":{}}`)
	w := request(h, "GET", "/api/jobs?status=queued&limit=2", "", "")
	expect(t, w, 200)
	var p queue.Page
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Jobs) != 2 || p.NextCursor == 0 {
		t.Fatalf("page: %+v", p)
	}
	w = request(h, "GET", fmt.Sprintf("/api/jobs?status=queued&limit=2&before=%d", p.NextCursor), "", "")
	expect(t, w, 200)
	if err := json.Unmarshal(w.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if len(p.Jobs) != 2 {
		t.Fatalf("next page: %+v", p)
	}
	for _, query := range []string{"limit=0", "limit=101", "limit=abc", "before=-1", "before=abc", "status=unknown", "queue=../bad"} {
		expect(t, request(h, "GET", "/api/jobs?"+query, "", ""), 400)
	}
	expect(t, request(h, "GET", "/api/jobs/nope", "", ""), 400)
	expect(t, request(h, "GET", "/api/jobs/"+queue.NewID(), "", ""), 404)
}

func TestAPICancellationAndTerminalConflict(t *testing.T) {
	s, h := setup(t)
	j := submitted(t, h, `{"kind":"demo","payload":{}}`)
	for range 2 {
		expect(t, request(h, "POST", "/api/jobs/"+j.ID+"/cancel", "", ""), 200)
	}
	j = submitted(t, h, `{"kind":"demo","payload":{}}`)
	claimed, err := s.Claim(context.Background(), "worker", []string{"default"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Finish(context.Background(), claimed, json.RawMessage(`{}`), "", false); err != nil {
		t.Fatal(err)
	}
	expect(t, request(h, "POST", "/api/jobs/"+j.ID+"/cancel", "", ""), 409)
	expect(t, request(h, "POST", "/api/jobs/"+queue.NewID()+"/cancel", "", ""), 404)
}

func TestStatsReflectJobStates(t *testing.T) {
	_, h := setup(t)
	j := submitted(t, h, `{"kind":"demo","payload":{}}`)
	submitted(t, h, `{"kind":"demo","payload":{}}`)
	expect(t, request(h, "POST", "/api/jobs/"+j.ID+"/cancel", "", ""), 200)
	w := request(h, "GET", "/api/stats", "", "")
	expect(t, w, 200)
	var st queue.Stats
	if err := json.Unmarshal(w.Body.Bytes(), &st); err != nil {
		t.Fatal(err)
	}
	if st.Queued != 1 || st.Canceled != 1 {
		t.Fatalf("stats: %+v", st)
	}

}

func TestRouteRegistrationAndStaticAssetsWithoutDatabase(t *testing.T) {
	api := httpapi.Server{Store: queue.New(nil)}
	h, err := api.Handler(fstest.MapFS{"index.html": {Data: []byte("Job Runner")}})
	if err != nil {
		t.Fatal(err)
	}
	expect(t, request(h, "GET", "/", "", ""), 200)
	expect(t, request(h, "GET", "/healthz", "", ""), 200)

	expect(t, request(h, "POST", "/", "", ""), 405)
}
