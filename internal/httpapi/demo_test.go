package httpapi_test

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/Parkryan0128/distributed-job-runner/internal/httpapi"
	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/testdb"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func asVisitor(h http.Handler, id, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	r.AddCookie(&http.Cookie{Name: "demo_session", Value: id})
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}
func TestExclusiveControlAndExpiry(t *testing.T) {
	s, _ := testdb.New(t)
	d := httpapi.NewDemo(s)
	d.Duration = 150 * time.Millisecond
	api := httpapi.Server{Store: s, Demo: d}
	h, _ := api.Handler(nil)
	a, b := queue.NewID(), queue.NewID()
	ids := []string{a, b}
	codes := make([]int, 2)
	var wg sync.WaitGroup
	for i := range ids {
		wg.Go(func() { codes[i] = asVisitor(h, ids[i], "POST", "/api/demo", `{"action":"claim"}`).Code })
	}
	wg.Wait()
	owner, other := a, b
	if codes[0] == 409 {
		owner, other = b, a
	}
	if codes[0]+codes[1] != 609 {
		t.Fatalf("both visitors acquired control: %v", codes)
	}
	if w := asVisitor(h, other, "POST", "/api/jobs", `{"kind":"demo","payload":{}}`); w.Code != 409 {
		t.Fatalf("spectator submit: %d", w.Code)
	}
	w := asVisitor(h, owner, "POST", "/api/jobs", `{"kind":"demo","payload":{}}`)
	if w.Code != 201 {
		t.Fatal(w.Body.String())
	}
	var j queue.Job
	json.Unmarshal(w.Body.Bytes(), &j)
	if w = asVisitor(h, other, "POST", "/api/jobs/"+j.ID+"/cancel", ""); w.Code != 409 {
		t.Fatal("spectator cancellation permitted")
	}
	if w = asVisitor(h, other, "POST", "/api/demo", `{"action":"stop"}`); w.Code != 409 {
		t.Fatal("spectator stopped demo")
	}
	time.Sleep(170 * time.Millisecond)
	if w = asVisitor(h, owner, "POST", "/api/jobs", `{"kind":"demo","payload":{}}`); w.Code != 409 {
		t.Fatal("expired controller submitted work")
	}
	if w = asVisitor(h, other, "POST", "/api/demo", `{"action":"claim"}`); w.Code != 200 {
		t.Fatal("control was not released")
	}
}
func TestStreamPreservesRapidTransitionsAndReconnects(t *testing.T) {
	s, _ := testdb.New(t)
	d := httpapi.NewDemo(s)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)
	<-d.Ready
	api := httpapi.Server{Store: s, Demo: d}
	h, _ := api.Handler(nil)
	server := httptest.NewServer(h)
	defer server.Close()
	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Get(server.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data:") {
			break
		}
	}
	j, _, err := s.Submit(ctx, queue.Submit{Kind: "demo", Payload: json.RawMessage(`{}`)}, "")
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := s.Claim(ctx, "worker-a", []string{"default"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Finish(ctx, claimed, json.RawMessage(`{}`), "", false); err != nil {
		t.Fatal(err)
	}
	statuses := []string{}
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		var e queue.Event
		if json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &e) == nil && e.Job.ID == j.ID {
			statuses = append(statuses, e.Job.Status)
			if len(statuses) == 3 {
				break
			}
		}
	}
	if strings.Join(statuses, ",") != "queued,running,succeeded" {
		t.Fatalf("lost transitions: %v %v", statuses, scanner.Err())
	}
	resp.Body.Close()
	resp, err = client.Get(server.URL + "/api/events")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	scanner = bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data:") {
			var snap queue.Snapshot
			json.Unmarshal([]byte(strings.TrimPrefix(scanner.Text(), "data: ")), &snap)
			if snap.Stats.Succeeded != 1 || len(snap.Jobs) != 0 {
				t.Fatalf("bad reconnect snapshot: %+v", snap)
			}
			break
		}
	}
}
func TestServerGeneratorDoesNotDependOnBrowser(t *testing.T) {
	s, _ := testdb.New(t)
	d := httpapi.NewDemo(s)
	d.Duration = 450 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.Run(ctx)
	<-d.Ready
	api := httpapi.Server{Store: s, Demo: d}
	h, _ := api.Handler(nil)
	w := asVisitor(h, queue.NewID(), "POST", "/api/demo", `{"action":"start","level":"high"}`)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	time.Sleep(700 * time.Millisecond)
	first, _ := s.Stats(ctx)
	if first.Queued < 1 || first.Queued > 2 {
		t.Fatalf("generation: %+v", first)
	}
	time.Sleep(400 * time.Millisecond)
	second, _ := s.Stats(ctx)
	if first != second {
		t.Fatal("generator continued after expiration")
	}
}
