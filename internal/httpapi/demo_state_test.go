package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

func controlRequest(d *Demo, session, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/api/demo", strings.NewReader(body))
	r.AddCookie(&http.Cookie{Name: "demo_session", Value: session})
	w := httptest.NewRecorder()
	d.control(w, r)
	return w
}

func TestControlLifecycleDoesNotExtendOwnership(t *testing.T) {
	d := NewDemo(nil)
	owner, visitor := queue.NewID(), queue.NewID()
	if w := controlRequest(d, owner, `{"action":"claim"}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	expires := d.expires
	for _, body := range []string{`{"action":"claim"}`, `{"action":"start","level":"medium"}`, `{"action":"level","level":"high"}`} {
		w := controlRequest(d, owner, body)
		if w.Code != 200 || !d.expires.Equal(expires) {
			t.Fatalf("control renewed ownership: status=%d expires=%v want=%v", w.Code, d.expires, expires)
		}
	}
	if view := d.view(owner); !view.Mine || view.Available || !view.Running || view.Level != "high" || view.RemainingMS <= 0 {
		t.Fatalf("owner view: %+v", view)
	}
	if view := d.view(visitor); view.Mine || view.Available || !view.Running {
		t.Fatalf("visitor view: %+v", view)
	}
	for _, action := range []string{"claim", "start", "stop", "level"} {
		if w := controlRequest(d, visitor, `{"action":"`+action+`"}`); w.Code != 409 {
			t.Fatalf("visitor %s: %d", action, w.Code)
		}
	}
	if w := controlRequest(d, owner, `{"action":"stop"}`); w.Code != 200 || d.running || d.owner != "" {
		t.Fatalf("stop: %d owner=%q running=%v", w.Code, d.owner, d.running)
	}
	if w := controlRequest(d, visitor, `{"action":"start"}`); w.Code != 200 || d.owner != visitor || !d.running {
		t.Fatalf("new controller: %d owner=%q running=%v", w.Code, d.owner, d.running)
	}
	d.expires = time.Now().Add(-time.Second)
	if view := d.view(visitor); !view.Available || view.Mine || view.Running || view.RemainingMS != 0 {
		t.Fatalf("expired view: %+v", view)
	}
}

func TestInvalidControlRequestsLeaveTheDemoAvailable(t *testing.T) {
	d := NewDemo(nil)
	for _, tc := range []struct {
		body   string
		status int
	}{
		{"", 400}, {`{`, 400}, {`null`, 400}, {`[]`, 400},
		{`{"action":"unknown"}`, 400}, {`{"action":"start","level":"extreme"}`, 400},
		{strings.Repeat(" ", 1024) + `{"action":"start"}`, 400},
		{`{"action":"stop"}`, 409}, {`{"action":"level","level":"high"}`, 409},
	} {
		w := controlRequest(d, queue.NewID(), tc.body)
		if w.Code != tc.status || d.owner != "" || d.running || d.level != "low" {
			t.Fatalf("body=%q status=%d owner=%q running=%v level=%q", tc.body, w.Code, d.owner, d.running, d.level)
		}
	}
}

func TestSessionCookieCreationAndReuse(t *testing.T) {
	for _, tc := range []struct {
		name, url, cookie string
		secure, reuse     bool
	}{
		{"HTTP", "http://example.com", "", false, false},
		{"TLS", "https://example.com", "", true, false},
		{"invalid cookie", "http://example.com", "invalid", false, false},
		{"existing cookie", "http://example.com", queue.NewID(), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := NewDemo(nil)
			r := httptest.NewRequest("GET", tc.url+"/api/demo", nil)
			if tc.cookie != "" {
				r.AddCookie(&http.Cookie{Name: "demo_session", Value: tc.cookie})
			}
			w := httptest.NewRecorder()
			id := d.session(w, r)
			cookies := w.Result().Cookies()
			if tc.reuse {
				if id != tc.cookie || len(cookies) != 0 {
					t.Fatalf("existing session replaced: %q %v", id, cookies)
				}
				return
			}
			if !queue.ValidID(id) || len(cookies) != 1 {
				t.Fatalf("invalid session: %q %v", id, cookies)
			}
			c := cookies[0]
			if c.Value != id || c.Path != "/" || !c.HttpOnly || c.Secure != tc.secure || c.SameSite != http.SameSiteStrictMode || c.MaxAge != 86400 {
				t.Fatalf("cookie: %+v", c)
			}
		})
	}
}

func TestSubmissionBudgetRefillsAndSurvivesControlTransfer(t *testing.T) {
	d := NewDemo(nil)
	owner := queue.NewID()
	if w := controlRequest(d, owner, `{"action":"claim"}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	for range 20 {
		if !d.allowSubmit() {
			t.Fatal("initial burst rejected")
		}
	}
	if d.allowSubmit() {
		t.Fatal("exhausted burst accepted")
	}
	if w := controlRequest(d, owner, `{"action":"stop"}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if w := controlRequest(d, queue.NewID(), `{"action":"claim"}`); w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if d.allowSubmit() {
		t.Fatal("changing controllers reset the burst")
	}
	d.submitTokens = 0
	d.tokensUpdated = time.Now().Add(-time.Hour)
	for range 20 {
		if !d.allowSubmit() {
			t.Fatal("idle budget did not refill")
		}
	}
	if d.allowSubmit() {
		t.Fatal("idle budget exceeded the burst cap")
	}
}

func TestControlResponsePersonalizesOwnership(t *testing.T) {
	d := NewDemo(nil)
	owner := queue.NewID()
	controlRequest(d, owner, `{"action":"claim"}`)
	for _, id := range []string{owner, queue.NewID()} {
		r := httptest.NewRequest("GET", "/api/demo", nil)
		r.AddCookie(&http.Cookie{Name: "demo_session", Value: id})
		w := httptest.NewRecorder()
		d.control(w, r)
		var view demoView
		if err := json.Unmarshal(w.Body.Bytes(), &view); err != nil || w.Code != 200 || view.Mine != (id == owner) || view.Available {
			t.Fatalf("control response: %+v status=%d err=%v", view, w.Code, err)
		}
	}
}
