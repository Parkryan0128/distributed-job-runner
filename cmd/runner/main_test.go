package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthcheckUsesConfiguredAddress(t *testing.T) {
	for _, status := range []int{http.StatusOK, http.StatusServiceUnavailable} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path != "/readyz" {
				t.Errorf("unexpected health path: %s", r.URL.Path)
			}
			w.WriteHeader(status)
		}))
		err := healthcheck(strings.TrimPrefix(server.URL, "http://"))
		server.Close()
		if (err == nil) != (status == http.StatusOK) {
			t.Fatalf("readiness %d: %v", status, err)
		}
	}
}
