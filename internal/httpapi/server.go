package httpapi

import (
	"context"
	"errors"
	"io/fs"
	"log/slog"
	"net/http"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

type Server struct {
	Store  *queue.Store
	Demo   *Demo
	Logger *slog.Logger
}

func (s *Server) Handler(assets fs.FS) (http.Handler, error) {
	if s.Store == nil {
		return nil, errors.New("store is required")
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	if s.Demo == nil {
		s.Demo = NewDemo(s.Store)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/events", s.Demo.stream)
	mux.HandleFunc("GET /api/demo", s.Demo.control)
	mux.HandleFunc("POST /api/demo", s.Demo.control)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		write(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.Store.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "database unavailable")
			return
		}
		write(w, http.StatusOK, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("POST /api/jobs", s.Demo.authorize(s.submit))
	mux.HandleFunc("GET /api/jobs", s.list)
	mux.HandleFunc("GET /api/jobs/{id}", s.get)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", s.Demo.authorize(s.cancel))
	mux.HandleFunc("GET /api/stats", s.stats)
	mux.HandleFunc("GET /api/pending", func(w http.ResponseWriter, r *http.Request) {
		jobs, err := s.Store.Pending(r.Context())
		if err != nil {
			s.failure(w, err)
			return
		}
		write(w, http.StatusOK, jobs)
	})
	mux.HandleFunc("GET /api/workers", func(w http.ResponseWriter, r *http.Request) {
		workers, err := s.Store.Workers(r.Context())
		if err != nil {
			s.failure(w, err)
			return
		}
		write(w, http.StatusOK, workers)
	})
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) { writeError(w, http.StatusNotFound, "route not found") })
	if assets != nil {
		files := http.FileServerFS(assets)
		mux.Handle("/", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodGet && r.Method != http.MethodHead {
				w.Header().Set("Allow", "GET, HEAD")
				writeError(w, http.StatusMethodNotAllowed, "method not allowed")
				return
			}
			files.ServeHTTP(w, r)
		}))
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method == "POST" && !sameOrigin(w, r) {
			return
		}
		if r.URL.Path == "/api/events" {
			mux.ServeHTTP(w, r)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	}), nil
}
