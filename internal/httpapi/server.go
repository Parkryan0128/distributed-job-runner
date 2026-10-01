package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

type Server struct {
	Store  *queue.Store
	Logger *slog.Logger
}

func (s *Server) Handler(assets fs.FS) (http.Handler, error) {
	if s.Store == nil {
		return nil, errors.New("store is required")
	}
	if s.Logger == nil {
		s.Logger = slog.Default()
	}
	mux := http.NewServeMux()
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
	mux.HandleFunc("POST /api/jobs", s.submit)
	mux.HandleFunc("GET /api/jobs", s.list)
	mux.HandleFunc("GET /api/jobs/{id}", s.get)
	mux.HandleFunc("POST /api/jobs/{id}/cancel", s.cancel)
	mux.HandleFunc("GET /api/stats", s.stats)
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
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	}), nil
}

func write(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func writeError(w http.ResponseWriter, status int, message string) {
	write(w, status, map[string]string{"error": message})
}

func (s *Server) failure(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, queue.ErrNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, queue.ErrConflict), errors.Is(err, queue.ErrIdempotency):
		writeError(w, http.StatusConflict, err.Error())
	default:
		s.Logger.Error("API request failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "service temporarily unavailable")
	}
}

func (s *Server) submit(w http.ResponseWriter, r *http.Request) {
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		writeError(w, http.StatusUnsupportedMediaType, "Content-Type must be application/json")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 32768)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	var input queue.Submit
	if err := decoder.Decode(&input); err != nil {
		s.badBody(w, err)
		return
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		s.badBody(w, err)
		return
	}
	if err := input.Normalize(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if err := task.Validate(input.Kind, input.Payload); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if len(key) > 128 || strings.IndexFunc(key, func(r rune) bool { return r < 33 || r > 126 }) >= 0 {
		writeError(w, http.StatusBadRequest, "Idempotency-Key must contain at most 128 visible ASCII characters")
		return
	}
	job, created, err := s.Store.Submit(r.Context(), input, key)
	if err != nil {
		s.failure(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.Header().Set("Location", "/api/jobs/"+job.ID)
	write(w, status, job)
}

func (s *Server) badBody(w http.ResponseWriter, err error) {
	var size *http.MaxBytesError
	if errors.As(err, &size) {
		writeError(w, http.StatusRequestEntityTooLarge, "request exceeds 32 KiB")
		return
	}
	writeError(w, http.StatusBadRequest, "invalid JSON body or unknown field")
}

func (s *Server) list(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := queue.Filter{Status: q.Get("status"), Queue: q.Get("queue"), Limit: 50}
	if raw := q.Get("limit"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "limit must be 1..100")
			return
		}
		f.Limit = n
	}
	if raw := q.Get("before"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 1 {
			writeError(w, http.StatusBadRequest, "before must be a positive cursor")
			return
		}
		f.Before = n
	}
	if err := f.Validate(); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	page, err := s.Store.List(r.Context(), f)
	if err != nil {
		s.failure(w, err)
		return
	}
	write(w, http.StatusOK, page)
}

func validID(w http.ResponseWriter, r *http.Request) bool {
	if !queue.ValidID(r.PathValue("id")) {
		writeError(w, http.StatusBadRequest, "invalid job ID")
		return false
	}
	return true
}
func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	if !validID(w, r) {
		return
	}
	j, err := s.Store.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		s.failure(w, err)
		return
	}
	write(w, http.StatusOK, j)
}
func (s *Server) cancel(w http.ResponseWriter, r *http.Request) {
	if !validID(w, r) {
		return
	}
	id := r.PathValue("id")
	if err := s.Store.Cancel(r.Context(), id); err != nil {
		s.failure(w, err)
		return
	}
	j, err := s.Store.Get(r.Context(), id)
	if err != nil {
		s.failure(w, err)
		return
	}
	write(w, http.StatusOK, j)
}
func (s *Server) stats(w http.ResponseWriter, r *http.Request) {
	st, err := s.Store.Stats(r.Context())
	if err != nil {
		s.failure(w, err)
		return
	}
	write(w, http.StatusOK, st)
}
