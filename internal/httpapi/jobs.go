package httpapi

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
	"github.com/Parkryan0128/distributed-job-runner/internal/task"
)

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
	if input.Queue != "" && input.Queue != "default" {
		writeError(w, 400, "Only the shared demo queue is available")
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
	if !s.Demo.authorize(w, r) {
		return
	}
	if !s.Demo.allowSubmit() {
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, "Too many submissions. Try again shortly.")
		return
	}
	job, created, err := s.Store.SubmitLimited(r.Context(), input, key, 100)
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
	if !validID(w, r) || !s.Demo.authorize(w, r) {
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
