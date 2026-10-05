package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/Parkryan0128/distributed-job-runner/internal/queue"
)

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
	case errors.Is(err, queue.ErrCapacity):
		w.Header().Set("Retry-After", "1")
		writeError(w, http.StatusTooManyRequests, err.Error())
	case errors.Is(err, queue.ErrConflict), errors.Is(err, queue.ErrIdempotency):
		writeError(w, http.StatusConflict, err.Error())
	default:
		s.Logger.Error("API request failed", "error", err)
		writeError(w, http.StatusServiceUnavailable, "service temporarily unavailable")
	}
}
