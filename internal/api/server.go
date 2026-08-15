package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"
)

// ServerOptions configures the HTTP surface.
type ServerOptions struct {
	Version   string
	StartedAt time.Time
	Log       *slog.Logger

	// OnActivity, when set, is called on every request. The daemon uses it
	// to drive its idle timeout.
	OnActivity func()
}

// Server implements the daemon's HTTP API.
type Server struct {
	opts ServerOptions
	mux  *http.ServeMux

	mu  sync.Mutex
	pid int
}

// NewServer builds the handler tree.
func NewServer(opts ServerOptions) *Server {
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.StartedAt.IsZero() {
		opts.StartedAt = time.Now()
	}
	s := &Server{opts: opts, mux: http.NewServeMux(), pid: os.Getpid()}

	// GET is a deliberate exception to the POST /v1/<noun>.<verb> convention:
	// health is a read-only liveness probe, and staying curl-able without a
	// body is worth the inconsistency. Everything else uses POST.
	s.mux.HandleFunc("GET /v1/health", s.handleHealth)

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.opts.OnActivity != nil {
		s.opts.OnActivity()
	}
	s.mux.ServeHTTP(w, r)
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	pid := s.pid
	s.mu.Unlock()

	resp := HealthResponse{
		Status:    "ok",
		PID:       pid,
		Version:   s.opts.Version,
		StartedAt: s.opts.StartedAt,
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		s.opts.Log.Error("encode health response", "error", err)
	}
}
