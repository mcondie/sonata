package api

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/mcondie/sonata/internal/store"
)

// ServerOptions configures the HTTP surface.
type ServerOptions struct {
	Version   string
	StartedAt time.Time
	Log       *slog.Logger

	// Store backs the data endpoints. When nil (some lifecycle tests), only
	// health is served.
	Store *store.Store

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

	if opts.Store != nil {
		handle(s, "message.send", s.messageSend)
		handle(s, "message.list", s.messageList)
		handle(s, "message.show", s.messageShow)
		handle(s, "queue.list", s.queueList)
		handle(s, "action.apply", s.actionApply)
		handle(s, "action.list", s.actionList)
		handle(s, "action.show", s.actionShow)
		handle(s, "action.enable", s.actionEnable)
		handle(s, "action.disable", s.actionDisable)
	}

	return s
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if s.opts.OnActivity != nil {
		s.opts.OnActivity()
	}
	s.mux.ServeHTTP(w, r)
}

// handle registers a POST /v1/<endpoint> route with uniform JSON
// decode/encode and sentinel-to-status error mapping.
func handle[Req, Resp any](s *Server, endpoint string, fn func(context.Context, *Req) (*Resp, error)) {
	s.mux.HandleFunc("POST /v1/"+endpoint, func(w http.ResponseWriter, r *http.Request) {
		var req Req
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<20))
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid_request", "read body: "+err.Error())
			return
		}
		if len(body) > 0 {
			if err := json.Unmarshal(body, &req); err != nil {
				writeError(w, http.StatusBadRequest, "invalid_request", "decode request: "+err.Error())
				return
			}
		}

		resp, err := fn(r.Context(), &req)
		if err != nil {
			status, code := errorStatus(err)
			if status == http.StatusInternalServerError {
				s.opts.Log.Error("handler failed", "endpoint", endpoint, "error", err)
			}
			writeError(w, status, code, err.Error())
			return
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			s.opts.Log.Error("encode response", "endpoint", endpoint, "error", err)
		}
	})
}

func (s *Server) messageSend(ctx context.Context, req *SendMessageRequest) (*SendMessageResponse, error) {
	if req.Queue == "" {
		return nil, invalidf("queue is required")
	}
	// An absent field decodes as literal null, so null means "no payload".
	if len(req.Payload) == 0 || string(req.Payload) == "null" {
		return nil, invalidf("payload is required")
	}
	if !json.Valid(req.Payload) {
		return nil, invalidf("payload must be valid JSON")
	}

	// Ingress stamping (spec 003): fresh trace, zero hops, no origin. The
	// user's headers ride along; hops is system-owned and always wins.
	headers := make(map[string]any, len(req.Headers)+1)
	for k, v := range req.Headers {
		headers[k] = v
	}
	headers["hops"] = 0
	headersJSON, err := json.Marshal(headers)
	if err != nil {
		return nil, invalidf("encode headers: %v", err)
	}

	m := &store.Message{
		ID:        store.NewID(),
		Queue:     req.Queue,
		Payload:   req.Payload,
		Headers:   headersJSON,
		TraceID:   store.NewID(),
		CreatedAt: time.Now().UTC(),
	}
	if err := s.opts.Store.AppendMessage(ctx, m); err != nil {
		return nil, err
	}
	return &SendMessageResponse{ID: m.ID, TraceID: m.TraceID, CreatedAt: m.CreatedAt}, nil
}

func (s *Server) messageList(ctx context.Context, req *ListMessagesRequest) (*ListMessagesResponse, error) {
	if req.Limit < 0 {
		return nil, invalidf("limit must be >= 0")
	}
	msgs, err := s.opts.Store.ListMessages(ctx, store.ListOptions{
		Queue:    req.Queue,
		TraceID:  req.TraceID,
		Limit:    req.Limit,
		BeforeID: req.BeforeID,
	})
	if err != nil {
		return nil, err
	}
	resp := &ListMessagesResponse{Messages: make([]Message, 0, len(msgs))}
	for _, m := range msgs {
		resp.Messages = append(resp.Messages, toAPIMessage(m))
	}
	return resp, nil
}

func (s *Server) messageShow(ctx context.Context, req *ShowMessageRequest) (*Message, error) {
	if req.ID == "" {
		return nil, invalidf("id is required")
	}
	m, err := s.opts.Store.GetMessage(ctx, req.ID)
	if err != nil {
		return nil, err
	}
	resp := toAPIMessage(m)
	return &resp, nil
}

func (s *Server) queueList(ctx context.Context, _ *ListQueuesRequest) (*ListQueuesResponse, error) {
	queues, err := s.opts.Store.ListQueues(ctx)
	if err != nil {
		return nil, err
	}
	resp := &ListQueuesResponse{Queues: make([]QueueInfo, 0, len(queues))}
	for _, q := range queues {
		resp.Queues = append(resp.Queues, QueueInfo{Name: q.Name, Messages: q.Messages})
	}
	return resp, nil
}

func toAPIMessage(m *store.Message) Message {
	return Message{
		ID:                  m.ID,
		Queue:               m.Queue,
		Payload:             m.Payload,
		Headers:             m.Headers,
		TraceID:             m.TraceID,
		OriginAction:        m.OriginAction,
		OriginActionVersion: m.OriginActionVersion,
		OriginMessageID:     m.OriginMessageID,
		CreatedAt:           m.CreatedAt,
	}
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
