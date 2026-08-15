// Package api defines the HTTP/JSON contract between the CLI and the daemon,
// plus a client and server for it.
//
// Request and response types are shared by both sides: the type is the
// contract, so a change breaks compilation on both ends at once.
package api

import (
	"encoding/json"
	"time"
)

// HealthResponse is returned by GET /v1/health.
type HealthResponse struct {
	Status    string    `json:"status"`
	PID       int       `json:"pid"`
	Version   string    `json:"version"`
	StartedAt time.Time `json:"started_at"`
}

// Uptime returns how long the daemon has been running.
func (h *HealthResponse) Uptime() time.Duration {
	return time.Since(h.StartedAt).Truncate(time.Second)
}

// Message is the wire form of a stored message.
type Message struct {
	ID                  string          `json:"id"`
	Queue               string          `json:"queue"`
	Payload             json.RawMessage `json:"payload"`
	Headers             json.RawMessage `json:"headers"`
	TraceID             string          `json:"trace_id"`
	OriginAction        *string         `json:"origin_action,omitempty"`
	OriginActionVersion *int64          `json:"origin_action_version,omitempty"`
	OriginMessageID     *string         `json:"origin_message_id,omitempty"`
	CreatedAt           time.Time       `json:"created_at"`
}

// SendMessageRequest is the body of POST /v1/message.send. Headers are
// user metadata; the server merges the system-stamped fields (hops) in.
type SendMessageRequest struct {
	Queue   string          `json:"queue"`
	Payload json.RawMessage `json:"payload"`
	Headers map[string]any  `json:"headers,omitempty"`
}

// SendMessageResponse identifies the appended message.
type SendMessageResponse struct {
	ID        string    `json:"id"`
	TraceID   string    `json:"trace_id"`
	CreatedAt time.Time `json:"created_at"`
}

// ListMessagesRequest is the body of POST /v1/message.list. Pagination is
// keyset: pass the last id of a page as BeforeID for the next page.
type ListMessagesRequest struct {
	Queue    string `json:"queue,omitempty"`
	TraceID  string `json:"trace_id,omitempty"`
	Limit    int    `json:"limit,omitempty"`
	BeforeID string `json:"before_id,omitempty"`
}

// ListMessagesResponse returns messages newest-first.
type ListMessagesResponse struct {
	Messages []Message `json:"messages"`
}

// ShowMessageRequest is the body of POST /v1/message.show.
type ShowMessageRequest struct {
	ID string `json:"id"`
}

// ListQueuesRequest is the (empty) body of POST /v1/queue.list.
type ListQueuesRequest struct{}

// QueueInfo describes one queue, derived from the messages that reference it.
type QueueInfo struct {
	Name     string `json:"name"`
	Messages int64  `json:"messages"`
}

// ListQueuesResponse lists every queue any message has referenced.
type ListQueuesResponse struct {
	Queues []QueueInfo `json:"queues"`
}

// ErrorBody carries a machine-readable code and a human-readable message.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorResponse is the envelope for every non-2xx response.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}
