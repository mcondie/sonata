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

// ApplyActionRequest is the body of POST /v1/action.apply. Definition is the
// action in JSON; the daemon parses and validates it again rather than
// trusting the client, because ad-hoc callers hit the endpoint directly.
type ApplyActionRequest struct {
	Definition json.RawMessage `json:"definition"`
}

// ApplyActionResponse reports the stored version. Changed is false when the
// definition matched the current version, so re-applying a directory of files
// is idempotent.
type ApplyActionResponse struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
	Changed bool   `json:"changed"`
	Enabled bool   `json:"enabled"`
}

// Action is the wire form of one stored action version.
type Action struct {
	Name       string          `json:"name"`
	Version    int64           `json:"version"`
	Definition json.RawMessage `json:"definition"`
	Enabled    bool            `json:"enabled"`
	CreatedAt  time.Time       `json:"created_at"`
}

// ListActionsRequest is the (empty) body of POST /v1/action.list.
type ListActionsRequest struct{}

// ActionSummary is one row of the action listing: the current version of a
// name and whether it is enabled.
type ActionSummary struct {
	Name      string    `json:"name"`
	Version   int64     `json:"version"`
	Enabled   bool      `json:"enabled"`
	Actor     string    `json:"actor"`
	Inputs    []string  `json:"inputs"`
	Output    string    `json:"output,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// ListActionsResponse lists the current version of every action, by name.
type ListActionsResponse struct {
	Actions []ActionSummary `json:"actions"`
}

// ShowActionRequest is the body of POST /v1/action.show. Version 0 means the
// current version.
type ShowActionRequest struct {
	Name    string `json:"name"`
	Version int64  `json:"version,omitempty"`
}

// SetActionEnabledRequest is the body of POST /v1/action.enable and
// /v1/action.disable.
type SetActionEnabledRequest struct {
	Name string `json:"name"`
}

// SetActionEnabledResponse reports the flag as it now stands.
type SetActionEnabledResponse struct {
	Name    string `json:"name"`
	Version int64  `json:"version"`
	Enabled bool   `json:"enabled"`
}

// Delivery is the wire form of one per-(message × action) processing record.
type Delivery struct {
	ID            string     `json:"id"`
	MessageID     *string    `json:"message_id,omitempty"`
	ActionName    string     `json:"action"`
	ActionVersion *int64     `json:"action_version,omitempty"`
	State         string     `json:"state"`
	Attempt       int        `json:"attempt"`
	NotBefore     *time.Time `json:"not_before,omitempty"`
	StderrTail    *string    `json:"stderr_tail,omitempty"`
	Error         *string    `json:"error,omitempty"`
	ClaimedAt     *time.Time `json:"claimed_at,omitempty"`
	CompletedAt   *time.Time `json:"completed_at,omitempty"`
}

// ListDeliveriesRequest is the body of POST /v1/delivery.list.
type ListDeliveriesRequest struct {
	Action    string `json:"action,omitempty"`
	State     string `json:"state,omitempty"`
	MessageID string `json:"message_id,omitempty"`
	Limit     int    `json:"limit,omitempty"`
	BeforeID  string `json:"before_id,omitempty"`
}

// ListDeliveriesResponse returns deliveries newest-first.
type ListDeliveriesResponse struct {
	Deliveries []Delivery `json:"deliveries"`
}

// ShowDeliveryRequest is the body of POST /v1/delivery.show.
type ShowDeliveryRequest struct {
	ID string `json:"id"`
}

// ReplayDeliveryRequest is the body of POST /v1/delivery.replay. Only dead
// deliveries replay; anything else is a 409 not_dead.
type ReplayDeliveryRequest struct {
	ID string `json:"id"`
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
