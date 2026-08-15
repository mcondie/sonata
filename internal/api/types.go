// Package api defines the HTTP/JSON contract between the CLI and the daemon,
// plus a client and server for it.
//
// Request and response types are shared by both sides: the type is the
// contract, so a change breaks compilation on both ends at once.
package api

import "time"

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

// ErrorBody carries a machine-readable code and a human-readable message.
type ErrorBody struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ErrorResponse is the envelope for every non-2xx response.
type ErrorResponse struct {
	Error ErrorBody `json:"error"`
}
