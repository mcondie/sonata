package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"syscall"

	"github.com/mcondie/sonata/internal/store"
)

// ErrNoDaemon indicates nothing is listening on the socket. Callers
// distinguish "not running" from "running but broken" with this.
var ErrNoDaemon = errors.New("no daemon")

// ErrInvalid marks a request the server refuses as malformed. Handlers wrap
// it with the specifics; it maps to 400.
var ErrInvalid = errors.New("invalid request")

// Error is a server-reported failure, decoded from the error envelope.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// isNoDaemon reports whether a dial error means the socket has no listener.
// A missing socket file and a refused connection both mean the same thing to
// a caller; a leftover socket file from a crash produces the latter.
func isNoDaemon(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ENOENT) ||
		errors.Is(err, fs.ErrNotExist)
}

// errorStatus maps sentinel errors to HTTP status and machine code. This is
// the only place status codes are decided; handlers return sentinels.
func errorStatus(err error) (status int, code string) {
	switch {
	case errors.Is(err, store.ErrNotFound):
		return http.StatusNotFound, "not_found"
	case errors.Is(err, ErrInvalid):
		return http.StatusBadRequest, "invalid_request"
	default:
		return http.StatusInternalServerError, "internal"
	}
}

// invalidf wraps ErrInvalid with a caller-facing explanation.
func invalidf(format string, args ...any) error {
	return fmt.Errorf(format+": %w", append(args, ErrInvalid)...)
}

// writeError emits the standard error envelope. Status codes are mapped here
// and nowhere else, so handlers return sentinels rather than codes.
func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: ErrorBody{Code: code, Message: msg}})
}
