package api

import (
	"encoding/json"
	"errors"
	"io/fs"
	"net/http"
	"syscall"
)

// ErrNoDaemon indicates nothing is listening on the socket. Callers
// distinguish "not running" from "running but broken" with this.
var ErrNoDaemon = errors.New("no daemon")

// isNoDaemon reports whether a dial error means the socket has no listener.
// A missing socket file and a refused connection both mean the same thing to
// a caller; a leftover socket file from a crash produces the latter.
func isNoDaemon(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.ENOENT) ||
		errors.Is(err, fs.ErrNotExist)
}

// writeError emits the standard error envelope. Status codes are mapped here
// and nowhere else, so handlers return sentinels rather than codes.
func writeError(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorResponse{Error: ErrorBody{Code: code, Message: msg}})
}
