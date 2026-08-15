package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/config"
)

// Default bounds for EnsureRunning, applied when EnsureOptions leaves them
// zero.
const (
	defaultReadyTimeout  = 10 * time.Second
	defaultStartLockWait = 10 * time.Second
)

// EnsureOptions configures EnsureRunning.
type EnsureOptions struct {
	// ReadyTimeout bounds the wait for a spawned daemon to answer health.
	ReadyTimeout time.Duration // default 10s
	// IdleTimeout is passed through to the spawned daemon (0 disables).
	IdleTimeout time.Duration
	// StartLockWait bounds the wait on the start lock while another
	// process runs this same sequence.
	StartLockWait time.Duration // default 10s
}

// StartError reports that a spawned daemon never became ready. It carries the
// tail of the daemon log, which is the only place a detached child's startup
// failure is recorded — the process is released, so there is no pipe left to
// read.
type StartError struct {
	Err     error
	LogTail string
}

func (e *StartError) Error() string { return e.Err.Error() }
func (e *StartError) Unwrap() error { return e.Err }

// EnsureRunning returns a healthy daemon's identity, spawning one if
// nothing is listening. started reports whether this call spawned it.
//
// The whole sequence runs under the start lock, so concurrent callers
// collapse into a single spawn: the winner starts the daemon, the rest find
// it healthy and return started == false.
func EnsureRunning(ctx context.Context, cfg *config.Config, opts EnsureOptions) (*api.HealthResponse, bool, error) {
	readyTimeout := opts.ReadyTimeout
	if readyTimeout <= 0 {
		readyTimeout = defaultReadyTimeout
	}
	startLockWait := opts.StartLockWait
	if startLockWait <= 0 {
		startLockWait = defaultStartLockWait
	}

	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return nil, false, fmt.Errorf("create state dir: %w", err)
	}

	// Serialize concurrent ensures. Everything below — the health probe,
	// stale socket removal, and spawn — must happen under this lock or two
	// callers can both conclude nothing is running.
	startLock, err := AcquireWait(cfg.StartLockPath(), startLockWait)
	if err != nil {
		return nil, false, fmt.Errorf("acquire start lock: %w", err)
	}
	defer func() { _ = startLock.Release() }()

	client := api.NewClient(cfg.Socket)

	if h, err := client.Health(ctx); err == nil {
		return h, false, nil
	} else if !errors.Is(err, api.ErrNoDaemon) {
		return nil, false, fmt.Errorf("probe daemon: %w", err)
	}

	// Nothing answered. A socket file present now is stale: no live daemon
	// is behind it, and we hold the start lock so none can appear.
	if err := os.Remove(cfg.Socket); err != nil && !os.IsNotExist(err) {
		return nil, false, fmt.Errorf("remove stale socket: %w", err)
	}

	pid, err := Spawn(cfg, opts.IdleTimeout)
	if err != nil {
		return nil, false, err
	}

	h, err := WaitReady(ctx, client, readyTimeout)
	if err != nil {
		// Do not leave a half-started process behind.
		_, _ = Stop(pid, cfg, 2*time.Second)
		return nil, false, &StartError{
			Err:     err,
			LogTail: strings.TrimSpace(LogTail(cfg, 4096)),
		}
	}

	// We hold the start lock and spawned this PID, so anything else
	// answering means the child resolved a different socket than we probed
	// — config or lock-dir confusion. Fail loudly rather than report a PID
	// we did not start.
	if h.PID != pid {
		return nil, false, fmt.Errorf("daemon at %s reports pid %d, but we spawned pid %d", cfg.Socket, h.PID, pid)
	}

	return h, true, nil
}
