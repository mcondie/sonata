// Package daemon implements the long-running Sonata process: its lock,
// socket, HTTP server, and lifecycle.
package daemon

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/config"
)

// Options configures a daemon run.
type Options struct {
	Config *config.Config
	// IdleTimeout, when > 0, stops the daemon after this long without a
	// request. Orphaned daemons from failed test runs self-terminate.
	IdleTimeout time.Duration
	Version     string
	Log         *slog.Logger
}

// shutdownGrace bounds how long in-flight requests get to finish.
const shutdownGrace = 5 * time.Second

// Run starts the daemon and blocks until it is signalled, its idle timeout
// expires, or ctx is cancelled. It never forks.
func Run(ctx context.Context, opts Options) error {
	cfg := opts.Config
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}

	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		return fmt.Errorf("create state dir: %w", err)
	}

	lock, err := Acquire(cfg.LockPath())
	if err != nil {
		if errors.Is(err, ErrLocked) {
			if pid, ok := RunningPID(cfg.LockPath()); ok && pid > 0 {
				return fmt.Errorf("another daemon is already running (pid %d)", pid)
			}
			return errors.New("another daemon is already running")
		}
		return err
	}
	defer func() { _ = lock.Release() }()

	if err := lock.WritePID(os.Getpid()); err != nil {
		return err
	}

	// Holding the exclusive lock means any socket file present is by
	// definition stale: no live daemon can be listening on it.
	if err := os.Remove(cfg.Socket); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove stale socket: %w", err)
	}

	ln, err := net.Listen("unix", cfg.Socket)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.Socket, err)
	}
	// The socket is an unauthenticated control channel; it must not be
	// reachable by other users on the machine.
	if err := os.Chmod(cfg.Socket, 0o600); err != nil {
		_ = ln.Close()
		return fmt.Errorf("chmod socket: %w", err)
	}
	defer func() { _ = os.Remove(cfg.Socket) }()

	idle := newIdleTracker(opts.IdleTimeout)
	srv := &http.Server{
		Handler: api.NewServer(api.ServerOptions{
			Version:    opts.Version,
			StartedAt:  time.Now(),
			Log:        log,
			OnActivity: idle.touch,
		}),
	}

	sigCtx, stop := signal.NotifyContext(ctx, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	serveErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	log.Info("daemon started",
		"pid", os.Getpid(),
		"socket", cfg.Socket,
		"version", opts.Version,
		"idle_timeout", opts.IdleTimeout)

	var reason string
	select {
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("serve: %w", err)
		}
		return nil
	case <-sigCtx.Done():
		reason = "signal"
	case <-idle.expired():
		reason = "idle timeout"
	}

	log.Info("daemon stopping", "reason", reason)

	shutCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		// Shutdown timed out; drop connections rather than hang.
		_ = srv.Close()
	}
	<-serveErr

	log.Info("daemon stopped", "pid", os.Getpid())
	return nil
}

// idleTracker fires when no request has arrived for the configured duration.
type idleTracker struct {
	timeout time.Duration
	ch      chan struct{}
	once    sync.Once

	mu   sync.Mutex
	last time.Time
}

func newIdleTracker(timeout time.Duration) *idleTracker {
	t := &idleTracker{timeout: timeout, ch: make(chan struct{}), last: time.Now()}
	if timeout > 0 {
		go t.watch()
	}
	return t
}

func (t *idleTracker) touch() {
	t.mu.Lock()
	t.last = time.Now()
	t.mu.Unlock()
}

func (t *idleTracker) expired() <-chan struct{} { return t.ch }

func (t *idleTracker) watch() {
	// Check several times per timeout so the granularity is not the timeout.
	interval := t.timeout / 4
	if interval < 100*time.Millisecond {
		interval = 100 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		t.mu.Lock()
		idleFor := time.Since(t.last)
		t.mu.Unlock()
		if idleFor >= t.timeout {
			t.once.Do(func() { close(t.ch) })
			return
		}
	}
}
