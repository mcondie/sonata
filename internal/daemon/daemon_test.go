package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/config"
)

// testConfig builds a config with a socket short enough for sun_path.
// t.TempDir() on macOS lives under /var/folders/... and overruns the
// 104-byte limit.
func testConfig(t *testing.T) *config.Config {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "sn")
	if err != nil {
		t.Fatalf("temp socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	stateDir := t.TempDir()
	return &config.Config{
		StateDir: stateDir,
		Socket:   filepath.Join(sockDir, "s.sock"),
		Database: filepath.Join(stateDir, "sonata.db"),
		LogLevel: "error",
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// startDaemon runs the daemon in-process and waits for it to serve.
func startDaemon(t *testing.T, cfg *config.Config, idle time.Duration) (stop func(), errCh <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan error, 1)

	go func() {
		ch <- Run(ctx, Options{
			Config:      cfg,
			IdleTimeout: idle,
			Version:     "test",
			Log:         discardLogger(),
		})
	}()

	client := api.NewClient(cfg.Socket)
	if _, err := WaitReady(context.Background(), client, 5*time.Second); err != nil {
		cancel()
		t.Fatalf("daemon never became ready: %v", err)
	}

	stopped := false
	return func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case <-ch:
		case <-time.After(10 * time.Second):
			t.Error("daemon did not shut down")
		}
	}, ch
}

func TestRunServesHealthAndCleansUp(t *testing.T) {
	cfg := testConfig(t)
	stop, _ := startDaemon(t, cfg, 0)

	client := api.NewClient(cfg.Socket)
	h, err := client.Health(context.Background())
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	if h.Status != "ok" {
		t.Errorf("status = %q, want ok", h.Status)
	}
	if h.PID != os.Getpid() {
		t.Errorf("pid = %d, want %d", h.PID, os.Getpid())
	}
	if h.Version != "test" {
		t.Errorf("version = %q, want test", h.Version)
	}

	stop()

	// A leftover socket file would make the next `up` do stale-socket
	// detection it should not need to.
	if _, err := os.Stat(cfg.Socket); !os.IsNotExist(err) {
		t.Errorf("socket should be removed on shutdown, stat err = %v", err)
	}
	// The lock must be free once the process releases it.
	if _, running := RunningPID(cfg.LockPath()); running {
		t.Error("lock still held after shutdown")
	}
}

func TestRunRefusesSecondDaemon(t *testing.T) {
	// Short-guarded because Run now waits out lockWait before giving up; a
	// live daemon holds the lock for that whole grace.
	if testing.Short() {
		t.Skip("integration test")
	}
	cfg := testConfig(t)
	stop, _ := startDaemon(t, cfg, 0)
	defer stop()

	err := Run(context.Background(), Options{
		Config:  cfg,
		Version: "test",
		Log:     discardLogger(),
	})
	if err == nil {
		t.Fatal("second daemon should refuse to start")
	}
	if got := err.Error(); !contains(got, "already running") {
		t.Errorf("error = %q, want it to mention 'already running'", got)
	}
}

// TestRunWaitsForDyingPredecessor covers the restart race: a stopping daemon
// closes its listener before releasing the lock, so a successor spawned in
// that window must wait out the drain rather than exit "already running".
func TestRunWaitsForDyingPredecessor(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	cfg := testConfig(t)

	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() {
		first <- Run(ctx, Options{Config: cfg, Version: "first", Log: discardLogger()})
	}()

	client := api.NewClient(cfg.Socket)
	if _, err := WaitReady(context.Background(), client, 5*time.Second); err != nil {
		cancel()
		t.Fatalf("first daemon never became ready: %v", err)
	}

	// Pin the first daemon inside its drain window. A connection that has
	// sent request bytes but no terminating blank line counts as active, so
	// Shutdown waits on it for the full shutdownGrace.
	conn, err := net.Dial("unix", cfg.Socket)
	if err != nil {
		cancel()
		t.Fatalf("dial socket: %v", err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte("POST /v1/health HTTP/1.1\r\nHost: sonata\r\n")); err != nil {
		cancel()
		t.Fatalf("write partial request: %v", err)
	}

	cancel()

	// Confirm we are actually in the window this test exists for: the
	// listener is closed (clients see ECONNREFUSED) while the lock is still
	// held. Without the pinned connection this window is microseconds.
	if !waitForCond(2*time.Second, func() bool {
		_, held := RunningPID(cfg.LockPath())
		return held && !accepting(cfg.Socket)
	}) {
		t.Fatal("predecessor never entered the drain window; test cannot exercise the race")
	}

	// Immediately: this is exactly what an autostart client does after
	// seeing ECONNREFUSED on the dying daemon's socket.
	second := make(chan error, 1)
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	go func() {
		second <- Run(secondCtx, Options{Config: cfg, Version: "second", Log: discardLogger()})
	}()

	h, err := WaitReady(context.Background(), client, lockWait+5*time.Second)
	if err != nil {
		select {
		case rerr := <-second:
			t.Fatalf("successor exited instead of waiting out the drain: %v", rerr)
		default:
		}
		t.Fatalf("successor never became ready: %v", err)
	}
	if h.Version != "second" {
		t.Errorf("version = %q, want second — the predecessor is still serving", h.Version)
	}

	if err := <-first; err != nil {
		t.Errorf("first daemon exited with error: %v", err)
	}
	cancelSecond()
	if err := <-second; err != nil {
		t.Errorf("second daemon exited with error: %v", err)
	}
}

func TestRunFailsWhenLockHeldBeyondGrace(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	cfg := testConfig(t)
	if err := os.MkdirAll(cfg.StateDir, 0o700); err != nil {
		t.Fatalf("mkdir state dir: %v", err)
	}

	// A holder that never releases stands in for a live, serving daemon:
	// waiting longer than the grace would buy nothing.
	lock, err := Acquire(cfg.LockPath())
	if err != nil {
		t.Fatalf("acquire lock: %v", err)
	}
	defer func() { _ = lock.Release() }()
	if err := lock.WritePID(424242); err != nil {
		t.Fatalf("write pid: %v", err)
	}

	start := time.Now()
	err = Run(context.Background(), Options{Config: cfg, Version: "test", Log: discardLogger()})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("Run should refuse to start while the lock is held")
	}
	if got := err.Error(); !contains(got, "already running") || !contains(got, "424242") {
		t.Errorf("error = %q, want it to mention 'already running' and the holder's pid", got)
	}
	if elapsed < lockWait {
		t.Errorf("failed after %s, want it to wait out the full %s grace first", elapsed, lockWait)
	}
}

func TestEnsureRunningIdempotent(t *testing.T) {
	cfg := testConfig(t)
	stop, _ := startDaemon(t, cfg, 0)
	defer stop()

	h, started, err := EnsureRunning(context.Background(), cfg, EnsureOptions{})
	if err != nil {
		t.Fatalf("EnsureRunning: %v", err)
	}
	if started {
		t.Error("started = true, want false — a daemon was already serving")
	}
	if h.PID != os.Getpid() {
		t.Errorf("pid = %d, want %d (the in-process daemon)", h.PID, os.Getpid())
	}

	// The start lock must not be left held, or the next ensure stalls.
	if _, held := RunningPID(cfg.StartLockPath()); held {
		t.Error("start lock still held after EnsureRunning returned")
	}
}

// waitForCond polls cond until it holds, reporting whether it ever did.
func waitForCond(timeout time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// accepting reports whether something is answering connects on path.
func accepting(path string) bool {
	c, err := net.Dial("unix", path)
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func TestRunRemovesStaleSocket(t *testing.T) {
	cfg := testConfig(t)

	// A plain file where the socket belongs, as a crashed daemon leaves.
	if err := os.WriteFile(cfg.Socket, []byte("stale"), 0o600); err != nil {
		t.Fatalf("write stale socket: %v", err)
	}

	stop, _ := startDaemon(t, cfg, 0)
	defer stop()

	if _, err := api.NewClient(cfg.Socket).Health(context.Background()); err != nil {
		t.Fatalf("daemon should have replaced the stale socket: %v", err)
	}
}

func TestRunIdleTimeout(t *testing.T) {
	cfg := testConfig(t)
	_, errCh := startDaemon(t, cfg, 250*time.Millisecond)

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("daemon exited with error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("daemon did not stop on idle timeout")
	}

	if _, err := api.NewClient(cfg.Socket).Health(context.Background()); !errors.Is(err, api.ErrNoDaemon) {
		t.Errorf("after idle stop: got %v, want ErrNoDaemon", err)
	}
}

func TestHealthOnMissingSocketIsErrNoDaemon(t *testing.T) {
	cfg := testConfig(t)
	_, err := api.NewClient(cfg.Socket).Health(context.Background())
	if !errors.Is(err, api.ErrNoDaemon) {
		t.Fatalf("got %v, want ErrNoDaemon", err)
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && (s == sub || len(sub) == 0 || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
