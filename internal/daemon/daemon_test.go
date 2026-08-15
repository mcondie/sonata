package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
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
