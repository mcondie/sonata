package daemon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/config"
)

// readyPollInterval is how often Spawn re-probes health while waiting.
const readyPollInterval = 25 * time.Millisecond

// Spawn starts a detached daemon child running `sonata daemon`.
//
// The child is placed in a new session so it survives the CLI exiting and is
// not killed by terminal signals sent to the parent's process group. Its
// output goes to the state directory's log file, which is also where startup
// failures are read back from — the process is released, so there is no pipe
// to read after the fact.
func Spawn(cfg *config.Config, idleTimeout time.Duration) (int, error) {
	exe, err := os.Executable()
	if err != nil {
		return 0, fmt.Errorf("locate own binary: %w", err)
	}

	logFile, err := os.OpenFile(cfg.LogPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return 0, fmt.Errorf("open daemon log: %w", err)
	}
	defer logFile.Close()

	args := []string{"daemon"}
	if idleTimeout > 0 {
		args = append(args, "--idle-timeout", idleTimeout.String())
	}

	cmd := exec.Command(exe, args...)
	cmd.Stdin = nil
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	// Setsid detaches the child into its own session and process group.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	// The child re-resolves config independently, so the resolved settings
	// must be propagated explicitly rather than inherited from flags.
	// config.Env is the single source of truth for what gets forwarded.
	cmd.Env = append(os.Environ(), cfg.Env()...)

	if err := cmd.Start(); err != nil {
		return 0, fmt.Errorf("start daemon: %w", err)
	}
	pid := cmd.Process.Pid
	// The child is not ours to reap; detach from it entirely.
	if err := cmd.Process.Release(); err != nil {
		return pid, fmt.Errorf("release daemon process: %w", err)
	}
	return pid, nil
}

// WaitReady polls health until the daemon answers or timeout elapses.
func WaitReady(ctx context.Context, c *api.Client, timeout time.Duration) (*api.HealthResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(readyPollInterval)
	defer ticker.Stop()

	var last error
	for {
		h, err := c.Health(ctx)
		if err == nil {
			return h, nil
		}
		last = err

		select {
		case <-ctx.Done():
			if last != nil && !errors.Is(last, api.ErrNoDaemon) {
				return nil, fmt.Errorf("daemon did not become ready within %s: %w", timeout, last)
			}
			return nil, fmt.Errorf("daemon did not become ready within %s", timeout)
		case <-ticker.C:
		}
	}
}

// LogTail returns the last n bytes of the daemon log, for surfacing startup
// failures to the user.
func LogTail(cfg *config.Config, n int64) string {
	f, err := os.Open(cfg.LogPath())
	if err != nil {
		return ""
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return ""
	}
	off := info.Size() - n
	if off < 0 {
		off = 0
	}
	buf := make([]byte, info.Size()-off)
	if _, err := f.ReadAt(buf, off); err != nil {
		return ""
	}
	return string(buf)
}

// Stop signals the daemon and waits for both the process and its socket to
// disappear. It escalates to SIGKILL if the graceful stop times out.
func Stop(pid int, cfg *config.Config, timeout time.Duration) (killed bool, err error) {
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil {
		if errors.Is(err, syscall.ESRCH) {
			return false, nil // already gone
		}
		return false, fmt.Errorf("signal pid %d: %w", pid, err)
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !Alive(pid) && !socketExists(cfg.Socket) {
			return false, nil
		}
		time.Sleep(readyPollInterval)
	}

	// Graceful stop failed; take the process out and clear the socket so the
	// next `up` is not blocked by a file nobody is listening on.
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		return true, fmt.Errorf("kill pid %d: %w", pid, err)
	}
	if err := os.Remove(cfg.Socket); err != nil && !os.IsNotExist(err) {
		return true, fmt.Errorf("remove socket: %w", err)
	}
	return true, nil
}

func socketExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
