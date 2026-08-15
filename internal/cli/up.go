package cli

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/daemon"
)

// startLockWait bounds how long `up` waits for another `up` to finish.
const startLockWait = 10 * time.Second

func newUpCmd(a *app) *cobra.Command {
	var (
		timeout     time.Duration
		idleTimeout time.Duration
	)

	cmd := &cobra.Command{
		Use:   "up",
		Short: "Start the daemon, waiting until it is ready",
		Long: "Start the daemon and block until it is accepting connections.\n" +
			"Idempotent: exits 0 if a daemon is already running.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.up(cmd.Context(), timeout, idleTimeout)
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "how long to wait for the daemon to become ready")
	cmd.Flags().DurationVar(&idleTimeout, "idle-timeout", 0, "stop the daemon after this long without a request (0 disables)")
	return cmd
}

func (a *app) up(ctx context.Context, timeout, idleTimeout time.Duration) error {
	if err := os.MkdirAll(a.cfg.StateDir, 0o700); err != nil {
		return failf(ExitOperational, "create state dir: %w", err)
	}

	// Serialize concurrent `up` invocations. Everything below — the health
	// probe, stale socket removal, and spawn — must happen under this lock
	// or two callers can both conclude nothing is running.
	startLock, err := daemon.AcquireWait(a.cfg.StartLockPath(), startLockWait)
	if err != nil {
		return failf(ExitOperational, "acquire start lock: %w", err)
	}
	defer func() { _ = startLock.Release() }()

	client := a.client()

	if h, err := client.Health(ctx); err == nil {
		a.printf("daemon already running (pid %d)\n", h.PID)
		return nil
	} else if !errors.Is(err, api.ErrNoDaemon) {
		return failf(ExitOperational, "probe daemon: %w", err)
	}

	// Nothing answered. A socket file present now is stale: no live daemon
	// is behind it, and we hold the start lock so none can appear.
	if err := os.Remove(a.cfg.Socket); err != nil && !os.IsNotExist(err) {
		return failf(ExitOperational, "remove stale socket: %w", err)
	}

	pid, err := daemon.Spawn(a.cfg, idleTimeout)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	h, err := daemon.WaitReady(ctx, client, timeout)
	if err != nil {
		// Do not leave a half-started process behind.
		_, _ = daemon.Stop(pid, a.cfg, 2*time.Second)
		if tail := strings.TrimSpace(daemon.LogTail(a.cfg, 4096)); tail != "" {
			fmt.Fprintf(a.errOut, "daemon log:\n%s\n", tail)
		}
		return failf(ExitOperational, "%w", err)
	}

	a.printf("daemon started (pid %d)\n", h.PID)
	return nil
}

func (a *app) printf(format string, args ...any) {
	fmt.Fprintf(a.out, format, args...)
}
