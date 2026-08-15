package cli

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/daemon"
)

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
			if !cmd.Flags().Changed("idle-timeout") {
				idleTimeout = a.cfg.IdleTimeout
			}
			return a.up(cmd.Context(), timeout, idleTimeout)
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "how long to wait for the daemon to become ready")
	cmd.Flags().DurationVar(&idleTimeout, "idle-timeout", 0, "stop the daemon after this long without a request (0 disables)")
	return cmd
}

func (a *app) up(ctx context.Context, timeout, idleTimeout time.Duration) error {
	h, started, err := daemon.EnsureRunning(ctx, a.cfg, daemon.EnsureOptions{
		ReadyTimeout: timeout,
		IdleTimeout:  idleTimeout,
	})
	if err != nil {
		// A spawned daemon that never answered leaves its diagnosis in the
		// log file, not in the error.
		var se *daemon.StartError
		if errors.As(err, &se) && se.LogTail != "" {
			fmt.Fprintf(a.errOut, "daemon log:\n%s\n", se.LogTail)
		}
		return failf(ExitOperational, "%w", err)
	}

	if started {
		a.printf("daemon started (pid %d)\n", h.PID)
	} else {
		a.printf("daemon already running (pid %d)\n", h.PID)
	}
	return nil
}

func (a *app) printf(format string, args ...any) {
	fmt.Fprintf(a.out, format, args...)
}
