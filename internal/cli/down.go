package cli

import (
	"context"
	"errors"
	"time"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/daemon"
)

func newDownCmd(a *app) *cobra.Command {
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "down",
		Short: "Stop the daemon, waiting until it is gone",
		Long: "Stop the daemon and block until the process has exited and the\n" +
			"socket has been removed. Idempotent: exits 0 if nothing was running.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.down(cmd.Context(), timeout)
		},
	}
	cmd.Flags().DurationVar(&timeout, "timeout", 10*time.Second, "how long to wait for a graceful stop before killing")
	return cmd
}

func (a *app) down(ctx context.Context, timeout time.Duration) error {
	pid, running, err := a.resolveDaemonPID(ctx)
	if err != nil {
		return err
	}
	if !running {
		// Unlike `status`, absence is success: the requested end state holds.
		a.printf("no daemon running\n")
		return nil
	}

	killed, err := daemon.Stop(pid, a.cfg, timeout)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}
	if killed {
		a.printf("daemon killed (pid %d) after %s\n", pid, timeout)
		return &exitError{
			code: ExitOperational,
			err:  errors.New("daemon did not stop gracefully; sent SIGKILL"),
		}
	}

	a.printf("daemon stopped (pid %d)\n", pid)
	return nil
}

// resolveDaemonPID finds the running daemon's PID.
//
// Health is authoritative and preferred: the PID comes from the process that
// actually answered. The lock is the fallback for a daemon that is alive but
// not serving — wedged, or still starting — which health alone cannot
// distinguish from absence.
func (a *app) resolveDaemonPID(ctx context.Context) (pid int, running bool, err error) {
	h, herr := a.client().Health(ctx)
	if herr == nil {
		return h.PID, true, nil
	}
	if !errors.Is(herr, api.ErrNoDaemon) {
		return 0, false, failf(ExitOperational, "probe daemon: %w", herr)
	}

	lockPID, held := daemon.RunningPID(a.cfg.LockPath())
	if !held {
		return 0, false, nil
	}
	if lockPID <= 0 {
		return 0, false, failf(ExitOperational,
			"a daemon holds %s but its pid is unreadable", a.cfg.LockPath())
	}
	return lockPID, true, nil
}
