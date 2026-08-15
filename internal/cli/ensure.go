package cli

import (
	"context"
	"errors"
	"fmt"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/daemon"
)

// ensureClient returns a client backed by a running daemon, autostarting one
// if needed. Data commands use this; lifecycle commands (`status`, `down`,
// `daemon`) must never autostart and keep constructing clients directly.
func (a *app) ensureClient(ctx context.Context) (*api.Client, error) {
	_, started, err := daemon.EnsureRunning(ctx, a.cfg, daemon.EnsureOptions{
		IdleTimeout: a.cfg.IdleTimeout,
	})
	if err != nil {
		var se *daemon.StartError
		if errors.As(err, &se) && se.LogTail != "" {
			fmt.Fprintf(a.errOut, "daemon log:\n%s\n", se.LogTail)
		}
		return nil, failf(ExitOperational, "start daemon: %w", err)
	}
	if started {
		// Stderr, not stdout: this is a side note, and stdout must stay
		// parseable for piped and --output json callers.
		fmt.Fprintln(a.errOut, "sonata: daemon autostarted")
	}
	return a.client(), nil
}
