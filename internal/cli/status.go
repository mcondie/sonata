package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/api"
)

// statusJSON is the --output json shape for `sonata status`.
type statusJSON struct {
	Running bool `json:"running"`
	PID     int  `json:"pid,omitempty"`
	// Not omitempty: a daemon in its first second has uptime 0, and the
	// field disappearing would read as null to a consumer.
	UptimeS int64  `json:"uptime_s"`
	Version string `json:"version,omitempty"`
	Socket  string `json:"socket"`
}

func newStatusCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Show daemon state and PID",
		Long: "Report whether the daemon is running.\n" +
			"Exits 1 when it is not. This command never starts a daemon.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.status(cmd.Context())
		},
	}
}

func (a *app) status(ctx context.Context) error {
	h, err := a.client().Health(ctx)
	if err != nil {
		if !errors.Is(err, api.ErrNoDaemon) {
			return failf(ExitOperational, "%w", err)
		}
		if a.jsonOutput() {
			if err := a.writeJSON(statusJSON{Running: false, Socket: a.cfg.Socket}); err != nil {
				return err
			}
		}
		// Absence is reported on stderr with a non-zero exit: `status` is a
		// query, so "not running" is a negative result, not a success.
		return &exitError{
			code: ExitNotRunning,
			err:  fmt.Errorf("no daemon running at %s", a.cfg.Socket),
		}
	}

	if a.jsonOutput() {
		return a.writeJSON(statusJSON{
			Running: true,
			PID:     h.PID,
			UptimeS: int64(h.Uptime().Seconds()),
			Version: h.Version,
			Socket:  a.cfg.Socket,
		})
	}

	a.printf("running  pid %d  uptime %s  version %s  socket %s\n",
		h.PID, h.Uptime(), h.Version, a.cfg.Socket)
	return nil
}

func (a *app) writeJSON(v any) error {
	enc := json.NewEncoder(a.out)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return failf(ExitOperational, "encode output: %w", err)
	}
	return nil
}
