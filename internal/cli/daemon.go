package cli

import (
	"time"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/daemon"
)

func newDaemonCmd(a *app) *cobra.Command {
	var idleTimeout time.Duration

	cmd := &cobra.Command{
		Use:   "daemon",
		Short: "Run the daemon in the foreground",
		Long: "Run the daemon in the foreground. It never forks, so this is the\n" +
			"entrypoint for systemd, launchd, or watching logs directly.\n" +
			"Use `sonata up` to start a detached daemon.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if !cmd.Flags().Changed("idle-timeout") {
				idleTimeout = a.cfg.IdleTimeout
			}
			err := daemon.Run(cmd.Context(), daemon.Options{
				Config:      a.cfg,
				IdleTimeout: idleTimeout,
				Version:     a.bi.Version,
				Log:         a.logger(),
			})
			if err != nil {
				return failf(ExitOperational, "%w", err)
			}
			return nil
		},
	}
	cmd.Flags().DurationVar(&idleTimeout, "idle-timeout", 0,
		"stop after this long without a request (0 disables)")
	return cmd
}
