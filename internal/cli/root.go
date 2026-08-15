// Package cli implements Sonata's command tree.
package cli

import (
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/config"
)

// Exit codes are part of the CLI contract — the E2E scripts branch on them.
const (
	ExitOK          = 0 // success
	ExitNotRunning  = 1 // expected negative outcome: no daemon
	ExitUsage       = 2 // bad flag or argument
	ExitOperational = 3 // spawn failed, timeout, socket unusable
)

// BuildInfo is stamped in at link time.
type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

// exitError carries an exit code out of a command.
type exitError struct {
	code int
	err  error
}

func (e *exitError) Error() string { return e.err.Error() }
func (e *exitError) Unwrap() error { return e.err }

func failf(code int, format string, args ...any) error {
	return &exitError{code: code, err: fmt.Errorf(format, args...)}
}

// app holds state shared by the commands.
type app struct {
	bi      BuildInfo
	v       *viper.Viper
	cfg     *config.Config
	cfgFile string
	output  string
	verbose bool

	out    io.Writer
	errOut io.Writer
}

func (a *app) client() *api.Client { return api.NewClient(a.cfg.Socket) }

func (a *app) logger() *slog.Logger {
	level := slog.LevelInfo
	if a.verbose {
		level = slog.LevelDebug
	} else if a.cfg != nil {
		switch a.cfg.LogLevel {
		case "debug":
			level = slog.LevelDebug
		case "warn":
			level = slog.LevelWarn
		case "error":
			level = slog.LevelError
		}
	}
	return slog.New(slog.NewTextHandler(a.errOut, &slog.HandlerOptions{Level: level}))
}

func (a *app) jsonOutput() bool { return a.output == "json" }

// Execute runs the command tree and returns a process exit code.
func Execute(bi BuildInfo) int {
	a := &app{bi: bi, v: config.New(), out: os.Stdout, errOut: os.Stderr}
	root := newRootCmd(a)

	if err := root.Execute(); err != nil {
		var ee *exitError
		if errors.As(err, &ee) {
			fmt.Fprintf(a.errOut, "sonata: %v\n", ee.err)
			return ee.code
		}
		fmt.Fprintf(a.errOut, "sonata: %v\n", err)
		return ExitOperational
	}
	return ExitOK
}

func newRootCmd(a *app) *cobra.Command {
	root := &cobra.Command{
		Use:           "sonata",
		Short:         "Local workflow orchestration",
		Version:       a.bi.Version,
		SilenceUsage:  true,
		SilenceErrors: true,
		PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
			if err := config.Bind(a.v, cmd.Root().PersistentFlags()); err != nil {
				return failf(ExitOperational, "%w", err)
			}
			cfg, err := config.Load(a.v, a.cfgFile)
			if err != nil {
				return failf(ExitOperational, "%w", err)
			}
			a.cfg = cfg
			if a.output != "table" && a.output != "json" {
				return failf(ExitUsage, "invalid --output %q: want table or json", a.output)
			}
			return nil
		},
	}

	pf := root.PersistentFlags()
	pf.StringVar(&a.cfgFile, "config", "", "config file (default: ./sonata.yaml or $XDG_CONFIG_HOME/sonata/config.yaml)")
	pf.String("state-dir", "", "state directory (default: $XDG_STATE_HOME/sonata)")
	pf.String("socket", "", "daemon socket path (default: <state-dir>/sonata.sock)")
	pf.String("database", "", "database path (default: <state-dir>/sonata.db)")
	pf.String("log-level", "", "log level: debug|info|warn|error")
	pf.StringVar(&a.output, "output", "table", "output format: table|json")
	pf.BoolVar(&a.verbose, "verbose", false, "verbose logging")

	root.SetOut(a.out)
	root.SetErr(a.errOut)

	root.AddCommand(
		newUpCmd(a),
		newDownCmd(a),
		newStatusCmd(a),
		newDaemonCmd(a),
		newSendCmd(a),
		newMessageCmd(a),
		newQueueCmd(a),
		newActionCmd(a),
	)

	// Cobra reports flag parse errors through this hook; map them to the
	// usage exit code rather than the generic operational one.
	root.SetFlagErrorFunc(func(_ *cobra.Command, err error) error {
		return failf(ExitUsage, "%w", err)
	})

	return root
}
