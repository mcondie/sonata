package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/api"
	"github.com/mcondie/sonata/internal/workflow"
)

func newActionCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "action",
		Short: "Register and inspect actions",
	}
	cmd.AddCommand(
		newActionApplyCmd(a),
		newActionListCmd(a),
		newActionShowCmd(a),
		newActionEnableCmd(a, true),
		newActionEnableCmd(a, false),
	)
	return cmd
}

func newActionApplyCmd(a *app) *cobra.Command {
	var file string

	cmd := &cobra.Command{
		Use:   "apply -f <file>",
		Short: "Register an action definition, creating a new version",
		Long: "Validate a definition and store it as the next version of the action.\n" +
			"Reads YAML (or JSON) from a file, or JSON from stdin with -f -.\n" +
			"Re-applying an unchanged definition is a no-op.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.actionApply(cmd.Context(), file, cmd.InOrStdin())
		},
	}
	cmd.Flags().StringVarP(&file, "file", "f", "", "definition file, or - for JSON on stdin")
	return cmd
}

func (a *app) actionApply(ctx context.Context, file string, stdin io.Reader) error {
	if file == "" {
		return failf(ExitUsage, "-f is required: give a definition file, or - for JSON on stdin")
	}

	var (
		data []byte
		err  error
		def  *workflow.Action
	)
	if file == "-" {
		if data, err = io.ReadAll(stdin); err != nil {
			return failf(ExitOperational, "read stdin: %w", err)
		}
		if len(strings.TrimSpace(string(data))) == 0 {
			return failf(ExitUsage, "no definition on stdin")
		}
		def, err = workflow.ParseJSON(data)
	} else {
		if data, err = os.ReadFile(file); err != nil {
			return failf(ExitOperational, "read %s: %w", file, err)
		}
		// YAML is a JSON superset, so a .json file parses on this path too.
		def, err = workflow.ParseYAML(data)
	}
	if err != nil {
		// Client-side parsing exists to fail fast with the file name attached;
		// the daemon validates again regardless.
		return failf(ExitUsage, "%s: %w", displayPath(file), err)
	}

	canonical, err := def.Canonical()
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	resp, err := client.ApplyAction(ctx, &api.ApplyActionRequest{Definition: canonical})
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(resp)
	}
	if !resp.Changed {
		a.printf("%s unchanged (version %d)\n", resp.Name, resp.Version)
		return nil
	}
	if resp.Enabled {
		a.printf("%s version %d\n", resp.Name, resp.Version)
	} else {
		a.printf("%s version %d (disabled)\n", resp.Name, resp.Version)
	}
	return nil
}

// displayPath keeps the error prefix short for files in the working directory
// and readable for "-".
func displayPath(file string) string {
	if file == "-" {
		return "stdin"
	}
	return filepath.Clean(file)
}

func newActionListCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List actions with their current version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.actionList(cmd.Context())
		},
	}
}

func (a *app) actionList(ctx context.Context) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	resp, err := client.ListActions(ctx)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(resp)
	}
	if len(resp.Actions) == 0 {
		a.printf("no actions\n")
		return nil
	}
	w := tabwriter.NewWriter(a.out, 2, 8, 2, ' ', 0)
	fmt.Fprintln(w, "NAME\tVERSION\tENABLED\tINPUTS\tOUTPUT\tCREATED")
	for _, act := range resp.Actions {
		output := act.Output
		if output == "" {
			output = "-"
		}
		fmt.Fprintf(w, "%s\t%d\t%t\t%s\t%s\t%s\n",
			act.Name, act.Version, act.Enabled,
			strings.Join(act.Inputs, ","), output,
			act.CreatedAt.Local().Format(time.RFC3339))
	}
	return w.Flush()
}

func newActionShowCmd(a *app) *cobra.Command {
	var version int64

	cmd := &cobra.Command{
		Use:   "show <name>",
		Short: "Show a stored action definition",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.actionShow(cmd.Context(), args[0], version)
		},
	}
	cmd.Flags().Int64Var(&version, "version", 0, "version to show (default: the current one)")
	return cmd
}

func (a *app) actionShow(ctx context.Context, name string, version int64) error {
	if version < 0 {
		return failf(ExitUsage, "invalid --version %d: want a positive version", version)
	}
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	act, err := client.ShowAction(ctx, name, version)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}
	// The definition is structured data either way; the table format prints
	// the same document.
	return a.writeJSON(act)
}

func newActionEnableCmd(a *app, enable bool) *cobra.Command {
	verb, short := "disable", "Stop an action from accruing new work"
	if enable {
		verb, short = "enable", "Let an action accrue work again"
	}
	return &cobra.Command{
		Use:   verb + " <name>",
		Short: short,
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.actionSetEnabled(cmd.Context(), args[0], enable)
		},
	}
}

func (a *app) actionSetEnabled(ctx context.Context, name string, enabled bool) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	resp, err := client.SetActionEnabled(ctx, name, enabled)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(resp)
	}
	state := "disabled"
	if resp.Enabled {
		state = "enabled"
	}
	a.printf("%s %s (version %d)\n", resp.Name, state, resp.Version)
	return nil
}
