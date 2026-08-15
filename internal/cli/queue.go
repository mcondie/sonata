package cli

import (
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"
)

func newQueueCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "queue",
		Short: "Inspect queues",
	}
	cmd.AddCommand(newQueueListCmd(a))
	return cmd
}

func newQueueListCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "list",
		Short: "List queues and their message counts",
		Long: "List every queue any message has referenced. Queues are not\n" +
			"declared; they exist by being named.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.queueList(cmd.Context())
		},
	}
}

func (a *app) queueList(ctx context.Context) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	resp, err := client.ListQueues(ctx)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(resp)
	}
	if len(resp.Queues) == 0 {
		a.printf("no queues\n")
		return nil
	}
	w := tabwriter.NewWriter(a.out, 2, 8, 2, ' ', 0)
	fmt.Fprintln(w, "QUEUE\tMESSAGES")
	for _, q := range resp.Queues {
		fmt.Fprintf(w, "%s\t%d\n", q.Name, q.Messages)
	}
	return w.Flush()
}
