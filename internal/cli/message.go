package cli

import (
	"context"
	"fmt"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/api"
)

func newMessageCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "message",
		Short: "Inspect messages",
	}
	cmd.AddCommand(newMessageListCmd(a), newMessageShowCmd(a))
	return cmd
}

func newMessageListCmd(a *app) *cobra.Command {
	var req api.ListMessagesRequest

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List messages, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.messageList(cmd.Context(), &req)
		},
	}
	cmd.Flags().StringVar(&req.Queue, "queue", "", "only messages on this queue")
	cmd.Flags().StringVar(&req.TraceID, "trace", "", "only messages with this trace id")
	cmd.Flags().IntVar(&req.Limit, "limit", 0, "maximum messages to return (default 50)")
	cmd.Flags().StringVar(&req.BeforeID, "before", "", "keyset cursor: only messages older than this id")
	return cmd
}

func (a *app) messageList(ctx context.Context, req *api.ListMessagesRequest) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	resp, err := client.ListMessages(ctx, req)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(resp)
	}
	if len(resp.Messages) == 0 {
		a.printf("no messages\n")
		return nil
	}
	w := tabwriter.NewWriter(a.out, 2, 8, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tQUEUE\tTRACE\tCREATED")
	for _, m := range resp.Messages {
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n",
			m.ID, m.Queue, m.TraceID, m.CreatedAt.Local().Format(time.RFC3339))
	}
	return w.Flush()
}

func newMessageShowCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Show one message in full",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.messageShow(cmd.Context(), args[0])
		},
	}
}

func (a *app) messageShow(ctx context.Context, id string) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	m, err := client.ShowMessage(ctx, id)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	// The full message is structured data either way; the table format just
	// pretty-prints the same JSON.
	return a.writeJSON(m)
}
