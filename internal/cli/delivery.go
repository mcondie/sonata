package cli

import (
	"context"
	"fmt"
	"text/tabwriter"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/api"
)

func newDeliveryCmd(a *app) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "delivery",
		Short: "Inspect and replay deliveries",
		Long: "A delivery is one action's processing of one message.\n" +
			"`delivery list --state dead` is the dead-letter queue.",
	}
	cmd.AddCommand(newDeliveryListCmd(a), newDeliveryShowCmd(a), newDeliveryReplayCmd(a))
	return cmd
}

func newDeliveryListCmd(a *app) *cobra.Command {
	var req api.ListDeliveriesRequest

	cmd := &cobra.Command{
		Use:   "list",
		Short: "List deliveries, newest first",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return a.deliveryList(cmd.Context(), &req)
		},
	}
	cmd.Flags().StringVar(&req.Action, "action", "", "only deliveries of this action")
	cmd.Flags().StringVar(&req.State, "state", "", "only deliveries in this state")
	cmd.Flags().StringVar(&req.MessageID, "message", "", "only deliveries of this message id")
	cmd.Flags().IntVar(&req.Limit, "limit", 0, "maximum deliveries to return (default 50)")
	cmd.Flags().StringVar(&req.BeforeID, "before", "", "keyset cursor: only deliveries older than this id")
	return cmd
}

func (a *app) deliveryList(ctx context.Context, req *api.ListDeliveriesRequest) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	resp, err := client.ListDeliveries(ctx, req)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(resp)
	}
	if len(resp.Deliveries) == 0 {
		a.printf("no deliveries\n")
		return nil
	}
	w := tabwriter.NewWriter(a.out, 2, 8, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tACTION\tSTATE\tATTEMPT\tMESSAGE\tERROR")
	for _, d := range resp.Deliveries {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d\t%s\t%s\n",
			d.ID, d.ActionName, d.State, d.Attempt,
			strOr(d.MessageID, "-"), strOr(d.Error, ""))
	}
	return w.Flush()
}

func newDeliveryShowCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "show <id>",
		Short: "Show one delivery in full, including its stderr tail",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.deliveryShow(cmd.Context(), args[0])
		},
	}
}

func (a *app) deliveryShow(ctx context.Context, id string) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	d, err := client.ShowDelivery(ctx, id)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}
	return a.writeJSON(d)
}

func newDeliveryReplayCmd(a *app) *cobra.Command {
	return &cobra.Command{
		Use:   "replay <id>",
		Short: "Reset a dead delivery to run again",
		Long: "Reset a dead delivery to pending with a fresh attempt budget.\n" +
			"It executes under the action's current version — fix the action,\n" +
			"then replay.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.deliveryReplay(cmd.Context(), args[0])
		},
	}
}

func (a *app) deliveryReplay(ctx context.Context, id string) error {
	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	d, err := client.ReplayDelivery(ctx, id)
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(d)
	}
	a.printf("%s replaying (action %s)\n", d.ID, d.ActionName)
	return nil
}

func strOr(s *string, fallback string) string {
	if s == nil {
		return fallback
	}
	return *s
}
