package cli

import (
	"context"
	"encoding/json"
	"io"
	"strings"

	"github.com/spf13/cobra"

	"github.com/mcondie/sonata/internal/api"
)

func newSendCmd(a *app) *cobra.Command {
	var headers []string

	cmd := &cobra.Command{
		Use:   "send <queue>",
		Short: "Append a message to a queue",
		Long: "Append a JSON message read from stdin to a queue, starting the\n" +
			"daemon if none is running. Prints the new message id.",
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return a.send(cmd.Context(), args[0], headers, cmd.InOrStdin())
		},
	}
	cmd.Flags().StringArrayVar(&headers, "header", nil, "message header as key=value (repeatable)")
	return cmd
}

func (a *app) send(ctx context.Context, queue string, headerFlags []string, stdin io.Reader) error {
	payload, err := io.ReadAll(stdin)
	if err != nil {
		return failf(ExitOperational, "read stdin: %w", err)
	}
	if len(strings.TrimSpace(string(payload))) == 0 {
		return failf(ExitUsage, "no payload on stdin: pipe a JSON document, e.g. echo '{}' | sonata send %s", queue)
	}
	if !json.Valid(payload) {
		return failf(ExitUsage, "payload is not valid JSON")
	}

	headers := make(map[string]any, len(headerFlags))
	for _, h := range headerFlags {
		k, v, ok := strings.Cut(h, "=")
		if !ok || k == "" {
			return failf(ExitUsage, "invalid --header %q: want key=value", h)
		}
		headers[k] = v
	}

	client, err := a.ensureClient(ctx)
	if err != nil {
		return err
	}
	resp, err := client.SendMessage(ctx, &api.SendMessageRequest{
		Queue:   queue,
		Payload: payload,
		Headers: headers,
	})
	if err != nil {
		return failf(ExitOperational, "%w", err)
	}

	if a.jsonOutput() {
		return a.writeJSON(resp)
	}
	a.printf("%s\n", resp.ID)
	return nil
}
