package api

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Client talks to the daemon over a Unix domain socket.
type Client struct {
	socket string
	hc     *http.Client
}

// NewClient returns a client dialling the given socket path.
func NewClient(socket string) *Client {
	return &Client{
		socket: socket,
		hc: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "unix", socket)
				},
				DisableKeepAlives: true,
			},
			Timeout: 10 * time.Second,
		},
	}
}

// Socket returns the path this client dials.
func (c *Client) Socket() string { return c.socket }

// post issues a POST /v1/<endpoint> call and decodes the response into out.
// Non-2xx responses come back as *Error carrying the server's code/message.
func (c *Client) post(ctx context.Context, endpoint string, in, out any) error {
	body, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("%s: encode request: %w", endpoint, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		"http://sonata/v1/"+endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.hc.Do(req)
	if err != nil {
		if isNoDaemon(err) {
			return fmt.Errorf("%w at %s", ErrNoDaemon, c.socket)
		}
		return fmt.Errorf("%s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		var e ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&e); err == nil && e.Error.Message != "" {
			return &Error{Code: e.Error.Code, Message: e.Error.Message}
		}
		return fmt.Errorf("%s: unexpected status %d", endpoint, resp.StatusCode)
	}

	if out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("%s: decode response: %w", endpoint, err)
	}
	return nil
}

// SendMessage appends a message to a queue.
func (c *Client) SendMessage(ctx context.Context, req *SendMessageRequest) (*SendMessageResponse, error) {
	var resp SendMessageResponse
	if err := c.post(ctx, "message.send", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListMessages returns messages newest-first.
func (c *Client) ListMessages(ctx context.Context, req *ListMessagesRequest) (*ListMessagesResponse, error) {
	var resp ListMessagesResponse
	if err := c.post(ctx, "message.list", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ShowMessage returns one message by id.
func (c *Client) ShowMessage(ctx context.Context, id string) (*Message, error) {
	var resp Message
	if err := c.post(ctx, "message.show", &ShowMessageRequest{ID: id}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListQueues returns every queue any message has referenced.
func (c *Client) ListQueues(ctx context.Context) (*ListQueuesResponse, error) {
	var resp ListQueuesResponse
	if err := c.post(ctx, "queue.list", &ListQueuesRequest{}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ApplyAction registers a definition, returning the version it landed on.
func (c *Client) ApplyAction(ctx context.Context, req *ApplyActionRequest) (*ApplyActionResponse, error) {
	var resp ApplyActionResponse
	if err := c.post(ctx, "action.apply", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListActions returns the current version of every registered action.
func (c *Client) ListActions(ctx context.Context) (*ListActionsResponse, error) {
	var resp ListActionsResponse
	if err := c.post(ctx, "action.list", &ListActionsRequest{}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ShowAction returns one action version; version 0 means the current one.
func (c *Client) ShowAction(ctx context.Context, name string, version int64) (*Action, error) {
	var resp Action
	if err := c.post(ctx, "action.show", &ShowActionRequest{Name: name, Version: version}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// SetActionEnabled enables or disables an action by name.
func (c *Client) SetActionEnabled(ctx context.Context, name string, enabled bool) (*SetActionEnabledResponse, error) {
	endpoint := "action.disable"
	if enabled {
		endpoint = "action.enable"
	}
	var resp SetActionEnabledResponse
	if err := c.post(ctx, endpoint, &SetActionEnabledRequest{Name: name}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ListDeliveries returns deliveries newest-first.
func (c *Client) ListDeliveries(ctx context.Context, req *ListDeliveriesRequest) (*ListDeliveriesResponse, error) {
	var resp ListDeliveriesResponse
	if err := c.post(ctx, "delivery.list", req, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ShowDelivery returns one delivery by id.
func (c *Client) ShowDelivery(ctx context.Context, id string) (*Delivery, error) {
	var resp Delivery
	if err := c.post(ctx, "delivery.show", &ShowDeliveryRequest{ID: id}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// ReplayDelivery resets a dead delivery to pending.
func (c *Client) ReplayDelivery(ctx context.Context, id string) (*Delivery, error) {
	var resp Delivery
	if err := c.post(ctx, "delivery.replay", &ReplayDeliveryRequest{ID: id}, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Health probes the daemon. It returns ErrNoDaemon when nothing is listening.
func (c *Client) Health(ctx context.Context) (*HealthResponse, error) {
	// The host in the URL is ignored by the Unix dialler but must be
	// syntactically valid.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://sonata/v1/health", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		if isNoDaemon(err) {
			return nil, fmt.Errorf("%w at %s", ErrNoDaemon, c.socket)
		}
		return nil, fmt.Errorf("health: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		var e ErrorResponse
		if err := json.NewDecoder(resp.Body).Decode(&e); err == nil && e.Error.Message != "" {
			return nil, fmt.Errorf("health: %s: %s", e.Error.Code, e.Error.Message)
		}
		return nil, fmt.Errorf("health: unexpected status %d", resp.StatusCode)
	}

	var h HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return nil, fmt.Errorf("decode health response: %w", err)
	}
	return &h, nil
}
