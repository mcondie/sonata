package api

import (
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
