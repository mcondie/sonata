package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	"github.com/mcondie/sonata/internal/store"
)

// startServer serves an in-process API over a real unix socket and returns a
// client for it. Socket lives under /tmp: macOS t.TempDir() paths overrun
// sun_path.
func startServer(t *testing.T, st *store.Store) *Client {
	t.Helper()
	sockDir, err := os.MkdirTemp("/tmp", "sn")
	if err != nil {
		t.Fatalf("temp socket dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(sockDir) })

	socket := filepath.Join(sockDir, "s.sock")
	ln, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &http.Server{Handler: NewServer(ServerOptions{
		Store: st,
		Log:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	return NewClient(socket)
}

func testStore(t *testing.T) *store.Store {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
}

func TestMessageEndpointsRoundTrip(t *testing.T) {
	client := startServer(t, testStore(t))
	ctx := context.Background()

	sent, err := client.SendMessage(ctx, &SendMessageRequest{
		Queue:   "demo",
		Payload: json.RawMessage(`{"a":1}`),
		Headers: map[string]any{"source": "test"},
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	if sent.ID == "" || sent.TraceID == "" || sent.CreatedAt.IsZero() {
		t.Fatalf("send response incomplete: %+v", sent)
	}

	// show returns the full message with ingress stamping applied.
	m, err := client.ShowMessage(ctx, sent.ID)
	if err != nil {
		t.Fatalf("show: %v", err)
	}
	if m.Queue != "demo" || m.TraceID != sent.TraceID || m.OriginAction != nil {
		t.Errorf("message = %+v", m)
	}
	var headers map[string]any
	if err := json.Unmarshal(m.Headers, &headers); err != nil {
		t.Fatalf("headers: %v", err)
	}
	if headers["hops"] != float64(0) || headers["source"] != "test" {
		t.Errorf("headers = %v, want hops 0 and source test", headers)
	}

	// list filters by queue and by trace.
	list, err := client.ListMessages(ctx, &ListMessagesRequest{Queue: "demo"})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Messages) != 1 || list.Messages[0].ID != sent.ID {
		t.Errorf("list by queue = %+v", list.Messages)
	}
	list, err = client.ListMessages(ctx, &ListMessagesRequest{TraceID: sent.TraceID})
	if err != nil {
		t.Fatalf("list by trace: %v", err)
	}
	if len(list.Messages) != 1 {
		t.Errorf("list by trace = %+v", list.Messages)
	}

	queues, err := client.ListQueues(ctx)
	if err != nil {
		t.Fatalf("queue list: %v", err)
	}
	if len(queues.Queues) != 1 || queues.Queues[0] != (QueueInfo{Name: "demo", Messages: 1}) {
		t.Errorf("queues = %+v", queues.Queues)
	}
}

func TestShowMessageNotFound(t *testing.T) {
	client := startServer(t, testStore(t))

	_, err := client.ShowMessage(context.Background(), "does-not-exist")
	var apiErr *Error
	if !errors.As(err, &apiErr) {
		t.Fatalf("err = %v, want *api.Error", err)
	}
	if apiErr.Code != "not_found" {
		t.Errorf("code = %q, want not_found", apiErr.Code)
	}
}

func TestSendMessageRejectsInvalid(t *testing.T) {
	client := startServer(t, testStore(t))
	ctx := context.Background()

	cases := []struct {
		name string
		req  *SendMessageRequest
	}{
		{"missing queue", &SendMessageRequest{Payload: json.RawMessage(`{}`)}},
		{"empty payload", &SendMessageRequest{Queue: "q"}},
	}
	for _, tc := range cases {
		_, err := client.SendMessage(ctx, tc.req)
		var apiErr *Error
		if !errors.As(err, &apiErr) {
			t.Fatalf("%s: err = %v, want *api.Error", tc.name, err)
		}
		if apiErr.Code != "invalid_request" {
			t.Errorf("%s: code = %q, want invalid_request", tc.name, apiErr.Code)
		}
	}

	// A payload that is not valid JSON cannot be built through the typed
	// client (RawMessage marshalling rejects it), so drive the wire directly.
	hc := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", client.Socket())
		},
	}}
	resp, err := hc.Post("http://sonata/v1/message.send", "application/json",
		bytes.NewReader([]byte(`{"queue":"q","payload":{nope}`)))
	if err != nil {
		t.Fatalf("raw post: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("raw invalid payload: status %d, want 400", resp.StatusCode)
	}
	var e ErrorResponse
	if err := json.NewDecoder(resp.Body).Decode(&e); err != nil || e.Error.Code != "invalid_request" {
		t.Errorf("raw invalid payload: envelope %+v, err %v", e, err)
	}
}
