package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/mcondie/sonata/internal/workflow"
)

func actionDef(t *testing.T, command string) json.RawMessage {
	t.Helper()
	src := fmt.Sprintf(`{
		"name": "close-order",
		"inputs": [{"queue": "invoices.approved", "filter": "payload.total > 0"}],
		"actor": "subprocess",
		"instructions": {"command": [%q]},
		"output": "orders.closed"
	}`, command)
	a, err := workflow.ParseJSON([]byte(src))
	if err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	canonical, err := a.Canonical()
	if err != nil {
		t.Fatalf("canonicalize fixture: %v", err)
	}
	return canonical
}

func TestActionEndpointsRoundTrip(t *testing.T) {
	client := startServer(t, testStore(t))
	ctx := context.Background()

	v1, err := client.ApplyAction(ctx, &ApplyActionRequest{Definition: actionDef(t, "./one.sh")})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if v1.Name != "close-order" || v1.Version != 1 || !v1.Changed || !v1.Enabled {
		t.Fatalf("first apply: %+v", v1)
	}

	same, err := client.ApplyAction(ctx, &ApplyActionRequest{Definition: actionDef(t, "./one.sh")})
	if err != nil {
		t.Fatalf("re-apply: %v", err)
	}
	if same.Changed || same.Version != 1 {
		t.Fatalf("re-apply should be a no-op: %+v", same)
	}

	v2, err := client.ApplyAction(ctx, &ApplyActionRequest{Definition: actionDef(t, "./two.sh")})
	if err != nil {
		t.Fatalf("edited apply: %v", err)
	}
	if !v2.Changed || v2.Version != 2 {
		t.Fatalf("edited apply: %+v", v2)
	}

	// Both versions remain retrievable, and version 0 means "current".
	for _, tc := range []struct {
		version int64
		want    string
	}{{1, "./one.sh"}, {2, "./two.sh"}, {0, "./two.sh"}} {
		got, err := client.ShowAction(ctx, "close-order", tc.version)
		if err != nil {
			t.Fatalf("show v%d: %v", tc.version, err)
		}
		if !strings.Contains(string(got.Definition), tc.want) {
			t.Fatalf("show v%d definition = %s, want %s", tc.version, got.Definition, tc.want)
		}
	}

	list, err := client.ListActions(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Actions) != 1 {
		t.Fatalf("listed %d actions, want 1", len(list.Actions))
	}
	got := list.Actions[0]
	if got.Version != 2 || !got.Enabled || got.Actor != "subprocess" || got.Output != "orders.closed" {
		t.Fatalf("summary = %+v", got)
	}
	if len(got.Inputs) != 1 || got.Inputs[0] != "invoices.approved" {
		t.Fatalf("summary inputs = %v", got.Inputs)
	}
}

// Apply must not re-enable a disabled action, and only an explicit enable
// call may flip it back.
func TestActionEnableDisableRoundTrip(t *testing.T) {
	client := startServer(t, testStore(t))
	ctx := context.Background()

	if _, err := client.ApplyAction(ctx, &ApplyActionRequest{Definition: actionDef(t, "./one.sh")}); err != nil {
		t.Fatalf("apply: %v", err)
	}
	off, err := client.SetActionEnabled(ctx, "close-order", false)
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if off.Enabled {
		t.Fatalf("disable: %+v", off)
	}

	applied, err := client.ApplyAction(ctx, &ApplyActionRequest{Definition: actionDef(t, "./two.sh")})
	if err != nil {
		t.Fatalf("apply v2: %v", err)
	}
	if applied.Version != 2 || applied.Enabled {
		t.Fatalf("apply on a disabled action: %+v", applied)
	}

	on, err := client.SetActionEnabled(ctx, "close-order", true)
	if err != nil {
		t.Fatalf("enable: %v", err)
	}
	if !on.Enabled || on.Version != 2 {
		t.Fatalf("enable: %+v", on)
	}
}

// The daemon re-validates whatever it is handed: the CLI is one caller among
// several and ad-hoc JSON callers post here directly.
func TestActionApplyRejectsInvalidDefinitions(t *testing.T) {
	client := startServer(t, testStore(t))
	ctx := context.Background()

	cases := map[string]string{
		"missing name":  `{"actor":"subprocess","inputs":[{"queue":"q"}],"instructions":{"command":["./x"]}}`,
		"unknown actor": `{"name":"a","actor":"rocket","inputs":[{"queue":"q"}],"instructions":{"command":["./x"]}}`,
		"self loop":     `{"name":"a","actor":"subprocess","inputs":[{"queue":"q"}],"output":"q","instructions":{"command":["./x"]}}`,
		"bad filter":    `{"name":"a","actor":"subprocess","inputs":[{"queue":"q","filter":"payload >"}],"instructions":{"command":["./x"]}}`,
		"unknown field": `{"name":"a","actor":"subprocess","inputs":[{"queue":"q"}],"instructions":{"command":["./x"]},"concurency":2}`,
	}
	for name, src := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := client.ApplyAction(ctx, &ApplyActionRequest{Definition: json.RawMessage(src)})
			if err == nil {
				t.Fatal("expected a rejection")
			}
			var apiErr *Error
			if !errors.As(err, &apiErr) {
				t.Fatalf("error is not a server error: %v", err)
			}
			if apiErr.Code != "invalid_action" {
				t.Fatalf("code = %q, want invalid_action (message %q)", apiErr.Code, apiErr.Message)
			}
			if apiErr.Message == "" || strings.Contains(apiErr.Message, "\n") {
				t.Fatalf("message is not one actionable line: %q", apiErr.Message)
			}
		})
	}

	if _, err := client.ApplyAction(ctx, &ApplyActionRequest{}); err == nil {
		t.Fatal("expected a rejection for a missing definition")
	}
}

func TestActionShowNotFound(t *testing.T) {
	client := startServer(t, testStore(t))
	ctx := context.Background()

	_, err := client.ShowAction(ctx, "missing", 0)
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Code != "not_found" {
		t.Fatalf("show missing: %v", err)
	}
	if _, err := client.SetActionEnabled(ctx, "missing", true); !errors.As(err, &apiErr) || apiErr.Code != "not_found" {
		t.Fatalf("enable missing: %v", err)
	}
	if _, err := client.ShowAction(ctx, "", 0); !errors.As(err, &apiErr) || apiErr.Code != "invalid_request" {
		t.Fatalf("show without a name: %v", err)
	}
}

func TestActionListEmpty(t *testing.T) {
	client := startServer(t, testStore(t))
	resp, err := client.ListActions(context.Background())
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	// An empty listing must serialize as [] rather than null so `--output
	// json` stays parseable by scripts.
	if resp.Actions == nil || len(resp.Actions) != 0 {
		t.Fatalf("empty listing = %#v", resp.Actions)
	}
}

// Concurrent applies through the HTTP surface exercise the same serializer the
// store test covers, one layer up: every caller gets a distinct dense version.
func TestActionApplyConcurrent(t *testing.T) {
	client := startServer(t, testStore(t))
	ctx := context.Background()

	const n = 8
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		versions = map[int64]int{}
	)
	for i := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.ApplyAction(ctx, &ApplyActionRequest{
				Definition: actionDef(t, fmt.Sprintf("./%d.sh", i)),
			})
			if err != nil {
				t.Errorf("apply %d: %v", i, err)
				return
			}
			mu.Lock()
			versions[resp.Version]++
			mu.Unlock()
		}()
	}
	wg.Wait()

	if len(versions) != n {
		t.Fatalf("got %d distinct versions, want %d: %v", len(versions), n, versions)
	}
	for v := int64(1); v <= n; v++ {
		if versions[v] != 1 {
			t.Fatalf("version %d issued %d times", v, versions[v])
		}
	}
}
