package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/mcondie/sonata/internal/executor"
	"github.com/mcondie/sonata/internal/store"
	"github.com/mcondie/sonata/internal/workflow"
)

// TestRealPipelineEndToEnd runs two real subprocess actions in-process: a
// shell script that transforms its input envelope feeds a second action via
// the output queue. This is the layer-2 version of the daemon's whole job.
func TestRealPipelineEndToEnd(t *testing.T) {
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })

	sched := New(Options{
		Store:    st,
		Executor: executor.Subprocess{},
		Clock:    RealClock{},
		Config: Config{
			MaxHops:            100,
			DefaultMaxAttempts: 3,
			DefaultTaskTimeout: 30 * time.Second,
			BackoffBase:        10 * time.Millisecond,
			BackoffCap:         100 * time.Millisecond,
			StdoutCap:          1 << 20,
		},
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sched.Run(ctx) }()
	<-sched.Ready()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	apply := func(def string) {
		t.Helper()
		a, err := workflow.ParseJSON([]byte(def))
		if err != nil {
			t.Fatal(err)
		}
		canonical, _ := a.Canonical()
		if _, _, err := st.ApplyAction(context.Background(), a.Name, canonical); err != nil {
			t.Fatal(err)
		}
	}
	// double reads the envelope, extracts payload.n with a shell-side JSON
	// poke (jq-free: the value is the only digit run), and emits n*2.
	apply(`{"name":"double","inputs":[{"queue":"nums"}],"actor":"subprocess",
		"instructions":{"command":["sh","-c",
			"read line; n=$(printf %s \"$line\" | sed 's/.*\\\"n\\\"://;s/[^0-9].*//'); echo {\\\"n\\\":$((n*2))}"]},
		"output":"doubled"}`)
	apply(`{"name":"stamp","inputs":[{"queue":"doubled"}],"actor":"subprocess",
		"instructions":{"command":["sh","-c","read line; echo {\\\"seen\\\":true}"]},
		"output":"stamped"}`)

	m := &store.Message{
		ID:      store.NewID(),
		Queue:   "nums",
		Payload: json.RawMessage(`{"n":21}`),
		Headers: json.RawMessage(`{"hops":0}`),
		TraceID: store.NewID(), CreatedAt: time.Now().UTC(),
	}
	n, err := st.AppendMessage(context.Background(), m)
	if err != nil {
		t.Fatal(err)
	}
	sched.WorkAdded(n)

	deadline := time.Now().Add(15 * time.Second)
	for {
		msgs, err := st.ListMessages(context.Background(), store.ListOptions{Queue: "stamped"})
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) == 1 {
			if msgs[0].TraceID != m.TraceID {
				t.Errorf("trace lost across two real subprocesses")
			}
			break
		}
		if time.Now().After(deadline) {
			ds, _ := st.ListDeliveries(context.Background(), store.DeliveryListOptions{})
			t.Fatalf("pipeline never finished; deliveries: %s", dumpDeliveries(ds))
		}
		time.Sleep(10 * time.Millisecond)
	}

	mid, _ := st.ListMessages(context.Background(), store.ListOptions{Queue: "doubled"})
	if len(mid) != 1 || string(mid[0].Payload) != `{"n":42}` {
		t.Errorf("intermediate payload: %+v", mid)
	}
}

func dumpDeliveries(ds []*store.Delivery) string {
	out := ""
	for _, d := range ds {
		e := ""
		if d.Error != nil {
			e = *d.Error
		}
		st := ""
		if d.StderrTail != nil {
			st = *d.StderrTail
		}
		out += fmt.Sprintf("\n  %s %s attempt=%d err=%q stderr=%q", d.ActionName, d.State, d.Attempt, e, st)
	}
	return out
}
