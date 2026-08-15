package api

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/mcondie/sonata/internal/store"
)

// fakeSched records SchedulerControl calls.
type fakeSched struct {
	mu        sync.Mutex
	workAdded int
	cancelled []string
}

func (f *fakeSched) WorkAdded(n int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.workAdded += n
}

func (f *fakeSched) CancelAction(_ context.Context, name string) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, name)
	return 1, nil
}

func TestDeliveryEndpoints(t *testing.T) {
	st := testStore(t)
	sched := &fakeSched{}
	client := startServerOpts(t, ServerOptions{Store: st, Scheduler: sched})
	ctx := context.Background()

	// Register a subscriber so send materializes a delivery, then kill it
	// store-side to have a dead row to replay.
	def := map[string]any{
		"name":         "act",
		"inputs":       []map[string]any{{"queue": "q"}},
		"actor":        "subprocess",
		"instructions": map[string]any{"command": []string{"./x"}},
	}
	b, _ := json.Marshal(def)
	if _, _, err := st.ApplyAction(ctx, "act", b); err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(ctx, &SendMessageRequest{Queue: "q", Payload: json.RawMessage(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if got := sched.workAdded; got != 1 {
		t.Errorf("send reported %d new deliveries to the scheduler, want 1", got)
	}

	list, err := client.ListDeliveries(ctx, &ListDeliveriesRequest{Action: "act"})
	if err != nil || len(list.Deliveries) != 1 {
		t.Fatalf("list: %+v %v", list, err)
	}
	id := list.Deliveries[0].ID

	shown, err := client.ShowDelivery(ctx, id)
	if err != nil || shown.State != store.StatePending {
		t.Fatalf("show: %+v %v", shown, err)
	}

	// Replay of a non-dead delivery is a 409 not_dead.
	_, err = client.ReplayDelivery(ctx, id)
	var apiErr *Error
	if !errors.As(err, &apiErr) || apiErr.Code != "not_dead" {
		t.Fatalf("replay pending: %v, want not_dead", err)
	}

	// Kill it, then replay for real.
	now := time.Now()
	if err := st.ClaimDelivery(ctx, id, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := st.FailDelivery(ctx, id, true, time.Time{}, "kaput", "", now); err != nil {
		t.Fatal(err)
	}
	replayed, err := client.ReplayDelivery(ctx, id)
	if err != nil || replayed.State != store.StatePending {
		t.Fatalf("replay dead: %+v %v", replayed, err)
	}
	if sched.workAdded != 2 {
		t.Errorf("replay did not report work to the scheduler")
	}

	// Disable routes through the scheduler's cancel.
	if _, err := client.SetActionEnabled(ctx, "act", false); err != nil {
		t.Fatal(err)
	}
	if len(sched.cancelled) != 1 || sched.cancelled[0] != "act" {
		t.Errorf("disable did not cancel via scheduler: %v", sched.cancelled)
	}

	// Not-found shape.
	_, err = client.ShowDelivery(ctx, "missing")
	if !errors.As(err, &apiErr) || apiErr.Code != "not_found" {
		t.Fatalf("show missing: %v", err)
	}
}
