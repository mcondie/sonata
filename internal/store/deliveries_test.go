package store

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

// registerAction stores a minimal subprocess action listening on queue.
func registerAction(t *testing.T, s *Store, name, queue string, enabled bool, correlate string) {
	t.Helper()
	def := map[string]any{
		"name": name,
		"inputs": []map[string]any{
			{"queue": queue, "correlate_on": correlate},
		},
		"actor":        "subprocess",
		"instructions": map[string]any{"command": []string{"./x.sh"}},
	}
	b, err := json.Marshal(def)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.ApplyAction(context.Background(), name, b); err != nil {
		t.Fatalf("apply %s: %v", name, err)
	}
	if !enabled {
		if _, err := s.SetActionEnabled(context.Background(), name, false); err != nil {
			t.Fatalf("disable %s: %v", name, err)
		}
	}
}

func TestMaterializationFansOut(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()

	registerAction(t, s, "sub-a", "q", true, "")
	registerAction(t, s, "sub-b", "q", true, "")
	registerAction(t, s, "disabled", "q", false, "")
	registerAction(t, s, "other-queue", "elsewhere", true, "")
	registerAction(t, s, "join", "q", true, "payload.k") // skipped until 007

	n, err := s.AppendMessage(ctx, testMessage("q"))
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	if n != 2 {
		t.Fatalf("materialized %d deliveries, want 2 (the enabled non-join subscribers)", n)
	}
	ds, err := s.ListDeliveries(ctx, DeliveryListOptions{})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	names := map[string]bool{}
	for _, d := range ds {
		if d.State != StatePending {
			t.Errorf("delivery %s state %s, want pending", d.ID, d.State)
		}
		names[d.ActionName] = true
	}
	if !names["sub-a"] || !names["sub-b"] || len(names) != 2 {
		t.Errorf("deliveries for %v, want sub-a and sub-b", names)
	}
}

func TestClaimLifecycle(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	registerAction(t, s, "act", "q", true, "")
	if _, err := s.AppendMessage(ctx, testMessage("q")); err != nil {
		t.Fatal(err)
	}

	w, err := s.NextEligible(ctx, "act", now)
	if err != nil {
		t.Fatalf("next eligible: %v", err)
	}
	if err := s.ClaimDelivery(ctx, w.Delivery.ID, 1, now); err != nil {
		t.Fatalf("claim: %v", err)
	}
	// Claimed deliveries are not eligible.
	if _, err := s.NextEligible(ctx, "act", now); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claimed delivery still eligible: %v", err)
	}
	if err := s.SetDeliveryPgid(ctx, w.Delivery.ID, 4242); err != nil {
		t.Fatalf("set pgid: %v", err)
	}

	// Fail retryably: eligible again only after the gate.
	gate := now.Add(time.Minute)
	if err := s.FailDelivery(ctx, w.Delivery.ID, false, gate, "exit status 1", "boom", now); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if _, err := s.NextEligible(ctx, "act", now); !errors.Is(err, ErrNotFound) {
		t.Fatal("failed delivery eligible before its gate")
	}
	if _, err := s.NextEligible(ctx, "act", gate.Add(time.Second)); err != nil {
		t.Fatalf("failed delivery not eligible after gate: %v", err)
	}
	at, err := s.NextRetryAt(ctx)
	if err != nil || !at.Equal(gate) {
		t.Fatalf("NextRetryAt = %v, %v; want %v", at, err, gate)
	}

	d, err := s.GetDelivery(ctx, w.Delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.Attempt != 1 || d.Pgid != nil || *d.Error != "exit status 1" || *d.StderrTail != "boom" {
		t.Errorf("after fail: %+v", d)
	}
}

func TestCompleteDeliveryOutbox(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	registerAction(t, s, "producer", "in", true, "")
	registerAction(t, s, "consumer", "out", true, "")
	if _, err := s.AppendMessage(ctx, testMessage("in")); err != nil {
		t.Fatal(err)
	}
	w, err := s.NextEligible(ctx, "producer", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimDelivery(ctx, w.Delivery.ID, 1, now); err != nil {
		t.Fatal(err)
	}

	out := testMessage("out")
	created, err := s.CompleteDelivery(ctx, w.Delivery.ID, "", []*Message{out}, now)
	if err != nil {
		t.Fatalf("complete: %v", err)
	}
	if created != 1 {
		t.Fatalf("created %d downstream deliveries, want 1", created)
	}
	if _, err := s.GetMessage(ctx, out.ID); err != nil {
		t.Fatalf("emitted message missing: %v", err)
	}
	if _, err := s.NextEligible(ctx, "consumer", now); err != nil {
		t.Fatalf("downstream delivery missing: %v", err)
	}
}

// The outbox contract: if any part of completion fails, nothing of it lands —
// no done state, no emitted message. Forced here by emitting a message whose
// id already exists, which fails the insert inside the transaction.
func TestCompleteDeliveryAtomicity(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	registerAction(t, s, "producer", "in", true, "")
	in := testMessage("in")
	if _, err := s.AppendMessage(ctx, in); err != nil {
		t.Fatal(err)
	}
	w, err := s.NextEligible(ctx, "producer", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimDelivery(ctx, w.Delivery.ID, 1, now); err != nil {
		t.Fatal(err)
	}

	good := testMessage("out")
	dup := testMessage("out")
	dup.ID = in.ID // collides with the existing message

	if _, err := s.CompleteDelivery(ctx, w.Delivery.ID, "", []*Message{good, dup}, now); err == nil {
		t.Fatal("complete with duplicate output id succeeded")
	}
	d, err := s.GetDelivery(ctx, w.Delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != StateClaimed {
		t.Errorf("delivery state %s after failed completion, want claimed", d.State)
	}
	if _, err := s.GetMessage(ctx, good.ID); !errors.Is(err, ErrNotFound) {
		t.Errorf("half-emitted message survived the rollback: %v", err)
	}
}

func TestReplayGuards(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	registerAction(t, s, "act", "q", true, "")
	if _, err := s.AppendMessage(ctx, testMessage("q")); err != nil {
		t.Fatal(err)
	}
	w, err := s.NextEligible(ctx, "act", now)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := s.ReplayDelivery(ctx, w.Delivery.ID); !errors.Is(err, ErrNotDead) {
		t.Fatalf("replaying a pending delivery: %v, want ErrNotDead", err)
	}
	if _, err := s.ReplayDelivery(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("replaying a missing delivery: %v, want ErrNotFound", err)
	}

	if err := s.ClaimDelivery(ctx, w.Delivery.ID, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := s.FailDelivery(ctx, w.Delivery.ID, true, time.Time{}, "kaput", "tail", now); err != nil {
		t.Fatal(err)
	}
	d, err := s.ReplayDelivery(ctx, w.Delivery.ID)
	if err != nil {
		t.Fatalf("replay dead: %v", err)
	}
	if d.State != StatePending || d.Attempt != 0 || d.Error != nil ||
		d.ActionVersion != nil || d.StderrTail != nil {
		t.Errorf("replayed delivery not reset: %+v", d)
	}
}

func TestCancelDeliveries(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	registerAction(t, s, "act", "q", true, "")
	for i := 0; i < 3; i++ {
		if _, err := s.AppendMessage(ctx, testMessage("q")); err != nil {
			t.Fatal(err)
		}
	}
	// One claimed (must survive), one failed-retrying, one pending.
	w, err := s.NextEligible(ctx, "act", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimDelivery(ctx, w.Delivery.ID, 1, now); err != nil {
		t.Fatal(err)
	}
	w2, err := s.NextEligible(ctx, "act", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimDelivery(ctx, w2.Delivery.ID, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := s.FailDelivery(ctx, w2.Delivery.ID, false, now.Add(time.Hour), "x", "", now); err != nil {
		t.Fatal(err)
	}

	n, err := s.CancelDeliveries(ctx, "act", "action disabled", now)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if n != 2 {
		t.Fatalf("cancelled %d, want 2 (pending + failed)", n)
	}
	d, err := s.GetDelivery(ctx, w.Delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != StateClaimed {
		t.Errorf("claimed delivery was cancelled")
	}
	cancelled, err := s.ListDeliveries(ctx, DeliveryListOptions{State: StateCancelled})
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range cancelled {
		if !strings.Contains(*d.Error, "action disabled") {
			t.Errorf("cancelled without reason: %+v", d)
		}
	}
}

func TestOrphanReapStoreSide(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	registerAction(t, s, "act", "q", true, "")
	if _, err := s.AppendMessage(ctx, testMessage("q")); err != nil {
		t.Fatal(err)
	}
	w, err := s.NextEligible(ctx, "act", now)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ClaimDelivery(ctx, w.Delivery.ID, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := s.SetDeliveryPgid(ctx, w.Delivery.ID, 999999); err != nil {
		t.Fatal(err)
	}

	claimed, err := s.ClaimedDeliveries(ctx)
	if err != nil || len(claimed) != 1 || *claimed[0].Pgid != 999999 {
		t.Fatalf("claimed = %+v, %v", claimed, err)
	}
	if err := s.ResetOrphan(ctx, w.Delivery.ID, now); err != nil {
		t.Fatal(err)
	}
	d, err := s.GetDelivery(ctx, w.Delivery.ID)
	if err != nil {
		t.Fatal(err)
	}
	if d.State != StateFailed || d.Pgid != nil || *d.Error != "daemon restarted" {
		t.Errorf("orphan not reset: %+v", d)
	}
	// Immediately claimable again.
	if _, err := s.NextEligible(ctx, "act", now.Add(time.Second)); err != nil {
		t.Errorf("reset orphan not eligible: %v", err)
	}
}

func TestCountActiveDeliveries(t *testing.T) {
	s := openTest(t)
	ctx := context.Background()
	now := time.Now().UTC()

	registerAction(t, s, "act", "q", true, "")
	for i := 0; i < 2; i++ {
		if _, err := s.AppendMessage(ctx, testMessage("q")); err != nil {
			t.Fatal(err)
		}
	}
	n, err := s.CountActiveDeliveries(ctx)
	if err != nil || n != 2 {
		t.Fatalf("count = %d, %v; want 2", n, err)
	}
	w, _ := s.NextEligible(ctx, "act", now)
	if err := s.ClaimDelivery(ctx, w.Delivery.ID, 1, now); err != nil {
		t.Fatal(err)
	}
	if err := s.ResolveDelivery(ctx, w.Delivery.ID, StateFiltered, "", now); err != nil {
		t.Fatal(err)
	}
	n, _ = s.CountActiveDeliveries(ctx)
	if n != 1 {
		t.Fatalf("count after filter = %d, want 1", n)
	}
}
