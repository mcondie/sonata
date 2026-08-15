package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mcondie/sonata/internal/executor"
	"github.com/mcondie/sonata/internal/store"
	"github.com/mcondie/sonata/internal/workflow"
)

// fakeExec scripts executor behavior per command name (command[0]).
type fakeExec struct {
	mu        sync.Mutex
	responses map[string]fakeResp
	calls     map[string][][]byte // command name → inputs seen
	gates     map[string]chan struct{}
}

type fakeResp struct {
	stdout string
	err    error
}

func newFakeExec() *fakeExec {
	return &fakeExec{
		responses: map[string]fakeResp{},
		calls:     map[string][][]byte{},
		gates:     map[string]chan struct{}{},
	}
}

func (f *fakeExec) respond(cmd, stdout string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.responses[cmd] = fakeResp{stdout: stdout, err: err}
}

// gate makes executions of cmd block until the returned channel is closed.
func (f *fakeExec) gate(cmd string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch := make(chan struct{})
	f.gates[cmd] = ch
	return ch
}

func (f *fakeExec) callCount(cmd string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.calls[cmd])
}

func (f *fakeExec) inputs(cmd string) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([][]byte(nil), f.calls[cmd]...)
}

func (f *fakeExec) Execute(ctx context.Context, spec executor.Spec, input []byte, onStart func(int)) (*executor.Result, error) {
	cmd := spec.Command[0]
	f.mu.Lock()
	f.calls[cmd] = append(f.calls[cmd], input)
	resp, ok := f.responses[cmd]
	gate := f.gates[cmd]
	f.mu.Unlock()
	if onStart != nil {
		onStart(12345)
	}
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return &executor.Result{}, fmt.Errorf("cancelled: %w", ctx.Err())
		}
	}
	if !ok {
		return &executor.Result{Stdout: []byte("")}, nil
	}
	return &executor.Result{Stdout: []byte(resp.stdout), StderrTail: []byte("tail")}, resp.err
}

type harness struct {
	t     *testing.T
	st    *store.Store
	sched *Scheduler
	clock *FakeClock
	exec  *fakeExec
	stop  func()
}

func newHarness(t *testing.T, mod func(*Config)) *harness {
	t.Helper()
	st, err := store.Open(context.Background(), filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })

	cfg := Config{
		MaxHops:            5,
		DefaultMaxAttempts: 3,
		DefaultTaskTimeout: time.Minute,
		BackoffBase:        time.Second,
		BackoffCap:         time.Minute,
		StdoutCap:          1 << 20,
	}
	if mod != nil {
		mod(&cfg)
	}
	h := &harness{
		t:     t,
		st:    st,
		clock: NewFakeClock(),
		exec:  newFakeExec(),
	}
	h.sched = New(Options{
		Store:    st,
		Executor: h.exec,
		Clock:    h.clock,
		Config:   cfg,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.sched.Run(ctx) }()
	<-h.sched.Ready()

	stopped := false
	h.stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("scheduler run: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("scheduler did not stop")
		}
	}
	t.Cleanup(h.stop)
	return h
}

// apply registers an action definition given as JSON.
func (h *harness) apply(def string) {
	h.t.Helper()
	a, err := workflow.ParseJSON([]byte(def))
	if err != nil {
		h.t.Fatalf("parse action: %v", err)
	}
	canonical, err := a.Canonical()
	if err != nil {
		h.t.Fatal(err)
	}
	if _, _, err := h.st.ApplyAction(context.Background(), a.Name, canonical); err != nil {
		h.t.Fatalf("apply action: %v", err)
	}
	h.sched.Nudge()
}

func action(name, queue, cmd string, extra string) string {
	if extra != "" {
		extra = "," + extra
	}
	return fmt.Sprintf(`{"name":%q,"inputs":[{"queue":%q}],"actor":"subprocess",
		"instructions":{"command":[%q]}%s}`, name, queue, cmd, extra)
}

// send appends a message the way the API does, then tells the scheduler.
func (h *harness) send(queue, payload, headers string) *store.Message {
	h.t.Helper()
	if headers == "" {
		headers = `{"hops":0}`
	}
	m := &store.Message{
		ID:        store.NewID(),
		Queue:     queue,
		Payload:   json.RawMessage(payload),
		Headers:   json.RawMessage(headers),
		TraceID:   store.NewID(),
		CreatedAt: h.clock.Now(),
	}
	n, err := h.st.AppendMessage(context.Background(), m)
	if err != nil {
		h.t.Fatalf("append: %v", err)
	}
	h.sched.WorkAdded(n)
	return m
}

// waitDeliveries polls until want deliveries match the filter, or fails. The
// scheduler is asynchronous; polling with a deadline is the sync mechanism.
func (h *harness) waitDeliveries(opts store.DeliveryListOptions, want int) []*store.Delivery {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ds, err := h.st.ListDeliveries(context.Background(), opts)
		if err != nil {
			h.t.Fatalf("list deliveries: %v", err)
		}
		if len(ds) == want {
			return ds
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("deliveries %+v never reached %d matches: have %d", opts, want, len(ds))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPipelineTwoActions(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("first", "in", "cmd-a", `"output":"mid"`))
	h.apply(action("second", "mid", "cmd-b", `"output":"out"`))
	h.exec.respond("cmd-a", `{"stage":1}`, nil)
	h.exec.respond("cmd-b", `{"stage":2}`, nil)

	in := h.send("in", `{"go":true}`, "")

	h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone}, 2)

	// The emitted messages carry trace, hops, and origin.
	mid, err := h.st.ListMessages(context.Background(), store.ListOptions{Queue: "mid"})
	if err != nil || len(mid) != 1 {
		t.Fatalf("mid queue: %v %v", mid, err)
	}
	if mid[0].TraceID != in.TraceID {
		t.Errorf("trace not inherited: %s vs %s", mid[0].TraceID, in.TraceID)
	}
	if *mid[0].OriginAction != "first" || *mid[0].OriginMessageID != in.ID {
		t.Errorf("origin wrong: %+v", mid[0])
	}
	var hd map[string]any
	_ = json.Unmarshal(mid[0].Headers, &hd)
	if hd["hops"] != float64(1) {
		t.Errorf("hops = %v, want 1", hd["hops"])
	}
	out, err := h.st.ListMessages(context.Background(), store.ListOptions{Queue: "out"})
	if err != nil || len(out) != 1 {
		t.Fatalf("out queue: %v %v", out, err)
	}
	var hd2 map[string]any
	_ = json.Unmarshal(out[0].Headers, &hd2)
	if hd2["hops"] != float64(2) || out[0].TraceID != in.TraceID {
		t.Errorf("second hop wrong: %+v", out[0])
	}

	if h.sched.Busy() {
		t.Error("scheduler still busy after drain")
	}
}

func TestConcurrencyCeiling(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("slow", "q", "cmd", `"concurrency":2`))
	gate := h.exec.gate("cmd")

	for i := 0; i < 5; i++ {
		h.send("q", `{}`, "")
	}

	h.waitDeliveries(store.DeliveryListOptions{State: store.StateClaimed}, 2)
	h.waitDeliveries(store.DeliveryListOptions{State: store.StatePending}, 3)
	if got := h.exec.callCount("cmd"); got != 2 {
		t.Fatalf("started %d executions, concurrency is 2", got)
	}

	close(gate)
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone}, 5)
}

func TestFilterOutcomes(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("picky", "q", "cmd", `"output":"out"`))
	// Re-apply with a filter via full definition.
	h.apply(`{"name":"picky","inputs":[{"queue":"q","filter":"payload.keep == true"}],
		"actor":"subprocess","instructions":{"command":["cmd"]},"output":"out"}`)
	h.exec.respond("cmd", "", nil)

	h.send("q", `{"keep":true}`, "")
	h.send("q", `{"keep":false}`, "")
	h.send("q", `{}`, "") // missing key → CEL eval error → dead

	h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone}, 1)
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateFiltered}, 1)
	dead := h.waitDeliveries(store.DeliveryListOptions{State: store.StateDead}, 1)
	if !strings.Contains(*dead[0].Error, "filter") {
		t.Errorf("dead error = %q, want a filter error", *dead[0].Error)
	}
	if h.sched.Busy() {
		t.Error("busy after all outcomes terminal")
	}
}

func TestHopCapDeadLetters(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.MaxHops = 3 })
	h.apply(action("hopper", "q", "cmd", ""))

	h.send("q", `{}`, `{"hops":3}`)

	dead := h.waitDeliveries(store.DeliveryListOptions{State: store.StateDead}, 1)
	if *dead[0].Error != "hop limit exceeded" {
		t.Errorf("error = %q", *dead[0].Error)
	}
}

func TestRetryScheduleToDead(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("flaky", "q", "cmd", ""))
	h.exec.respond("cmd", "", errors.New("exit status 1"))

	h.send("q", `{}`, "")

	// Attempt 1 fails and parks behind the backoff gate.
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateFailed}, 1)
	if !h.sched.Busy() {
		t.Error("failed-awaiting-retry must count as busy")
	}

	// Each advance clears the (jittered ≤ 1.2·base·2ⁿ) gate for the next
	// try. Wait for the failure to be *recorded* before advancing, or the
	// next gate is computed from the already-advanced clock.
	h.waitFailedAttempt("flaky", 1)
	h.clock.Advance(2 * time.Second)
	h.waitFailedAttempt("flaky", 2)
	h.clock.Advance(4 * time.Second)

	dead := h.waitDeliveries(store.DeliveryListOptions{State: store.StateDead}, 1)
	if dead[0].Attempt != 3 {
		t.Errorf("attempt = %d, want 3", dead[0].Attempt)
	}
	if *dead[0].Error != "exit status 1" || *dead[0].StderrTail != "tail" {
		t.Errorf("dead diagnostics: %+v", dead[0])
	}
	if h.sched.Busy() {
		t.Error("busy after dead")
	}
}

// waitFailedAttempt polls until the action's single delivery sits in failed
// with the given attempt recorded.
func (h *harness) waitFailedAttempt(action string, attempt int) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		ds, err := h.st.ListDeliveries(context.Background(), store.DeliveryListOptions{Action: action})
		if err != nil {
			h.t.Fatal(err)
		}
		if len(ds) == 1 && ds[0].State == store.StateFailed && ds[0].Attempt >= attempt {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatalf("never failed at attempt %d: %+v", attempt, ds)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestVersionPinnedAtClaim(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("versioned", "q", "cmd", ""))
	gate := h.exec.gate("cmd")

	h.send("q", `{}`, "")
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateClaimed}, 1)

	// An apply lands mid-flight; the running delivery keeps its version.
	h.apply(action("versioned", "q", "cmd-v2", ""))
	close(gate)

	done := h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone}, 1)
	if *done[0].ActionVersion != 1 {
		t.Errorf("action_version = %d, want 1 (claim-time pin)", *done[0].ActionVersion)
	}
}

func TestReplayRunsCurrentVersion(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.DefaultMaxAttempts = 1 })
	h.apply(action("fixme", "q", "cmd-broken", ""))
	h.exec.respond("cmd-broken", "", errors.New("exit status 1"))
	h.exec.respond("cmd-fixed", `{"ok":true}`, nil)

	h.send("q", `{}`, "")
	dead := h.waitDeliveries(store.DeliveryListOptions{State: store.StateDead}, 1)

	// Fix the action, then replay the dead delivery.
	h.apply(action("fixme", "q", "cmd-fixed", `"output":"out"`))
	if _, err := h.st.ReplayDelivery(context.Background(), dead[0].ID); err != nil {
		t.Fatalf("replay: %v", err)
	}
	h.sched.WorkAdded(1)

	done := h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone}, 1)
	if *done[0].ActionVersion != 2 {
		t.Errorf("replay ran version %d, want 2", *done[0].ActionVersion)
	}
	if msgs, _ := h.st.ListMessages(context.Background(), store.ListOptions{Queue: "out"}); len(msgs) != 1 {
		t.Errorf("replayed run did not emit")
	}
}

func TestDisableCancelsOutstanding(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("busy", "q", "cmd", ""))
	gate := h.exec.gate("cmd")

	h.send("q", `{}`, "") // will be claimed and blocked
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateClaimed}, 1)
	h.send("q", `{}`, "") // stays pending
	h.waitDeliveries(store.DeliveryListOptions{State: store.StatePending}, 1)

	if _, err := h.st.SetActionEnabled(context.Background(), "busy", false); err != nil {
		t.Fatal(err)
	}
	n, err := h.sched.CancelAction(context.Background(), "busy")
	if err != nil || n != 1 {
		t.Fatalf("cancel: n=%d err=%v, want 1 cancelled", n, err)
	}
	cancelled := h.waitDeliveries(store.DeliveryListOptions{State: store.StateCancelled}, 1)
	if !strings.Contains(*cancelled[0].Error, "action disabled") {
		t.Errorf("cancel reason = %q", *cancelled[0].Error)
	}

	// The claimed execution finishes untouched.
	close(gate)
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone}, 1)
	if h.sched.Busy() {
		t.Error("busy after disable-cancel and drain")
	}
}

func TestSupersededByInputChange(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("mover", "q1", "cmd", `"concurrency":1`))
	gate := h.exec.gate("cmd")

	h.send("q1", `{}`, "") // claimed, blocked
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateClaimed}, 1)
	h.send("q1", `{}`, "") // pending, materialized against v1
	h.waitDeliveries(store.DeliveryListOptions{State: store.StatePending}, 1)

	// v2 moves to another queue; the pending q1 delivery is now uncoverable.
	h.apply(action("mover", "q2", "cmd", `"concurrency":1`))
	close(gate)

	cancelled := h.waitDeliveries(store.DeliveryListOptions{State: store.StateCancelled}, 1)
	if !strings.Contains(*cancelled[0].Error, "superseded by version 2") {
		t.Errorf("cancel reason = %q", *cancelled[0].Error)
	}
}

func TestSupersededByJoinChange(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("joiner", "q1", "cmd", `"concurrency":1`))
	gate := h.exec.gate("cmd")

	h.send("q1", `{}`, "")
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateClaimed}, 1)
	h.send("q1", `{}`, "")
	h.waitDeliveries(store.DeliveryListOptions{State: store.StatePending}, 1)

	// v2 turns the action into a join: the whole processing path flips.
	h.apply(`{"name":"joiner","inputs":[
		{"queue":"q1","correlate_on":"payload.k"},
		{"queue":"q2","correlate_on":"payload.k"}],
		"actor":"subprocess","instructions":{"command":["cmd"]}}`)
	close(gate)

	h.waitDeliveries(store.DeliveryListOptions{State: store.StateCancelled}, 1)
}

func TestBadStdoutFailsRetryably(t *testing.T) {
	h := newHarness(t, func(c *Config) { c.DefaultMaxAttempts = 1 })
	h.apply(action("garbler", "q", "cmd", `"output":"out"`))
	h.exec.respond("cmd", "this is not json\n", nil)

	h.send("q", `{}`, "")

	dead := h.waitDeliveries(store.DeliveryListOptions{State: store.StateDead}, 1)
	if !strings.Contains(*dead[0].Error, "stdout") {
		t.Errorf("error = %q, want stdout parse error", *dead[0].Error)
	}
	if msgs, _ := h.st.ListMessages(context.Background(), store.ListOptions{Queue: "out"}); len(msgs) != 0 {
		t.Error("garbage stdout still emitted messages")
	}
}

func TestGracefulDrainLeavesRetryable(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("interrupted", "q", "cmd", ""))
	h.exec.gate("cmd") // never released; only ctx cancel ends it

	h.send("q", `{}`, "")
	h.waitDeliveries(store.DeliveryListOptions{State: store.StateClaimed}, 1)

	// Stopping the scheduler kills the execution and records the outcome —
	// a graceful stop leaves failed rows, not claimed ones.
	h.stop()

	ds, err := h.st.ListDeliveries(context.Background(), store.DeliveryListOptions{})
	if err != nil || len(ds) != 1 {
		t.Fatalf("deliveries: %v %v", ds, err)
	}
	if ds[0].State != store.StateFailed {
		t.Errorf("state after drain = %s, want failed", ds[0].State)
	}
}

func TestContention(t *testing.T) {
	h := newHarness(t, nil)
	h.apply(action("worker", "q", "cmd", `"concurrency":4,"output":"out"`))
	h.exec.respond("cmd", `{"n":1}`, nil)

	const senders, per = 4, 10
	var wg sync.WaitGroup
	for i := 0; i < senders; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				h.send("q", `{}`, "")
			}
		}()
	}
	wg.Wait()

	h.waitDeliveries(store.DeliveryListOptions{State: store.StateDone, Limit: 100}, senders*per)
	msgs, err := h.st.ListMessages(context.Background(), store.ListOptions{Queue: "out", Limit: 100})
	if err != nil || len(msgs) != senders*per {
		t.Fatalf("emitted %d messages, want %d (%v)", len(msgs), senders*per, err)
	}
	if h.sched.Busy() {
		t.Error("busy after full drain")
	}
}
