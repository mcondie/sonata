// Package scheduler owns delivery state transitions. One goroutine decides
// what runs next (invariant 4): claims, filters, retries, and completions all
// funnel through its loop, while executions themselves fan out to workers.
package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/mcondie/sonata/internal/executor"
	"github.com/mcondie/sonata/internal/store"
	"github.com/mcondie/sonata/internal/workflow"
)

// Config carries the execution policy, resolved by the daemon from
// internal/config.
type Config struct {
	MaxHops            int
	DefaultMaxAttempts int
	DefaultTaskTimeout time.Duration
	BackoffBase        time.Duration
	BackoffCap         time.Duration
	StdoutCap          int64
}

// Options wires a Scheduler.
type Options struct {
	Store    *store.Store
	Executor executor.Executor
	Clock    Clock
	Config   Config
	Log      *slog.Logger
}

// idleTimerInterval is the timer setting when no retry is pending — the loop
// wakes on nudges and results, so this is only a safety net.
const idleTimerInterval = time.Hour

// Scheduler runs the claim/dispatch/apply loop.
type Scheduler struct {
	st    *store.Store
	exec  executor.Executor
	clock Clock
	cfg   Config
	log   *slog.Logger
	cache *workflow.Cache

	nudgeCh chan struct{}
	ctrlCh  chan ctrlReq
	results chan result

	// active counts deliveries in pending, failed-awaiting-retry, or claimed
	// — the states that must keep the daemon alive. Seeded from the database
	// at startup, adjusted on every transition; the idle tracker reads it
	// instead of polling the database.
	active atomic.Int64

	// ready closes once the busy counter is seeded. The daemon must not
	// serve requests before then: a WorkAdded from a handler would be
	// clobbered by the seed's absolute store.
	ready chan struct{}

	inflight map[string]int // per-action running executions; loop-local
}

// Ready is closed once Run has seeded its state and entered the loop.
func (s *Scheduler) Ready() <-chan struct{} { return s.ready }

type ctrlReq struct {
	cancelAction string
	reason       string
	reply        chan ctrlResp
}

type ctrlResp struct {
	n   int
	err error
}

type result struct {
	work   *store.Work
	action *actionInfo
	res    *executor.Result
	err    error
}

// actionInfo is one decoded current action version.
type actionInfo struct {
	name        string
	version     int64
	def         *workflow.Action
	subprocess  *workflow.SubprocessInstructions
	concurrency int
	maxAttempts int
	timeout     time.Duration
}

// withDefaults guards against a zero-valued Config: MaxHops 0 would
// dead-letter everything and BackoffBase 0 would retry in a hot loop. The
// documented defaults live in internal/config; these are the same values as
// a safety net for direct constructors.
func (c Config) withDefaults() Config {
	if c.MaxHops <= 0 {
		c.MaxHops = 100
	}
	if c.DefaultMaxAttempts <= 0 {
		c.DefaultMaxAttempts = 3
	}
	if c.DefaultTaskTimeout <= 0 {
		c.DefaultTaskTimeout = 5 * time.Minute
	}
	if c.BackoffBase <= 0 {
		c.BackoffBase = time.Second
	}
	if c.BackoffCap <= 0 {
		c.BackoffCap = time.Minute
	}
	if c.StdoutCap <= 0 {
		c.StdoutCap = 4 << 20
	}
	return c
}

// New builds a Scheduler. Run must be called for it to do anything.
func New(opts Options) *Scheduler {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	clock := opts.Clock
	if clock == nil {
		clock = RealClock{}
	}
	opts.Config = opts.Config.withDefaults()
	return &Scheduler{
		st:       opts.Store,
		exec:     opts.Executor,
		clock:    clock,
		cfg:      opts.Config,
		log:      log,
		cache:    workflow.NewCache(),
		nudgeCh:  make(chan struct{}, 1),
		ctrlCh:   make(chan ctrlReq),
		results:  make(chan result, 64),
		ready:    make(chan struct{}),
		inflight: map[string]int{},
	}
}

// Nudge tells the loop work may exist. Never blocks.
func (s *Scheduler) Nudge() {
	select {
	case s.nudgeCh <- struct{}{}:
	default:
	}
}

// WorkAdded records n new busy deliveries (message append, replay) and nudges.
func (s *Scheduler) WorkAdded(n int) {
	s.active.Add(int64(n))
	s.Nudge()
}

// Busy reports whether any delivery still needs a running daemon.
func (s *Scheduler) Busy() bool { return s.active.Load() > 0 }

// CancelAction moves an action's pending and retry-waiting deliveries to
// cancelled, inside the scheduler goroutine so the transition cannot race a
// concurrent claim. Called by the action.disable handler.
func (s *Scheduler) CancelAction(ctx context.Context, name string) (int, error) {
	req := ctrlReq{cancelAction: name, reason: "action disabled", reply: make(chan ctrlResp, 1)}
	select {
	case s.ctrlCh <- req:
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	select {
	case resp := <-req.reply:
		return resp.n, resp.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// Run is the scheduler goroutine. It blocks until ctx is cancelled, then
// waits for in-flight executions (killed via ctx) and applies their results.
func (s *Scheduler) Run(ctx context.Context) error {
	n, err := s.st.CountActiveDeliveries(ctx)
	if err != nil {
		return fmt.Errorf("seed busy counter: %w", err)
	}
	s.active.Store(int64(n))
	close(s.ready)

	timer := s.clock.NewTimer(idleTimerInterval)
	defer timer.Stop()

	for {
		s.dispatch(ctx)
		s.armTimer(ctx, timer)

		select {
		case <-ctx.Done():
			return s.drain(ctx)
		case <-s.nudgeCh:
		case r := <-s.results:
			s.apply(ctx, r)
		case <-timer.C():
		case req := <-s.ctrlCh:
			s.handleCtrl(ctx, req)
		}
	}
}

// drain waits for the workers the cancelled ctx is killing and records their
// outcomes, so a graceful stop leaves failed-retryable rows, not claimed ones.
func (s *Scheduler) drain(ctx context.Context) error {
	// The parent ctx is done; state must still be persisted.
	bg := context.WithoutCancel(ctx)
	for total(s.inflight) > 0 {
		s.apply(bg, <-s.results)
	}
	return nil
}

func total(m map[string]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// dispatch claims eligible deliveries up to each action's concurrency and
// hands them to workers. Terminal resolutions (filtered, hop cap, broken
// filter, supersession) happen here too — they are state transitions.
func (s *Scheduler) dispatch(ctx context.Context) {
	actions, err := s.loadActions(ctx)
	if err != nil {
		s.log.Error("load actions", "error", err)
		return
	}
	now := s.clock.Now()

	for _, ai := range actions {
		if ai.def.IsJoin() {
			// Joins activate in spec 007. Deliveries can still exist from a
			// pre-join version of this action; the claiming version no longer
			// covers them, so resolve rather than strand them.
			s.cancelSuperseded(ctx, ai, now)
			continue
		}
		for s.inflight[ai.name] < ai.concurrency {
			w, err := s.st.NextEligible(ctx, ai.name, now)
			if errors.Is(err, store.ErrNotFound) {
				break
			}
			if err != nil {
				s.log.Error("next eligible", "action", ai.name, "error", err)
				break
			}
			if !s.vet(ctx, ai, w, now) {
				continue // resolved terminally; try the next delivery
			}
			if err := s.st.ClaimDelivery(ctx, w.Delivery.ID, ai.version, now); err != nil {
				s.log.Error("claim", "delivery_id", w.Delivery.ID, "error", err)
				break
			}
			w.Delivery.Attempt++ // mirror the claim's increment
			s.inflight[ai.name]++
			s.launch(ctx, ai, w)
		}
	}
}

// vet applies the pre-execution decisions. Returns false when the delivery
// was resolved terminally.
func (s *Scheduler) vet(ctx context.Context, ai *actionInfo, w *store.Work, now time.Time) bool {
	logw := func(state, msg string) {
		s.log.Info("delivery resolved", "delivery_id", w.Delivery.ID,
			"action", ai.name, "state", state, "reason", msg)
	}

	// Supersession: the delivery was materialized against an older version's
	// inputs; the version it would claim under must actually cover its queue.
	idx := inputIndex(ai.def, w.Message.Queue)
	if idx < 0 {
		return s.resolve(ctx, w, store.StateCancelled,
			fmt.Sprintf("superseded by version %d", ai.version), now, logw)
	}

	hops, err := messageHops(w.Message.Headers)
	if err != nil {
		return s.resolve(ctx, w, store.StateDead,
			fmt.Sprintf("headers: %v", err), now, logw)
	}
	if hops >= s.cfg.MaxHops {
		return s.resolve(ctx, w, store.StateDead, "hop limit exceeded", now, logw)
	}

	compiled, err := s.cache.Get(ai.name, ai.version, ai.def)
	if err != nil {
		// Stored definitions were validated at apply; failing here is
		// corruption, and loud beats wedged.
		return s.resolve(ctx, w, store.StateDead,
			fmt.Sprintf("compile filter: %v", err), now, logw)
	}
	act, err := workflow.NewActivation(w.Message.Payload, w.Message.Headers,
		w.Message.Queue, w.Message.TraceID)
	if err != nil {
		return s.resolve(ctx, w, store.StateDead,
			fmt.Sprintf("filter input: %v", err), now, logw)
	}
	ok, err := workflow.EvalFilter(compiled.Filters[idx], act)
	if err != nil {
		// A broken filter must be loud, not a silent skip.
		return s.resolve(ctx, w, store.StateDead,
			fmt.Sprintf("filter: %v", err), now, logw)
	}
	if !ok {
		return s.resolve(ctx, w, store.StateFiltered, "", now, logw)
	}
	return true
}

// resolve moves a delivery to a terminal state and adjusts the busy counter.
// Always returns false so vet can tail-call it.
func (s *Scheduler) resolve(ctx context.Context, w *store.Work, state, msg string, now time.Time, logw func(state, msg string)) bool {
	if err := s.st.ResolveDelivery(ctx, w.Delivery.ID, state, msg, now); err != nil {
		s.log.Error("resolve delivery", "delivery_id", w.Delivery.ID, "error", err)
		return false
	}
	s.active.Add(-1)
	logw(state, msg)
	return false
}

// cancelSuperseded resolves eligible deliveries of an action whose current
// version cannot claim them at all (it became a join).
func (s *Scheduler) cancelSuperseded(ctx context.Context, ai *actionInfo, now time.Time) {
	for {
		w, err := s.st.NextEligible(ctx, ai.name, now)
		if errors.Is(err, store.ErrNotFound) {
			return
		}
		if err != nil {
			s.log.Error("next eligible", "action", ai.name, "error", err)
			return
		}
		reason := fmt.Sprintf("superseded by version %d", ai.version)
		if err := s.st.ResolveDelivery(ctx, w.Delivery.ID, store.StateCancelled, reason, now); err != nil {
			s.log.Error("resolve delivery", "delivery_id", w.Delivery.ID, "error", err)
			return // do not spin on a failing write
		}
		s.active.Add(-1)
		s.log.Info("delivery resolved", "delivery_id", w.Delivery.ID,
			"action", ai.name, "state", store.StateCancelled, "reason", reason)
	}
}

// launch starts one execution in a worker goroutine. Only the send on
// s.results touches scheduler state.
func (s *Scheduler) launch(ctx context.Context, ai *actionInfo, w *store.Work) {
	input, err := envelope(&w.Message)
	if err != nil {
		// Should be impossible: the message came out of the database.
		s.results <- result{work: w, action: ai, res: &executor.Result{},
			err: fmt.Errorf("encode input: %w", err)}
		return
	}
	spec := executor.Spec{
		Command:   ai.subprocess.Command,
		Timeout:   ai.timeout,
		StdoutCap: s.cfg.StdoutCap,
	}
	deliveryID := w.Delivery.ID
	go func() {
		res, err := s.exec.Execute(ctx, spec, input, func(pgid int) {
			// Persist before the process can outlive a daemon crash. The
			// write must survive ctx cancellation during shutdown.
			if err := s.st.SetDeliveryPgid(context.WithoutCancel(ctx), deliveryID, pgid); err != nil {
				s.log.Error("persist pgid", "delivery_id", deliveryID, "error", err)
			}
		})
		s.results <- result{work: w, action: ai, res: res, err: err}
	}()
}

// apply records one execution outcome — the other half of the state machine.
func (s *Scheduler) apply(ctx context.Context, r result) {
	s.inflight[r.action.name]--
	now := s.clock.Now()
	d := &r.work.Delivery

	execErr := r.err
	var outputs []*store.Message
	if execErr == nil {
		lines, err := parseNDJSON(r.res.Stdout)
		if err != nil {
			execErr = fmt.Errorf("stdout: %w", err)
		} else if r.action.def.Output != "" {
			outputs = s.buildOutputs(r, lines, now)
		}
	}

	if execErr == nil {
		created, err := s.st.CompleteDelivery(ctx, d.ID, string(r.res.StderrTail), outputs, now)
		if err != nil {
			s.log.Error("complete delivery", "delivery_id", d.ID, "error", err)
			// The delivery is still claimed; leave it for the orphan path
			// rather than inventing state here.
			return
		}
		s.active.Add(int64(created) - 1)
		s.log.Info("delivery done", "delivery_id", d.ID, "action", r.action.name,
			"attempt", d.Attempt, "emitted", len(outputs))
		s.Nudge()
		return
	}

	dead := d.Attempt >= r.action.maxAttempts
	gate := now.Add(s.backoff(d.Attempt))
	if err := s.st.FailDelivery(ctx, d.ID, dead, gate, execErr.Error(),
		string(r.res.StderrTail), now); err != nil {
		s.log.Error("fail delivery", "delivery_id", d.ID, "error", err)
		return
	}
	if dead {
		s.active.Add(-1)
		s.log.Warn("delivery dead", "delivery_id", d.ID, "action", r.action.name,
			"attempt", d.Attempt, "error", execErr)
	} else {
		s.log.Warn("delivery failed, will retry", "delivery_id", d.ID,
			"action", r.action.name, "attempt", d.Attempt,
			"retry_at", gate, "error", execErr)
	}
	s.Nudge()
}

// buildOutputs turns NDJSON lines into messages for the action's output
// queue: trace inherited, hops incremented, origin recorded.
func (s *Scheduler) buildOutputs(r result, lines []json.RawMessage, now time.Time) []*store.Message {
	in := &r.work.Message
	hops, _ := messageHops(in.Headers)
	headers, _ := json.Marshal(map[string]any{"hops": hops + 1})
	version := r.action.version
	out := make([]*store.Message, 0, len(lines))
	for _, line := range lines {
		out = append(out, &store.Message{
			ID:                  store.NewID(),
			Queue:               r.action.def.Output,
			Payload:             line,
			Headers:             headers,
			TraceID:             in.TraceID,
			OriginAction:        &r.action.name,
			OriginActionVersion: &version,
			OriginMessageID:     &in.ID,
			CreatedAt:           now,
		})
	}
	return out
}

func (s *Scheduler) handleCtrl(ctx context.Context, req ctrlReq) {
	n, err := s.st.CancelDeliveries(ctx, req.cancelAction, req.reason, s.clock.Now())
	if err == nil {
		s.active.Add(int64(-n))
	}
	req.reply <- ctrlResp{n: n, err: err}
}

// armTimer points the loop's timer at the earliest retry gate.
func (s *Scheduler) armTimer(ctx context.Context, timer Timer) {
	next, err := s.st.NextRetryAt(ctx)
	if errors.Is(err, store.ErrNotFound) {
		timer.Reset(idleTimerInterval)
		return
	}
	if err != nil {
		s.log.Error("next retry", "error", err)
		timer.Reset(idleTimerInterval)
		return
	}
	d := next.Sub(s.clock.Now())
	if d < 0 {
		d = 0
	}
	timer.Reset(d)
}

// backoff returns the delay before retry n (n ≥ 1):
// min(base·2^(n−1), cap) with ±20% jitter.
func (s *Scheduler) backoff(attempt int) time.Duration {
	d := s.cfg.BackoffBase << (attempt - 1)
	if d > s.cfg.BackoffCap || d <= 0 { // <= 0 guards shift overflow
		d = s.cfg.BackoffCap
	}
	jitter := 0.8 + 0.4*rand.Float64()
	return time.Duration(float64(d) * jitter)
}

// loadActions returns the decoded current version of every enabled action.
func (s *Scheduler) loadActions(ctx context.Context) ([]*actionInfo, error) {
	rows, err := s.st.ListActions(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]*actionInfo, 0, len(rows))
	for _, row := range rows {
		if !row.Enabled {
			continue
		}
		var def workflow.Action
		if err := json.Unmarshal(row.Definition, &def); err != nil {
			s.log.Error("decode stored action", "action", row.Name, "error", err)
			continue
		}
		ai := &actionInfo{
			name:        row.Name,
			version:     row.Version,
			def:         &def,
			concurrency: def.ConcurrencyOrDefault(),
			maxAttempts: s.cfg.DefaultMaxAttempts,
			timeout:     s.cfg.DefaultTaskTimeout,
		}
		if def.MaxAttempts != nil {
			ai.maxAttempts = *def.MaxAttempts
		}
		if def.Actor == workflow.ActorSubprocess {
			si, err := def.Subprocess()
			if err != nil {
				s.log.Error("decode instructions", "action", row.Name, "error", err)
				continue
			}
			ai.subprocess = si
			if si.Timeout != nil {
				ai.timeout = time.Duration(*si.Timeout)
			}
		}
		out = append(out, ai)
	}
	return out, nil
}

// inputIndex returns which input subscribes to queue, or -1.
func inputIndex(def *workflow.Action, queue string) int {
	for i, in := range def.Inputs {
		if in.Queue == queue {
			return i
		}
	}
	return -1
}

// messageHops reads the hops header; absent means 0.
func messageHops(headers json.RawMessage) (int, error) {
	if len(headers) == 0 {
		return 0, nil
	}
	var h struct {
		Hops *float64 `json:"hops"`
	}
	if err := json.Unmarshal(headers, &h); err != nil {
		return 0, fmt.Errorf("decode hops: %w", err)
	}
	if h.Hops == nil {
		return 0, nil
	}
	return int(*h.Hops), nil
}

// envelope is the executor stdin contract: one NDJSON line per input message.
// Joins will send several lines in this same format, so scripts written now
// stay correct.
func envelope(m *store.Message) ([]byte, error) {
	line, err := json.Marshal(map[string]any{
		"id":       m.ID,
		"queue":    m.Queue,
		"payload":  m.Payload,
		"headers":  m.Headers,
		"trace_id": m.TraceID,
	})
	if err != nil {
		return nil, err
	}
	return append(line, '\n'), nil
}

// parseNDJSON splits stdout into JSON payload lines. Blank lines are
// ignored; any other non-JSON line fails the whole output — stdout is the
// message channel, logs belong on stderr.
func parseNDJSON(stdout []byte) ([]json.RawMessage, error) {
	var out []json.RawMessage
	for i, line := range bytes.Split(stdout, []byte("\n")) {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if !json.Valid(line) {
			return nil, fmt.Errorf("line %d is not valid JSON", i+1)
		}
		out = append(out, json.RawMessage(append([]byte(nil), line...)))
	}
	return out, nil
}
