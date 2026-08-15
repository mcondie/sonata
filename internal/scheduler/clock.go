package scheduler

import (
	"sync"
	"time"
)

// Clock abstracts time so scheduler tests drive retries and timers without
// sleeping — never time.Sleep to synchronize.
type Clock interface {
	Now() time.Time
	NewTimer(d time.Duration) Timer
}

// Timer is the slice of time.Timer the scheduler needs.
type Timer interface {
	C() <-chan time.Time
	Stop()
	Reset(d time.Duration)
}

// RealClock is the production Clock.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

func (RealClock) NewTimer(d time.Duration) Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time { return r.t.C }
func (r *realTimer) Stop()               { r.t.Stop() }
func (r *realTimer) Reset(d time.Duration) {
	// Drain-then-reset per time.Timer's contract for timers whose channel is
	// consumed by a select that may not have fired.
	if !r.t.Stop() {
		select {
		case <-r.t.C:
		default:
		}
	}
	r.t.Reset(d)
}

// FakeClock is a manually advanced Clock for tests.
type FakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

// NewFakeClock starts at a fixed, arbitrary instant.
func NewFakeClock() *FakeClock {
	return &FakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *FakeClock) NewTimer(d time.Duration) Timer {
	f.mu.Lock()
	defer f.mu.Unlock()
	t := &fakeTimer{clk: f, ch: make(chan time.Time, 1), deadline: f.now.Add(d)}
	f.timers = append(f.timers, t)
	return t
}

// Advance moves the clock and fires every due timer.
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	now := f.now
	due := make([]*fakeTimer, 0)
	for _, t := range f.timers {
		if !t.stopped && !t.deadline.After(now) {
			t.stopped = true
			due = append(due, t)
		}
	}
	f.mu.Unlock()
	for _, t := range due {
		select {
		case t.ch <- now:
		default:
		}
	}
}

type fakeTimer struct {
	clk      *FakeClock
	ch       chan time.Time
	deadline time.Time
	stopped  bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Stop() {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	t.stopped = true
}

func (t *fakeTimer) Reset(d time.Duration) {
	t.clk.mu.Lock()
	defer t.clk.mu.Unlock()
	t.deadline = t.clk.now.Add(d)
	t.stopped = false
	select {
	case <-t.ch:
	default:
	}
}
