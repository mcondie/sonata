// Package executor runs task subprocesses: process-group isolation, captured
// output, timeouts. It knows nothing about deliveries or queues — the
// scheduler decides what to run and what the outcome means.
package executor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// StderrTailSize bounds how much stderr is kept for diagnostics.
const StderrTailSize = 8 << 10

// defaultTermGrace is how long a signalled process group gets to exit before
// SIGKILL.
const defaultTermGrace = 5 * time.Second

// Spec describes one execution.
type Spec struct {
	Command []string
	// Timeout bounds the run. Zero means no timeout (the scheduler always
	// sets one; zero exists for tests).
	Timeout time.Duration
	// StdoutCap fails the run when stdout exceeds it, rather than truncating
	// silently. Zero means unlimited.
	StdoutCap int64
	// TermGrace overrides the SIGTERM→SIGKILL grace, for tests. Zero means
	// the 5s default.
	TermGrace time.Duration
}

// Result carries the captured output. It is non-nil even on error, so the
// caller can persist the stderr tail of a failed run.
type Result struct {
	Stdout     []byte
	StderrTail []byte
}

// Executor is the scheduler's execution interface; tests fake it, and later
// slices add actor types behind it.
type Executor interface {
	// Execute runs the spec with input on stdin. onStart, when non-nil, is
	// called with the process group id as soon as the process exists, so the
	// caller can persist it before a crash could orphan the group.
	Execute(ctx context.Context, spec Spec, input []byte, onStart func(pgid int)) (*Result, error)
}

// Subprocess is the real Executor.
type Subprocess struct{}

// Execute runs the command in its own process group, feeding input on stdin
// and capturing stdout/stderr — never inherited, so a task cannot write into
// the daemon's streams. Cancellation and timeout kill the whole group:
// SIGTERM, a grace period, then SIGKILL, so grandchildren cannot survive.
func (Subprocess) Execute(ctx context.Context, spec Spec, input []byte, onStart func(pgid int)) (*Result, error) {
	if len(spec.Command) == 0 {
		return &Result{}, errors.New("spawn: empty command")
	}
	grace := spec.TermGrace
	if grace <= 0 {
		grace = defaultTermGrace
	}

	timedOut := false
	if spec.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, spec.Timeout)
		defer cancel()
	}

	stdout := newCappedBuffer(spec.StdoutCap)
	stderr := newTailBuffer(StderrTailSize)

	// The context is handled manually below (group kill with grace), so the
	// command itself is not built with CommandContext — that would kill only
	// the direct child.
	cmd := exec.Command(spec.Command[0], spec.Command[1:]...)
	cmd.Stdin = bytes.NewReader(input)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		return &Result{StderrTail: stderr.Bytes()}, fmt.Errorf("spawn: %w", err)
	}
	// Setpgid makes the child the leader of a fresh group, so pgid == pid.
	pgid := cmd.Process.Pid
	if onStart != nil {
		onStart(pgid)
	}

	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()

	var waitErr error
	select {
	case waitErr = <-done:
	case <-ctx.Done():
		timedOut = errors.Is(ctx.Err(), context.DeadlineExceeded)
		waitErr = killGroup(pgid, grace, done)
	case <-stdout.exceeded():
		_ = killGroup(pgid, grace, done)
		waitErr = fmt.Errorf("stdout exceeded %d byte cap", spec.StdoutCap)
	}

	res := &Result{Stdout: stdout.Bytes(), StderrTail: stderr.Bytes()}
	switch {
	case timedOut:
		return res, fmt.Errorf("timeout after %s", spec.Timeout)
	case waitErr != nil && ctx.Err() != nil && !timedOut:
		return res, fmt.Errorf("cancelled: %w", waitErr)
	case waitErr != nil:
		return res, waitErr
	case stdout.overflowed():
		return res, fmt.Errorf("stdout exceeded %d byte cap", spec.StdoutCap)
	}
	return res, nil
}

// killGroup terminates a process group and returns the child's wait result:
// SIGTERM, up to grace for the direct child to exit, then SIGKILL. The final
// group-wide SIGKILL also sweeps grandchildren that outlived a cooperative
// child. ESRCH just means already gone.
func killGroup(pgid int, grace time.Duration, done <-chan error) error {
	_ = syscall.Kill(-pgid, syscall.SIGTERM)
	var waitErr error
	select {
	case waitErr = <-done:
	case <-time.After(grace):
		_ = syscall.Kill(-pgid, syscall.SIGKILL)
		waitErr = <-done
	}
	_ = syscall.Kill(-pgid, syscall.SIGKILL)
	return waitErr
}

// cappedBuffer stores up to cap bytes and signals when a write would exceed
// it. Excess is discarded, not stored — the run is failed, not truncated.
type cappedBuffer struct {
	mu   sync.Mutex
	buf  bytes.Buffer
	max  int64
	over chan struct{}
	done bool
}

func newCappedBuffer(max int64) *cappedBuffer {
	return &cappedBuffer{max: max, over: make(chan struct{})}
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.max > 0 && int64(b.buf.Len())+int64(len(p)) > b.max {
		if !b.done {
			b.done = true
			close(b.over)
		}
		// Report success so the pipe stays open until the kill lands; the
		// overflow flag is what fails the run.
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf.Bytes()...)
}

func (b *cappedBuffer) exceeded() <-chan struct{} { return b.over }

func (b *cappedBuffer) overflowed() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.done
}

// tailBuffer keeps the last max bytes written.
type tailBuffer struct {
	mu  sync.Mutex
	buf []byte
	max int
}

func newTailBuffer(max int) *tailBuffer { return &tailBuffer{max: max} }

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if len(b.buf) > b.max {
		b.buf = append([]byte(nil), b.buf[len(b.buf)-b.max:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) Bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.buf...)
}
