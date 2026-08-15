package daemon

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ErrLocked is returned when the lock is held by another process.
var ErrLocked = errors.New("lock held by another process")

// Lock is an advisory whole-file lock held via flock(2).
//
// The kernel releases a flock when the file descriptor closes, including on
// abnormal termination, so there is no such thing as a stale lock: if
// Acquire succeeds, no other live process holds it. That property is why
// liveness is determined by the lock and not by the PID file contents —
// PIDs are recycled, flocks are not.
type Lock struct {
	f    *os.File
	path string
}

// Acquire takes an exclusive, non-blocking lock. It returns ErrLocked if
// another process holds it.
func Acquire(path string) (*Lock, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open lock %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, fmt.Errorf("flock %s: %w", path, err)
	}
	return &Lock{f: f, path: path}, nil
}

// AcquireWait retries until the lock is available or timeout elapses.
// flock(2) has no timed variant, so this polls rather than blocking.
func AcquireWait(path string, timeout time.Duration) (*Lock, error) {
	deadline := time.Now().Add(timeout)
	for {
		l, err := Acquire(path)
		if err == nil {
			return l, nil
		}
		if !errors.Is(err, ErrLocked) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("timed out after %s waiting for %s: %w", timeout, path, ErrLocked)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// WritePID records pid as the lock file's contents, for use by `down`.
// It is diagnostic only; liveness comes from the lock itself.
func (l *Lock) WritePID(pid int) error {
	if err := l.f.Truncate(0); err != nil {
		return fmt.Errorf("truncate lock: %w", err)
	}
	if _, err := l.f.WriteAt([]byte(strconv.Itoa(pid)+"\n"), 0); err != nil {
		return fmt.Errorf("write pid: %w", err)
	}
	return l.f.Sync()
}

// Release drops the lock and closes the descriptor.
//
// The lock file is deliberately not removed. Unlinking it would let a later
// process create and lock a different inode while this one still believes it
// holds the lock.
func (l *Lock) Release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	if cerr := l.f.Close(); err == nil {
		err = cerr
	}
	l.f = nil
	return err
}

// ReadPID returns the PID recorded in the lock file.
func ReadPID(path string) (int, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil {
		return 0, fmt.Errorf("parse pid in %s: %w", path, err)
	}
	return pid, nil
}

// RunningPID reports whether a daemon holds the lock, and its PID.
//
// It answers the question by trying to take the lock: success means nothing
// is running, so the lock is immediately released and false returned.
func RunningPID(lockPath string) (int, bool) {
	l, err := Acquire(lockPath)
	if err == nil {
		_ = l.Release()
		return 0, false
	}
	if !errors.Is(err, ErrLocked) {
		return 0, false
	}
	pid, err := ReadPID(lockPath)
	if err != nil {
		// Locked but unreadable: something is alive, we just cannot name it.
		return 0, true
	}
	return pid, true
}

// Alive reports whether a process with the given PID exists. Signal 0
// performs error checking without delivering a signal.
func Alive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}
