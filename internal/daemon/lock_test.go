package daemon

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestAcquireExcludesSecondHolder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lock")

	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("first acquire: %v", err)
	}

	// flock is per open file description, not per process, so a second
	// Acquire opens a distinct descriptor and genuinely contends.
	if _, err := Acquire(path); !errors.Is(err, ErrLocked) {
		t.Fatalf("second acquire: got %v, want ErrLocked", err)
	}

	if err := first.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}

	second, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	_ = second.Release()
}

func TestReleaseKeepsLockFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lock")

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	if err := l.Release(); err != nil {
		t.Fatalf("release: %v", err)
	}

	// Unlinking on release would let a later process lock a different inode
	// while an earlier one still believed it held the lock.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("lock file should survive release: %v", err)
	}
}

func TestRunningPID(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lock")

	if _, running := RunningPID(path); running {
		t.Fatal("nothing holds the lock, want running=false")
	}

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer func() { _ = l.Release() }()

	if err := l.WritePID(os.Getpid()); err != nil {
		t.Fatalf("write pid: %v", err)
	}

	pid, running := RunningPID(path)
	if !running {
		t.Fatal("lock is held, want running=true")
	}
	if pid != os.Getpid() {
		t.Fatalf("pid = %d, want %d", pid, os.Getpid())
	}
}

func TestWritePIDTruncates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lock")

	l, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer func() { _ = l.Release() }()

	// A longer PID followed by a shorter one must not leave trailing digits.
	if err := l.WritePID(123456); err != nil {
		t.Fatalf("write pid: %v", err)
	}
	if err := l.WritePID(42); err != nil {
		t.Fatalf("rewrite pid: %v", err)
	}

	got, err := ReadPID(path)
	if err != nil {
		t.Fatalf("read pid: %v", err)
	}
	if got != 42 {
		t.Fatalf("pid = %d, want 42", got)
	}
}

func TestAcquireWaitTimesOut(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lock")

	held, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer func() { _ = held.Release() }()

	start := time.Now()
	if _, err := AcquireWait(path, 100*time.Millisecond); !errors.Is(err, ErrLocked) {
		t.Fatalf("got %v, want ErrLocked", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("returned after %s, want at least the full timeout", elapsed)
	}
}

func TestAcquireWaitSucceedsAfterRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.lock")

	held, err := Acquire(path)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = held.Release()
	}()

	l, err := AcquireWait(path, 2*time.Second)
	if err != nil {
		t.Fatalf("acquire wait: %v", err)
	}
	_ = l.Release()
}

func TestAlive(t *testing.T) {
	if !Alive(os.Getpid()) {
		t.Error("current process should be alive")
	}
	if Alive(0) || Alive(-1) {
		t.Error("invalid pids should not be reported alive")
	}
}
