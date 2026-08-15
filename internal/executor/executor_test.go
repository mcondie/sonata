package executor

import (
	"context"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

func run(t *testing.T, ctx context.Context, spec Spec, input string) (*Result, error) {
	t.Helper()
	if spec.TermGrace == 0 {
		spec.TermGrace = 200 * time.Millisecond
	}
	return Subprocess{}.Execute(ctx, spec, []byte(input), nil)
}

func TestExecuteCapturesOutput(t *testing.T) {
	res, err := run(t, context.Background(), Spec{
		Command: []string{"sh", "-c", `read line; echo "{\"got\": $line}"; echo oops >&2`},
	}, `1`)
	if err != nil {
		t.Fatalf("execute: %v (stderr %q)", err, res.StderrTail)
	}
	if strings.TrimSpace(string(res.Stdout)) != `{"got": 1}` {
		t.Errorf("stdout = %q", res.Stdout)
	}
	if strings.TrimSpace(string(res.StderrTail)) != "oops" {
		t.Errorf("stderr tail = %q", res.StderrTail)
	}
}

func TestExecuteNonZeroExit(t *testing.T) {
	res, err := run(t, context.Background(), Spec{
		Command: []string{"sh", "-c", "echo diagnostic >&2; exit 3"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "exit status 3") {
		t.Fatalf("err = %v, want exit status 3", err)
	}
	if !strings.Contains(string(res.StderrTail), "diagnostic") {
		t.Errorf("stderr tail = %q, want the diagnostic", res.StderrTail)
	}
}

func TestExecuteSpawnError(t *testing.T) {
	_, err := run(t, context.Background(), Spec{
		Command: []string{"/does/not/exist"},
	}, "")
	if err == nil || !strings.Contains(err.Error(), "spawn") {
		t.Fatalf("err = %v, want spawn error", err)
	}
}

func TestExecuteTimeoutKills(t *testing.T) {
	start := time.Now()
	_, err := run(t, context.Background(), Spec{
		Command: []string{"sh", "-c", "sleep 30"},
		Timeout: 100 * time.Millisecond,
	}, "")
	if err == nil || !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("err = %v, want timeout", err)
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("took %s: the group kill did not land", elapsed)
	}
}

func TestExecuteStdoutCapFails(t *testing.T) {
	_, err := run(t, context.Background(), Spec{
		Command:   []string{"sh", "-c", "yes | head -c 100000; sleep 30"},
		Timeout:   5 * time.Second,
		StdoutCap: 1 << 10,
	}, "")
	if err == nil || !strings.Contains(err.Error(), "stdout exceeded") {
		t.Fatalf("err = %v, want stdout cap error", err)
	}
}

// A task that spawns a grandchild and ignores SIGTERM must still be fully
// gone after cancellation: the kill targets the process group.
func TestCancelKillsGrandchildren(t *testing.T) {
	pidFile := t.TempDir() + "/pid"
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := Subprocess{}.Execute(ctx, Spec{
			Command: []string{"sh", "-c",
				`trap "" TERM; (trap "" TERM; echo $$ > ` + pidFile + `; sleep 60) & wait`},
			TermGrace: 100 * time.Millisecond,
		}, nil, nil)
		done <- err
	}()

	// Wait for the grandchild to record itself.
	var pid int
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if b, err := os.ReadFile(pidFile); err == nil {
			if p, err := strconv.Atoi(strings.TrimSpace(string(b))); err == nil && p > 0 {
				pid = p
				break
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pid == 0 {
		t.Fatal("grandchild never started")
	}

	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled execution reported success")
	}

	// The whole group must be gone; signal 0 probes existence.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := syscall.Kill(pid, 0); err == syscall.ESRCH {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("grandchild pid %d survived the group kill", pid)
}

func TestStderrTailKeepsLastBytes(t *testing.T) {
	res, err := run(t, context.Background(), Spec{
		Command: []string{"sh", "-c", `i=0; while [ $i -lt 2000 ]; do echo "line $i" >&2; i=$((i+1)); done`},
	}, "")
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(res.StderrTail) > StderrTailSize {
		t.Errorf("tail is %d bytes, cap is %d", len(res.StderrTail), StderrTailSize)
	}
	if !strings.Contains(string(res.StderrTail), "line 1999") {
		t.Errorf("tail lost the final lines")
	}
}
