// Package e2e runs the testscript suite against a real sonata binary.
//
// These tests exist only for behaviour that cannot be reached in-process:
// spawning and detaching the daemon, the start-lock race, signal handling,
// exit codes, and socket file lifecycle. Anything that does not cross a real
// process boundary belongs in a package-level test instead.
package e2e

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/rogpeppe/go-internal/testscript"
)

// binDir holds the freshly built sonata binary and is prepended to PATH so
// scripts can `exec sonata` by name.
var binDir string

func TestMain(m *testing.M) {
	code, err := run(m)
	if err != nil {
		fmt.Fprintln(os.Stderr, "e2e setup:", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func run(m *testing.M) (int, error) {
	// A real binary on PATH, not testscript's in-process command map: the
	// daemon spawns itself via os.Executable(), which in-process would
	// resolve to this test binary rather than sonata.
	dir, err := os.MkdirTemp("", "sonata-bin")
	if err != nil {
		return 0, err
	}
	defer os.RemoveAll(dir)

	args := []string{"build"}
	if cover := os.Getenv("SONATA_E2E_COVERDIR"); cover != "" {
		// Opt-in: a -cover binary warns on stderr when GOCOVERDIR is unset,
		// which would corrupt the scripts' stderr assertions.
		args = append(args, "-cover")
	}
	args = append(args, "-o", filepath.Join(dir, "sonata"), "../cmd/sonata")

	build := exec.Command("go", args...)
	if out, err := build.CombinedOutput(); err != nil {
		return 0, fmt.Errorf("build sonata: %w\n%s", err, out)
	}
	binDir = dir

	return m.Run(), nil
}

func TestScripts(t *testing.T) {
	if testing.Short() {
		t.Skip("e2e: spawns real daemons")
	}
	testscript.Run(t, testscript.Params{
		Dir:   filepath.Join("testdata", "script"),
		Setup: setup,
	})
}

func setup(env *testscript.Env) error {
	// Socket paths must be short. sockaddr_un.sun_path caps at 104 bytes on
	// macOS and 108 on Linux; testscript's work dir (and t.TempDir) sit under
	// /var/folders/... on macOS, which overruns it and fails with a
	// misleading "bind: invalid argument".
	sockDir, err := os.MkdirTemp("/tmp", "sn")
	if err != nil {
		return err
	}
	socket := filepath.Join(sockDir, "s.sock")
	stateDir := filepath.Join(env.WorkDir, "state")

	env.Vars = append(env.Vars,
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"SONATA_STATE_DIR="+stateDir,
		"SONATA_SOCKET="+socket,
		"SONATA_DATABASE="+filepath.Join(stateDir, "sonata.db"),
		"SONATA_LOG_LEVEL=debug",
		"SOCKET_DIR="+sockDir,
	)

	// A script failing between `up` and `down` would otherwise leak a daemon
	// that holds the socket and poisons later runs.
	env.Defer(func() {
		reap(stateDir)
		_ = os.RemoveAll(sockDir)
	})
	return nil
}

// reap force-kills any daemon still recorded in the state dir's lock file.
func reap(stateDir string) {
	b, err := os.ReadFile(filepath.Join(stateDir, "sonata.lock"))
	if err != nil {
		return
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		return
	}
	// Give the kernel a moment so the next script's socket dir is clear.
	time.Sleep(50 * time.Millisecond)
}
