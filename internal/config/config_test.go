package config

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

// newFlags mirrors the persistent flags declared in the CLI root.
func newFlags() *pflag.FlagSet {
	fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
	fs.String("state-dir", "", "")
	fs.String("socket", "", "")
	fs.String("database", "", "")
	fs.String("log-level", "", "")
	return fs
}

// load runs the full resolution path with the given flag arguments.
func load(t *testing.T, args []string, cfgFile string) *Config {
	t.Helper()
	fs := newFlags()
	if err := fs.Parse(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	v := New()
	if err := Bind(v, fs); err != nil {
		t.Fatalf("bind: %v", err)
	}
	cfg, err := Load(v, cfgFile)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	return cfg
}

// isolate points config discovery at an empty directory so a real
// ~/.config/sonata/config.yaml cannot influence the test.
func isolate(t *testing.T) {
	t.Helper()
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	t.Chdir(t.TempDir())
}

func TestDefaults(t *testing.T) {
	isolate(t)
	state := os.Getenv("XDG_STATE_HOME")

	cfg := load(t, nil, "")

	if want := filepath.Join(state, "sonata"); cfg.StateDir != want {
		t.Errorf("StateDir = %q, want %q", cfg.StateDir, want)
	}
	if want := filepath.Join(cfg.StateDir, SocketName); cfg.Socket != want {
		t.Errorf("Socket = %q, want %q", cfg.Socket, want)
	}
	if want := filepath.Join(cfg.StateDir, DatabaseName); cfg.Database != want {
		t.Errorf("Database = %q, want %q", cfg.Database, want)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("LogLevel = %q, want info", cfg.LogLevel)
	}
}

func TestEnvBeatsDefault(t *testing.T) {
	isolate(t)
	t.Setenv("SONATA_STATE_DIR", "/tmp/env-state")
	t.Setenv("SONATA_LOG_LEVEL", "warn")

	cfg := load(t, nil, "")

	if cfg.StateDir != "/tmp/env-state" {
		t.Errorf("StateDir = %q, want /tmp/env-state", cfg.StateDir)
	}
	if cfg.LogLevel != "warn" {
		t.Errorf("LogLevel = %q, want warn", cfg.LogLevel)
	}
	// Derived paths must follow the overridden state dir.
	if want := filepath.Join("/tmp/env-state", SocketName); cfg.Socket != want {
		t.Errorf("Socket = %q, want %q", cfg.Socket, want)
	}
}

func TestFlagBeatsEnv(t *testing.T) {
	isolate(t)
	t.Setenv("SONATA_STATE_DIR", "/tmp/env-state")

	cfg := load(t, []string{"--state-dir", "/tmp/flag-state"}, "")

	if cfg.StateDir != "/tmp/flag-state" {
		t.Errorf("StateDir = %q, want /tmp/flag-state", cfg.StateDir)
	}
}

func TestFileBeatsDefaultAndLosesToEnv(t *testing.T) {
	isolate(t)
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "sonata.yaml")
	body := "state_dir: /tmp/file-state\nlog_level: error\n"
	if err := os.WriteFile(cfgFile, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg := load(t, nil, cfgFile)
	if cfg.StateDir != "/tmp/file-state" {
		t.Errorf("StateDir = %q, want /tmp/file-state", cfg.StateDir)
	}
	if cfg.LogLevel != "error" {
		t.Errorf("LogLevel = %q, want error", cfg.LogLevel)
	}

	t.Setenv("SONATA_STATE_DIR", "/tmp/env-state")
	cfg = load(t, nil, cfgFile)
	if cfg.StateDir != "/tmp/env-state" {
		t.Errorf("env should beat file: StateDir = %q", cfg.StateDir)
	}
}

func TestExplicitSocketOverridesDerived(t *testing.T) {
	isolate(t)

	cfg := load(t, []string{"--state-dir", "/tmp/state", "--socket", "/tmp/custom.sock"}, "")

	if cfg.Socket != "/tmp/custom.sock" {
		t.Errorf("Socket = %q, want /tmp/custom.sock", cfg.Socket)
	}
	// Database is still derived from the state dir.
	if want := filepath.Join("/tmp/state", DatabaseName); cfg.Database != want {
		t.Errorf("Database = %q, want %q", cfg.Database, want)
	}
}

func TestMissingConfigFileIsNotAnError(t *testing.T) {
	isolate(t)
	// No sonata.yaml anywhere: resolution should fall back to defaults.
	if cfg := load(t, nil, ""); cfg.StateDir == "" {
		t.Error("StateDir should be populated from defaults")
	}
}

func TestExplicitMissingConfigFileIsAnError(t *testing.T) {
	isolate(t)
	fs := newFlags()
	v := New()
	if err := Bind(v, fs); err != nil {
		t.Fatalf("bind: %v", err)
	}
	// Naming a file that does not exist is a user error, unlike relying on
	// the default search path.
	if _, err := Load(v, filepath.Join(t.TempDir(), "nope.yaml")); err == nil {
		t.Fatal("want error for an explicitly named missing config file")
	}
}

func TestTildeExpansion(t *testing.T) {
	isolate(t)
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skip("no home directory")
	}

	cfg := load(t, []string{"--state-dir", "~/sonata-test"}, "")

	if want := filepath.Join(home, "sonata-test"); cfg.StateDir != want {
		t.Errorf("StateDir = %q, want %q", cfg.StateDir, want)
	}
}

func TestDerivedPathsAreAbsolute(t *testing.T) {
	isolate(t)

	cfg := load(t, []string{"--state-dir", "relative/path"}, "")

	if !filepath.IsAbs(cfg.StateDir) {
		t.Errorf("StateDir = %q, want an absolute path", cfg.StateDir)
	}
}

// Env must cover every Config field: a field missing there silently diverges
// between the CLI and the daemon it spawns. Adding a Config field breaks this
// test until Env is updated — that is the point.
func TestEnvCoversEveryConfigField(t *testing.T) {
	fields := reflect.TypeOf(Config{}).NumField()
	c := &Config{}
	if got := len(c.Env()); got != fields {
		t.Fatalf("Env returns %d vars but Config has %d fields; update Config.Env", got, fields)
	}
	for _, kv := range c.Env() {
		if !strings.HasPrefix(kv, "SONATA_") || !strings.Contains(kv, "=") {
			t.Errorf("malformed env entry %q", kv)
		}
	}
}
