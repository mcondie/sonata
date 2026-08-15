// Package config resolves Sonata's settings from flags, environment,
// config file, and defaults, in that precedence order.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// File names inside the state directory.
const (
	SocketName    = "sonata.sock"
	DatabaseName  = "sonata.db"
	LockName      = "sonata.lock"
	StartLockName = "sonata.start.lock"
	LogName       = "daemon.log"
)

// Config is the fully resolved configuration.
type Config struct {
	StateDir string
	Socket   string
	Database string
	LogLevel string
	// IdleTimeout is applied to daemons this process autostarts, and to
	// `sonata daemon` when its flag is unset. Zero (the default) means the
	// daemon runs until stopped.
	IdleTimeout time.Duration

	// MaxHops dead-letters a message whose causal chain exceeds this many
	// producing actions — the guard that makes cycles safe. Default 100.
	MaxHops int
	// DefaultMaxAttempts is the retry limit for actions that don't set
	// max_attempts. Default 3.
	DefaultMaxAttempts int
	// DefaultTaskTimeout bounds a task subprocess that doesn't set
	// instructions.timeout. Default 5m.
	DefaultTaskTimeout time.Duration
	// BackoffBase and BackoffCap shape the retry schedule:
	// min(base·2^(attempt−1), cap), ±20% jitter. Defaults 1s and 60s.
	BackoffBase time.Duration
	BackoffCap  time.Duration
	// StdoutCap fails a delivery whose subprocess writes more than this many
	// bytes to stdout, rather than truncating silently. Default 4MiB.
	StdoutCap int64
}

// Env returns the SONATA_* environment for a child process that must resolve
// the identical configuration — the detached daemon a CLI spawns. It is the
// single source of truth for propagation: a Config field that is not
// reflected here silently diverges between parent and child, so every new
// field must be added in this function, in the same change.
func (c *Config) Env() []string {
	return []string{
		"SONATA_STATE_DIR=" + c.StateDir,
		"SONATA_SOCKET=" + c.Socket,
		"SONATA_DATABASE=" + c.Database,
		"SONATA_LOG_LEVEL=" + c.LogLevel,
		"SONATA_IDLE_TIMEOUT=" + c.IdleTimeout.String(),
		"SONATA_MAX_HOPS=" + strconv.Itoa(c.MaxHops),
		"SONATA_DEFAULT_MAX_ATTEMPTS=" + strconv.Itoa(c.DefaultMaxAttempts),
		"SONATA_DEFAULT_TASK_TIMEOUT=" + c.DefaultTaskTimeout.String(),
		"SONATA_BACKOFF_BASE=" + c.BackoffBase.String(),
		"SONATA_BACKOFF_CAP=" + c.BackoffCap.String(),
		"SONATA_STDOUT_CAP=" + strconv.FormatInt(c.StdoutCap, 10),
	}
}

// LockPath returns the path of the daemon liveness lock.
func (c *Config) LockPath() string { return filepath.Join(c.StateDir, LockName) }

// StartLockPath returns the path of the lock serializing daemon startup.
func (c *Config) StartLockPath() string { return filepath.Join(c.StateDir, StartLockName) }

// LogPath returns the path the detached daemon writes its output to.
func (c *Config) LogPath() string { return filepath.Join(c.StateDir, LogName) }

// New returns a viper instance wired for Sonata's environment prefix.
func New() *viper.Viper {
	v := viper.New()
	v.SetEnvPrefix("SONATA")
	v.SetEnvKeyReplacer(strings.NewReplacer("-", "_", ".", "_"))
	v.AutomaticEnv()
	return v
}

// Bind connects persistent flags to their config keys. Flags win over
// environment only when actually set, which is viper's behaviour for a bound
// pflag whose Changed is false.
func Bind(v *viper.Viper, fs *pflag.FlagSet) error {
	for key, flag := range map[string]string{
		"state_dir": "state-dir",
		"socket":    "socket",
		"database":  "database",
		"log_level": "log-level",
	} {
		f := fs.Lookup(flag)
		if f == nil {
			continue
		}
		if err := v.BindPFlag(key, f); err != nil {
			return fmt.Errorf("bind %s: %w", flag, err)
		}
	}
	return nil
}

// Load resolves configuration. cfgFile, when non-empty, is used instead of
// searching the default locations.
func Load(v *viper.Viper, cfgFile string) (*Config, error) {
	def, err := DefaultStateDir()
	if err != nil {
		return nil, err
	}
	v.SetDefault("state_dir", def)
	v.SetDefault("log_level", "info")
	v.SetDefault("idle_timeout", time.Duration(0))
	v.SetDefault("max_hops", 100)
	v.SetDefault("default_max_attempts", 3)
	v.SetDefault("default_task_timeout", 5*time.Minute)
	v.SetDefault("backoff_base", time.Second)
	v.SetDefault("backoff_cap", time.Minute)
	v.SetDefault("stdout_cap", int64(4<<20))

	if cfgFile != "" {
		v.SetConfigFile(cfgFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config %s: %w", cfgFile, err)
		}
	} else {
		v.SetConfigName("sonata")
		v.SetConfigType("yaml")
		v.AddConfigPath(".")
		if dir, err := userConfigDir(); err == nil {
			v.SetConfigName("config")
			v.AddConfigPath(dir)
		}
		// A missing config file is not an error.
		var notFound viper.ConfigFileNotFoundError
		if err := v.ReadInConfig(); err != nil && !asConfigNotFound(err, &notFound) {
			return nil, fmt.Errorf("read config: %w", err)
		}
	}

	stateDir, err := expand(v.GetString("state_dir"))
	if err != nil {
		return nil, err
	}
	c := &Config{
		StateDir:           stateDir,
		LogLevel:           v.GetString("log_level"),
		IdleTimeout:        v.GetDuration("idle_timeout"),
		MaxHops:            v.GetInt("max_hops"),
		DefaultMaxAttempts: v.GetInt("default_max_attempts"),
		DefaultTaskTimeout: v.GetDuration("default_task_timeout"),
		BackoffBase:        v.GetDuration("backoff_base"),
		BackoffCap:         v.GetDuration("backoff_cap"),
		StdoutCap:          v.GetInt64("stdout_cap"),
	}
	if c.Socket, err = derive(v.GetString("socket"), stateDir, SocketName); err != nil {
		return nil, err
	}
	if c.Database, err = derive(v.GetString("database"), stateDir, DatabaseName); err != nil {
		return nil, err
	}
	return c, nil
}

// derive returns an explicit path when set, otherwise one under stateDir.
func derive(explicit, stateDir, name string) (string, error) {
	if explicit == "" {
		return filepath.Join(stateDir, name), nil
	}
	return expand(explicit)
}

// DefaultStateDir follows the XDG base directory spec.
func DefaultStateDir() (string, error) {
	if d := os.Getenv("XDG_STATE_HOME"); d != "" {
		return filepath.Join(d, "sonata"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home directory: %w", err)
	}
	return filepath.Join(home, ".local", "state", "sonata"), nil
}

func userConfigDir() (string, error) {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "sonata"), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, ".config", "sonata"), nil
}

// expand resolves a leading ~ and makes the path absolute.
func expand(p string) (string, error) {
	if p == "" {
		return "", nil
	}
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", fmt.Errorf("expand %q: %w", p, err)
		}
		p = filepath.Join(home, strings.TrimPrefix(p, "~"))
	}
	abs, err := filepath.Abs(p)
	if err != nil {
		return "", fmt.Errorf("resolve %q: %w", p, err)
	}
	return abs, nil
}

func asConfigNotFound(err error, target *viper.ConfigFileNotFoundError) bool {
	if e, ok := err.(viper.ConfigFileNotFoundError); ok {
		*target = e
		return true
	}
	return false
}
