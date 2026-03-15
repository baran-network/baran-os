package runtime

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds the resolved configuration for the runtime. Values are merged
// from defaults, environment variables, and CLI flags (ascending precedence).
type Config struct {
	NATSPort           int
	NATSStoreDir       string
	HeartbeatInterval  time.Duration
	UnhealthyThreshold int32
	DeadThreshold      int32
	WorkflowTimeout    time.Duration
	LogLevel           string
	HealthPort         int
	ShutdownGrace      time.Duration
}

// DefaultConfig returns a Config with all default values.
func DefaultConfig() Config {
	return Config{
		NATSPort:           4222,
		NATSStoreDir:       "./baran-data",
		HeartbeatInterval:  10 * time.Second,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
		WorkflowTimeout:    30 * time.Second,
		LogLevel:           "info",
		HealthPort:         8080,
		ShutdownGrace:      15 * time.Second,
	}
}

// Validate checks that the configuration values are within acceptable ranges.
func (c Config) Validate() error {
	if c.NATSPort < 1 || c.NATSPort > 65535 {
		return fmt.Errorf("nats-port must be between 1 and 65535, got %d", c.NATSPort)
	}
	if c.HealthPort < 1 || c.HealthPort > 65535 {
		return fmt.Errorf("health-port must be between 1 and 65535, got %d", c.HealthPort)
	}
	if c.NATSPort == c.HealthPort {
		return fmt.Errorf("nats-port and health-port must differ, both are %d", c.NATSPort)
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("health-interval must be positive, got %s", c.HeartbeatInterval)
	}
	if c.UnhealthyThreshold <= 0 {
		return fmt.Errorf("health-unhealthy must be positive, got %d", c.UnhealthyThreshold)
	}
	if c.DeadThreshold <= c.UnhealthyThreshold {
		return fmt.Errorf("health-dead (%d) must be greater than health-unhealthy (%d)", c.DeadThreshold, c.UnhealthyThreshold)
	}
	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("log-level must be one of debug, info, warn, error; got %q", c.LogLevel)
	}
	if c.ShutdownGrace <= 0 {
		return fmt.Errorf("shutdown-grace must be positive, got %s", c.ShutdownGrace)
	}
	return nil
}

// FlagValues holds the parsed flag values from the CLI. Used by ConfigFromFlags
// to determine whether a flag was explicitly set or should fall back to env/default.
type FlagValues struct {
	NATSPort           int
	NATSStoreDir       string
	HeartbeatInterval  time.Duration
	UnhealthyThreshold int32
	DeadThreshold      int32
	WorkflowTimeout    time.Duration
	LogLevel           string
	HealthPort         int
	ShutdownGrace      time.Duration
}

// ConfigFromFlags builds a Config by applying precedence: flag > env > default.
// If a flag was explicitly set by the user, its value is used. Otherwise, the
// corresponding BARAN_* environment variable is checked. If neither is set, the
// compiled default is used.
func ConfigFromFlags(fs *flag.FlagSet, fv FlagValues) (Config, error) {
	defaults := DefaultConfig()

	cfg := Config{
		NATSPort:           resolveInt(fs, "nats-port", fv.NATSPort, "BARAN_NATS_PORT", defaults.NATSPort),
		NATSStoreDir:       resolveString(fs, "nats-store-dir", fv.NATSStoreDir, "BARAN_NATS_STORE_DIR", defaults.NATSStoreDir),
		HeartbeatInterval:  resolveDuration(fs, "health-interval", fv.HeartbeatInterval, "BARAN_HEALTH_INTERVAL", defaults.HeartbeatInterval),
		UnhealthyThreshold: resolveInt32(fs, "health-unhealthy", fv.UnhealthyThreshold, "BARAN_HEALTH_UNHEALTHY", defaults.UnhealthyThreshold),
		DeadThreshold:      resolveInt32(fs, "health-dead", fv.DeadThreshold, "BARAN_HEALTH_DEAD", defaults.DeadThreshold),
		WorkflowTimeout:    resolveDuration(fs, "workflow-timeout", fv.WorkflowTimeout, "BARAN_WORKFLOW_TIMEOUT", defaults.WorkflowTimeout),
		LogLevel:           resolveString(fs, "log-level", fv.LogLevel, "BARAN_LOG_LEVEL", defaults.LogLevel),
		HealthPort:         resolveInt(fs, "health-port", fv.HealthPort, "BARAN_HEALTH_PORT", defaults.HealthPort),
		ShutdownGrace:      resolveDuration(fs, "shutdown-grace", fv.ShutdownGrace, "BARAN_SHUTDOWN_GRACE", defaults.ShutdownGrace),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// flagWasSet returns true if the named flag was explicitly provided on the command line.
func flagWasSet(fs *flag.FlagSet, name string) bool {
	set := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			set = true
		}
	})
	return set
}

func resolveInt(fs *flag.FlagSet, flagName string, flagVal int, envKey string, def int) int {
	if flagWasSet(fs, flagName) {
		return flagVal
	}
	return envOrInt(envKey, def)
}

func resolveInt32(fs *flag.FlagSet, flagName string, flagVal int32, envKey string, def int32) int32 {
	if flagWasSet(fs, flagName) {
		return flagVal
	}
	return envOrInt32(envKey, def)
}

func resolveString(fs *flag.FlagSet, flagName, flagVal, envKey, def string) string {
	if flagWasSet(fs, flagName) {
		return flagVal
	}
	return envOrString(envKey, def)
}

func resolveDuration(fs *flag.FlagSet, flagName string, flagVal time.Duration, envKey string, def time.Duration) time.Duration {
	if flagWasSet(fs, flagName) {
		return flagVal
	}
	return envOrDuration(envKey, def)
}

// envOrInt returns the integer value of the environment variable key, or
// fallback if the variable is unset or cannot be parsed.
func envOrInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

// envOrInt32 returns the int32 value of the environment variable key, or
// fallback if the variable is unset or cannot be parsed.
func envOrInt32(key string, fallback int32) int32 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseInt(v, 10, 32)
	if err != nil {
		return fallback
	}
	return int32(n)
}

// envOrString returns the string value of the environment variable key, or
// fallback if the variable is unset.
func envOrString(key, fallback string) string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	return v
}

// envOrDuration returns the duration value of the environment variable key, or
// fallback if the variable is unset or cannot be parsed.
func envOrDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
