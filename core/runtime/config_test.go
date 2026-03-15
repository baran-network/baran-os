package runtime

import (
	"flag"
	"testing"
	"time"
)

func TestConfigFromFlags_Defaults(t *testing.T) {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Int("nats-port", 4222, "")
	fs.String("nats-store-dir", "./baran-data", "")
	fs.Duration("health-interval", 10*time.Second, "")
	fs.Int("health-unhealthy", 3, "")
	fs.Int("health-dead", 6, "")
	fs.Duration("workflow-timeout", 30*time.Second, "")
	fs.String("log-level", "info", "")
	fs.Int("health-port", 8080, "")
	fs.Duration("shutdown-grace", 15*time.Second, "")

	if err := fs.Parse([]string{}); err != nil {
		t.Fatal(err)
	}

	cfg, err := ConfigFromFlags(fs, FlagValues{
		NATSPort: 4222, NATSStoreDir: "./baran-data",
		HeartbeatInterval: 10 * time.Second, UnhealthyThreshold: 3, DeadThreshold: 6,
		WorkflowTimeout: 30 * time.Second, LogLevel: "info", HealthPort: 8080,
		ShutdownGrace: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defaults := DefaultConfig()
	if cfg.NATSPort != defaults.NATSPort {
		t.Errorf("NATSPort = %d, want %d", cfg.NATSPort, defaults.NATSPort)
	}
	if cfg.LogLevel != defaults.LogLevel {
		t.Errorf("LogLevel = %s, want %s", cfg.LogLevel, defaults.LogLevel)
	}
}

func TestConfigFromFlags_FlagOverridesEnv(t *testing.T) {
	t.Setenv("BARAN_NATS_PORT", "6222")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Int("nats-port", 4222, "")
	fs.String("nats-store-dir", "./baran-data", "")
	fs.Duration("health-interval", 10*time.Second, "")
	fs.Int("health-unhealthy", 3, "")
	fs.Int("health-dead", 6, "")
	fs.Duration("workflow-timeout", 30*time.Second, "")
	fs.String("log-level", "info", "")
	fs.Int("health-port", 8080, "")
	fs.Duration("shutdown-grace", 15*time.Second, "")

	if err := fs.Parse([]string{"--nats-port", "5222"}); err != nil {
		t.Fatal(err)
	}

	cfg, err := ConfigFromFlags(fs, FlagValues{
		NATSPort: 5222, NATSStoreDir: "./baran-data",
		HeartbeatInterval: 10 * time.Second, UnhealthyThreshold: 3, DeadThreshold: 6,
		WorkflowTimeout: 30 * time.Second, LogLevel: "info", HealthPort: 8080,
		ShutdownGrace: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.NATSPort != 5222 {
		t.Errorf("NATSPort = %d, want 5222 (flag should override env)", cfg.NATSPort)
	}
}

func TestConfigEnvFallback(t *testing.T) {
	t.Setenv("BARAN_NATS_PORT", "6222")
	t.Setenv("BARAN_LOG_LEVEL", "debug")

	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.Int("nats-port", 4222, "")
	fs.String("nats-store-dir", "./baran-data", "")
	fs.Duration("health-interval", 10*time.Second, "")
	fs.Int("health-unhealthy", 3, "")
	fs.Int("health-dead", 6, "")
	fs.Duration("workflow-timeout", 30*time.Second, "")
	fs.String("log-level", "info", "")
	fs.Int("health-port", 8080, "")
	fs.Duration("shutdown-grace", 15*time.Second, "")

	// No flags explicitly set
	if err := fs.Parse([]string{}); err != nil {
		t.Fatal(err)
	}

	cfg, err := ConfigFromFlags(fs, FlagValues{
		NATSPort: 4222, NATSStoreDir: "./baran-data",
		HeartbeatInterval: 10 * time.Second, UnhealthyThreshold: 3, DeadThreshold: 6,
		WorkflowTimeout: 30 * time.Second, LogLevel: "info", HealthPort: 8080,
		ShutdownGrace: 15 * time.Second,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if cfg.NATSPort != 6222 {
		t.Errorf("NATSPort = %d, want 6222 (env should override default)", cfg.NATSPort)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("LogLevel = %s, want debug (env should override default)", cfg.LogLevel)
	}
}

func TestConfigValidation_InvalidPort(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NATSPort = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for port 0")
	}
}

func TestConfigValidation_BadLogLevel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LogLevel = "verbose"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid log level")
	}
}

func TestConfigValidation_DeadLessThanUnhealthy(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DeadThreshold = 2 // less than UnhealthyThreshold=3
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when dead <= unhealthy")
	}
}

func TestConfigValidation_SamePorts(t *testing.T) {
	cfg := DefaultConfig()
	cfg.NATSPort = 8080
	cfg.HealthPort = 8080
	if err := cfg.Validate(); err == nil {
		t.Error("expected error when nats-port equals health-port")
	}
}
