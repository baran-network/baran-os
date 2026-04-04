package a2a

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

// ExternalAgentConfig holds the connection details for an external A2A agent.
type ExternalAgentConfig struct {
	Name          string            `yaml:"name"           json:"name"`
	Endpoint      string            `yaml:"endpoint"       json:"endpoint"`
	PollInterval  time.Duration     `yaml:"poll_interval"  json:"poll_interval"`
	SkillsMapping map[string]string `yaml:"skills_mapping" json:"skills_mapping"`
}

// gatewayFileConfig is the YAML file format for the gateway configuration.
type gatewayFileConfig struct {
	ExternalAgents []ExternalAgentConfig `yaml:"external_agents"`
}

// GatewayConfig holds the resolved configuration for the A2A gateway.
// Precedence: command-line flags > environment variables > defaults.
type GatewayConfig struct {
	NATSUrl        string
	A2APort        int
	PSK            string
	LogLevel       string
	ShutdownGrace  time.Duration
	ExternalAgents []ExternalAgentConfig
}

// ParseConfig resolves configuration following flag > env > default precedence.
// If --config points to a YAML file, external_agents entries are loaded from it.
func ParseConfig() *GatewayConfig {
	natsURL := flag.String("nats-url", envStr("BARAN_A2A_NATS_URL", "nats://localhost:4222"), "NATS server URL")
	port := flag.Int("a2a-port", envInt("BARAN_A2A_PORT", 8090), "A2A HTTP server port")
	psk := flag.String("psk", envStr("BARAN_A2A_PSK", ""), "Pre-shared key for API authentication")
	logLevel := flag.String("log-level", envStr("BARAN_A2A_LOG_LEVEL", "info"), "Log verbosity (debug|info|warn|error)")
	graceSec := flag.Int("shutdown-grace", envInt("BARAN_A2A_SHUTDOWN_GRACE_SEC", 10), "Graceful shutdown period in seconds")
	configPath := flag.String("config", envStr("BARAN_A2A_CONFIG", ""), "Path to YAML config file (optional)")

	flag.Parse()

	cfg := &GatewayConfig{
		NATSUrl:       *natsURL,
		A2APort:       *port,
		PSK:           *psk,
		LogLevel:      *logLevel,
		ShutdownGrace: time.Duration(*graceSec) * time.Second,
	}

	if *configPath != "" {
		if err := loadFileConfig(cfg, *configPath); err != nil {
			fmt.Fprintf(os.Stderr, "warning: failed to load config file %s: %v\n", *configPath, err)
		}
	}

	return cfg
}

// loadFileConfig reads the YAML config file and merges external_agents into cfg.
func loadFileConfig(cfg *GatewayConfig, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read file: %w", err)
	}

	var fileCfg gatewayFileConfig
	if err := yaml.Unmarshal(data, &fileCfg); err != nil {
		return fmt.Errorf("parse YAML: %w", err)
	}

	cfg.ExternalAgents = fileCfg.ExternalAgents
	return nil
}

func envStr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
