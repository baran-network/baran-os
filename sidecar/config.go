package sidecar

import (
	"flag"
	"os"
	"strconv"
	"time"
)

// SidecarConfig holds the resolved configuration for the sidecar process.
// Precedence: command-line flags > environment variables > defaults.
type SidecarConfig struct {
	Port          int
	NATSUrl       string
	PSK           string
	LogLevel      string
	MaxAgents     int
	ShutdownGrace time.Duration
}

// ParseConfig resolves configuration following flag > env > default precedence.
// Call after flag.Parse() has run (this function calls flag.Parse internally).
func ParseConfig() *SidecarConfig {
	port := flag.Int("port", envInt("BARAN_SIDECAR_PORT", 9090), "HTTP server port")
	natsURL := flag.String("nats-url", envStr("BARAN_SIDECAR_NATS_URL", "nats://localhost:4222"), "NATS server URL")
	psk := flag.String("psk", envStr("BARAN_SIDECAR_PSK", ""), "Pre-shared key for API authentication")
	logLevel := flag.String("log-level", envStr("BARAN_SIDECAR_LOG_LEVEL", "info"), "Log verbosity (debug|info|warn|error)")
	maxAgents := flag.Int("max-agents", envInt("BARAN_SIDECAR_MAX_AGENTS", 50), "Maximum concurrent agent registrations")
	graceSec := flag.Int("shutdown-grace", envInt("BARAN_SIDECAR_SHUTDOWN_GRACE_SEC", 10), "Graceful shutdown period in seconds")

	flag.Parse()

	return &SidecarConfig{
		Port:          *port,
		NATSUrl:       *natsURL,
		PSK:           *psk,
		LogLevel:      *logLevel,
		MaxAgents:     *maxAgents,
		ShutdownGrace: time.Duration(*graceSec) * time.Second,
	}
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
