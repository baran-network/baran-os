package a2a

import (
	"flag"
	"os"
	"strconv"
	"time"
)

// GatewayConfig holds the resolved configuration for the A2A gateway.
// Precedence: command-line flags > environment variables > defaults.
type GatewayConfig struct {
	NATSUrl       string
	A2APort       int
	PSK           string
	LogLevel      string
	ShutdownGrace time.Duration
}

// ParseConfig resolves configuration following flag > env > default precedence.
func ParseConfig() *GatewayConfig {
	natsURL := flag.String("nats-url", envStr("BARAN_A2A_NATS_URL", "nats://localhost:4222"), "NATS server URL")
	port := flag.Int("a2a-port", envInt("BARAN_A2A_PORT", 8090), "A2A HTTP server port")
	psk := flag.String("psk", envStr("BARAN_A2A_PSK", ""), "Pre-shared key for API authentication")
	logLevel := flag.String("log-level", envStr("BARAN_A2A_LOG_LEVEL", "info"), "Log verbosity (debug|info|warn|error)")
	graceSec := flag.Int("shutdown-grace", envInt("BARAN_A2A_SHUTDOWN_GRACE_SEC", 10), "Graceful shutdown period in seconds")

	flag.Parse()

	return &GatewayConfig{
		NATSUrl:       *natsURL,
		A2APort:       *port,
		PSK:           *psk,
		LogLevel:      *logLevel,
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
