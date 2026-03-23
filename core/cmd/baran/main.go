package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/baran-network/baran-os/core/runtime"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	fs := flag.NewFlagSet("baran", flag.ExitOnError)

	natsPort := fs.Int("nats-port", 4222, "NATS server listen port")
	natsStoreDir := fs.String("nats-store-dir", "./baran-data", "JetStream data directory")
	healthInterval := fs.Duration("health-interval", 10*time.Second, "Health monitor heartbeat interval")
	healthUnhealthy := fs.Int("health-unhealthy", 3, "Missed heartbeats before UNHEALTHY")
	healthDead := fs.Int("health-dead", 6, "Missed heartbeats before DEAD")
	workflowTimeout := fs.Duration("workflow-timeout", 30*time.Second, "Default workflow step timeout")
	logLevel := fs.String("log-level", "info", "Log level (debug, info, warn, error)")
	healthPort := fs.Int("health-port", 8080, "HTTP health endpoint port")
	shutdownGrace := fs.Duration("shutdown-grace", 15*time.Second, "Graceful shutdown timeout")
	federationSeeds := fs.String("federation-seeds", "", "Comma-separated seed node addresses (host:port)")
	federationPSK := fs.String("federation-psk", "", "Pre-shared key for inter-node authentication")
	federationHeartbeat := fs.Duration("federation-heartbeat", 10*time.Second, "Inter-node heartbeat interval")
	federationUnhealthy := fs.Int("federation-unhealthy", 3, "Missed heartbeats before node is UNHEALTHY")
	federationDead := fs.Int("federation-dead", 6, "Missed heartbeats before node is DEAD")
	federationRelayTimeout := fs.Duration("federation-relay-timeout", 30*time.Second, "Max wait time for relay response")
	federationLeafPort := fs.Int("federation-leaf-port", 7422, "NATS leaf node listener port")
	federationCleanupTTL := fs.Duration("federation-cleanup-ttl", 5*time.Minute, "TTL before dead nodes are removed")
	showVersion := fs.Bool("version", false, "Print version and exit")

	if err := fs.Parse(os.Args[1:]); err != nil {
		os.Exit(1)
	}

	if *showVersion {
		fmt.Printf("baran version %s (%s) built %s\n", version, commit, buildDate)
		os.Exit(0)
	}

	// Build config: flag value if explicitly set, else env var, else default.
	cfg, err := runtime.ConfigFromFlags(fs, runtime.FlagValues{
		NATSPort:           *natsPort,
		NATSStoreDir:       *natsStoreDir,
		HeartbeatInterval:  *healthInterval,
		UnhealthyThreshold: int32(*healthUnhealthy),
		DeadThreshold:      int32(*healthDead),
		WorkflowTimeout:    *workflowTimeout,
		LogLevel:           *logLevel,
		HealthPort:         *healthPort,
		ShutdownGrace:      *shutdownGrace,

		FederationSeeds:              *federationSeeds,
		FederationPSK:                *federationPSK,
		FederationHeartbeatInterval:  *federationHeartbeat,
		FederationUnhealthyThreshold: int32(*federationUnhealthy),
		FederationDeadThreshold:      int32(*federationDead),
		FederationRelayTimeout:       *federationRelayTimeout,
		FederationLeafPort:           *federationLeafPort,
		FederationCleanupTTL:         *federationCleanupTTL,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid configuration: %v\n", err)
		os.Exit(1)
	}

	// Signal handling: first signal cancels context, second force-exits.
	ctx, cancel := context.WithCancel(context.Background())

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigCh
		cancel()
		<-sigCh
		fmt.Fprintln(os.Stderr, "forced shutdown")
		os.Exit(1)
	}()

	rt := runtime.New(cfg)
	if err := rt.Run(ctx); err != nil {
		cancel()
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
	cancel()
}
