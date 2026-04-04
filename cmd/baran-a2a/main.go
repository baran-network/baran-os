package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/baran-network/baran-os/a2a"
)

func main() {
	cfg := a2a.ParseConfig()

	level := slog.LevelInfo
	switch cfg.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: level}))

	srv, err := a2a.NewServer(cfg, logger)
	if err != nil {
		logger.Error("failed to create server", "error", err)
		os.Exit(1)
	}

	ctx := context.Background()

	// Onboard external A2A agents configured in the config file.
	if len(cfg.ExternalAgents) > 0 {
		extMgr := srv.ExternalAgentManager()
		for _, extCfg := range cfg.ExternalAgents {
			agentID, err := extMgr.OnboardExternalAgent(ctx, extCfg)
			if err != nil {
				logger.Error("failed to onboard external agent",
					"name", extCfg.Name,
					"endpoint", extCfg.Endpoint,
					"error", err,
				)
				continue
			}
			extMgr.StartHealthPolling(ctx, agentID, extCfg.Endpoint, extCfg.PollInterval)
		}
	}

	if err := srv.Run(ctx); err != nil {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
