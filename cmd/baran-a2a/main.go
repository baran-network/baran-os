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

	if err := srv.Run(context.Background()); err != nil {
		logger.Error("server error", "error", err)
		os.Exit(1)
	}
}
