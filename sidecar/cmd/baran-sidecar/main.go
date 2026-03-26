package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/baran-network/baran-os/sidecar"
)

func main() {
	cfg := sidecar.ParseConfig()

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
	slog.SetDefault(logger)

	srv, err := sidecar.NewServer(cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create sidecar server: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	if err := srv.Run(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "sidecar error: %v\n", err)
		os.Exit(1)
	}
}
