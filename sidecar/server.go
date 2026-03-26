package sidecar

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natsBus "github.com/baran-network/baran-os/core/eventbus/nats"
)

// Server is the sidecar HTTP server.
type Server struct {
	cfg      *SidecarConfig
	logger   *slog.Logger
	mux      *http.ServeMux
	manager  *AgentManager
	registry *PayloadRegistry
}

// NewServer creates a new sidecar Server. It connects to NATS and initialises
// the agent manager and payload registry.
func NewServer(cfg *SidecarConfig, logger *slog.Logger) (*Server, error) {
	ctx := context.Background()

	bus, err := natsBus.New(ctx, cfg.NATSUrl)
	if err != nil {
		return nil, fmt.Errorf("connect to NATS at %s: %w", cfg.NATSUrl, err)
	}

	return NewServerWithBus(cfg, bus, logger), nil
}

// NewServerWithBus constructs a Server from an already-created EventBus.
// Used by NewServer (production) and tests.
func NewServerWithBus(cfg *SidecarConfig, bus eventbus.EventBus, logger *slog.Logger) *Server {
	registry := NewPayloadRegistry()
	manager := NewAgentManager(cfg, bus, registry, logger)

	s := &Server{
		cfg:      cfg,
		logger:   logger,
		mux:      http.NewServeMux(),
		manager:  manager,
		registry: registry,
	}
	s.registerRoutes()
	return s
}

// Handler returns the Server's HTTP handler (for use in tests with httptest).
func (s *Server) Handler() http.Handler {
	return corsMiddleware(s.mux)
}

// registerRoutes wires all HTTP endpoints.
func (s *Server) registerRoutes() {
	authed := func(h http.HandlerFunc) http.Handler {
		return authMiddleware(s.cfg.PSK, h)
	}

	s.mux.Handle("GET /health", authed(s.handleHealth))
	s.mux.Handle("POST /agents", authed(s.handleRegister))
	s.mux.Handle("DELETE /agents/{id}", authed(s.handleDeregister))
	s.mux.Handle("POST /agents/{id}/events", authed(s.handlePublish))
	s.mux.Handle("GET /agents/{id}/events", authed(s.handleSubscribeSSE))
	s.mux.Handle("POST /agents/{id}/ack", authed(s.handleAck))
	s.mux.Handle("GET /agents/{id}/ws", authed(s.handleWebSocket))
}

// Run starts the HTTP server and blocks until SIGINT/SIGTERM or ctx cancellation.
func (s *Server) Run(ctx context.Context) error {
	addr := fmt.Sprintf(":%d", s.cfg.Port)
	httpServer := &http.Server{
		Addr:    addr,
		Handler: corsMiddleware(s.mux),
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("sidecar listening", "addr", addr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case sig := <-sigCh:
		s.logger.Info("signal received, shutting down", "signal", sig)
	case err := <-errCh:
		return err
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownGrace)
	defer cancel()

	s.manager.StopAll(shutdownCtx)

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	s.logger.Info("sidecar stopped")
	return nil
}

// corsMiddleware adds CORS headers to allow cross-origin requests from SDK clients.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Authorization, Content-Type, Last-Event-ID")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// errorResponse is the standard JSON error body.
type errorResponse struct {
	Error   string `json:"error"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

// writeError writes a JSON error response with the given HTTP status code.
func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(errorResponse{
		Error:   http.StatusText(status),
		Code:    code,
		Message: message,
	})
}

// writeJSON writes a JSON response with status 200.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// now returns the current time (injectable in tests).
var now = func() time.Time { return time.Now() }
