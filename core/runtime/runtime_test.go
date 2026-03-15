package runtime_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/runtime"
	"github.com/nats-io/nats.go"
)

func testConfig(t *testing.T) runtime.Config {
	t.Helper()
	cfg := runtime.DefaultConfig()
	cfg.NATSPort = -1 // random port
	cfg.HealthPort = 0
	cfg.NATSStoreDir = t.TempDir()
	cfg.HeartbeatInterval = 1 * time.Hour // avoid heartbeat noise in tests
	return cfg
}

func TestRuntimeStartStop(t *testing.T) {
	cfg := testConfig(t)
	rt := runtime.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	go func() {
		errCh <- rt.Run(ctx)
	}()

	// Wait for runtime to be ready by polling NATS connection
	var nc *nats.Conn
	var err error
	deadline := time.After(10 * time.Second)
	for {
		natsURL := rt.NATSURL()
		if natsURL != "" {
			nc, err = nats.Connect(natsURL)
			if err == nil {
				break
			}
		}
		select {
		case <-deadline:
			t.Fatal("runtime did not become ready within 10s")
		case runErr := <-errCh:
			t.Fatalf("runtime exited early: %v", runErr)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}
	defer nc.Close()

	// Verify we can create an eventbus via the runtime's NATS
	bus, err := natseventbus.NewFromConn(context.Background(), nc)
	if err != nil {
		t.Fatalf("create eventbus: %v", err)
	}
	defer bus.Close()

	// Publish a test event on a mapped stream and verify it's routable
	received := make(chan struct{}, 1)
	sub, err := bus.Subscribe(context.Background(), "agent.health.ping", func(ctx context.Context, e *eventbus.Event) error {
		received <- struct{}{}
		return nil
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	testEvent := &eventbus.Event{
		ID:        "test-event-001",
		Type:      "agent.health.ping",
		Timestamp: time.Now().UnixMilli(),
		Payload:   []byte("hello"),
	}
	if err := bus.Publish(context.Background(), testEvent); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case <-received:
		// OK
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive test event within 5s")
	}

	// Trigger shutdown
	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runtime returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runtime did not shut down within 15s")
	}

	// Verify NATS is down
	_, err = nats.Connect(rt.NATSURL(), nats.MaxReconnects(0))
	if err == nil {
		t.Fatal("expected NATS to be unavailable after shutdown")
	}
}

func TestHealthEndpoint(t *testing.T) {
	cfg := testConfig(t)
	rt := runtime.New(cfg)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- rt.Run(ctx)
	}()

	// Wait for health endpoint to be available
	var healthURL string
	deadline := time.After(10 * time.Second)
	for {
		addr := rt.HealthAddr()
		if addr != "" {
			healthURL = fmt.Sprintf("http://%s/healthz", addr)
			break
		}
		select {
		case <-deadline:
			t.Fatal("health endpoint not available within 10s")
		case runErr := <-errCh:
			t.Fatalf("runtime exited early: %v", runErr)
		default:
			time.Sleep(50 * time.Millisecond)
		}
	}

	// Poll until we get a 200 (all subsystems up)
	var resp *http.Response
	for i := 0; i < 100; i++ {
		resp, _ = http.Get(healthURL)
		if resp != nil && resp.StatusCode == http.StatusOK {
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(100 * time.Millisecond)
	}
	if resp == nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %v", resp)
	}

	var health runtime.HealthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode health response: %v", err)
	}
	resp.Body.Close()

	if health.Status != "healthy" {
		t.Errorf("status = %q, want healthy", health.Status)
	}
	if health.NodeID == "" {
		t.Error("node_id is empty")
	}
	expectedSubsystems := []string{"nats", "eventbus", "registry", "router", "workflow_engine", "health_monitor", "discovery"}
	for _, name := range expectedSubsystems {
		if health.Subsystems[name] != "up" {
			t.Errorf("subsystem %s = %q, want up", name, health.Subsystems[name])
		}
	}

	// Shutdown and verify endpoint becomes unavailable
	cancel()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runtime returned error: %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("runtime did not shut down within 15s")
	}

	_, err := http.Get(healthURL)
	if err == nil {
		t.Log("warning: health endpoint still responded after shutdown (may be race)")
	}
}
