package federation

import (
	"context"
	"fmt"
	"net/url"
	"sync"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// TransportSubscription represents an active subscription on the federation transport.
type TransportSubscription interface {
	Unsubscribe() error
}

// Transport is the interface for inter-node communication.
// Implementations handle the underlying connection mechanism (e.g., NATS leaf nodes).
type Transport interface {
	// Connect establishes the federation transport using the given NATS connection.
	// The connection should already be connected to a NATS server configured with
	// leaf node remotes (via LeafNodeServerOptions).
	Connect(ctx context.Context, nc *nats.Conn) error

	// Publish sends data to a federation subject. Because the underlying NATS server
	// is configured with leaf node connections, messages automatically flow to peers.
	Publish(ctx context.Context, subject string, data []byte) error

	// Subscribe registers a handler for messages on a federation subject.
	Subscribe(ctx context.Context, subject string, handler func([]byte)) (TransportSubscription, error)

	// Close unsubscribes all and releases resources.
	Close() error
}

// NATSLeafTransport implements Transport using the local NATS connection.
// Federation subjects flow between nodes via NATS leaf node connections
// configured at the server level.
type NATSLeafTransport struct {
	nc   *nats.Conn
	subs []*nats.Subscription
	mu   sync.Mutex
}

// NewNATSLeafTransport creates a new transport.
// Call Connect() to initialize with a NATS connection.
func NewNATSLeafTransport() *NATSLeafTransport {
	return &NATSLeafTransport{}
}

func (t *NATSLeafTransport) Connect(_ context.Context, nc *nats.Conn) error {
	if nc == nil {
		return fmt.Errorf("nats connection is nil")
	}
	t.mu.Lock()
	t.nc = nc
	t.mu.Unlock()
	return nil
}

func (t *NATSLeafTransport) Publish(_ context.Context, subject string, data []byte) error {
	t.mu.Lock()
	nc := t.nc
	t.mu.Unlock()
	if nc == nil {
		return fmt.Errorf("transport not connected")
	}
	return nc.Publish(subject, data)
}

func (t *NATSLeafTransport) Subscribe(_ context.Context, subject string, handler func([]byte)) (TransportSubscription, error) {
	t.mu.Lock()
	nc := t.nc
	t.mu.Unlock()
	if nc == nil {
		return nil, fmt.Errorf("transport not connected")
	}

	sub, err := nc.Subscribe(subject, func(msg *nats.Msg) {
		handler(msg.Data)
	})
	if err != nil {
		return nil, fmt.Errorf("subscribe %s: %w", subject, err)
	}

	t.mu.Lock()
	t.subs = append(t.subs, sub)
	t.mu.Unlock()

	return &natsTransportSub{sub: sub}, nil
}

func (t *NATSLeafTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	for _, sub := range t.subs {
		_ = sub.Unsubscribe()
	}
	t.subs = nil
	t.nc = nil
	return nil
}

// natsTransportSub wraps a NATS subscription.
type natsTransportSub struct {
	sub *nats.Subscription
}

func (s *natsTransportSub) Unsubscribe() error {
	return s.sub.Unsubscribe()
}

// LeafNodeServerOptions returns NATS server leaf node configuration.
// This must be merged into the embedded NATS server options BEFORE starting it.
// When seeds is empty, returns nil (standalone mode — no leaf node configuration).
func LeafNodeServerOptions(cfg GatewayConfig) *natsserver.LeafNodeOpts {
	if len(cfg.Seeds) == 0 {
		return nil
	}

	var remotes []*natsserver.RemoteLeafOpts
	for _, seed := range cfg.Seeds {
		remote := &natsserver.RemoteLeafOpts{
			URLs: []*url.URL{parseNATSURL(seed)},
		}
		remotes = append(remotes, remote)
	}

	return &natsserver.LeafNodeOpts{
		Port:    cfg.LeafPort,
		Remotes: remotes,
	}
}

// parseNATSURL converts a host:port string into a net/url.URL for NATS.
func parseNATSURL(addr string) *url.URL {
	u, err := url.Parse("nats://" + addr)
	if err != nil {
		return &url.URL{Scheme: "nats", Host: addr}
	}
	return u
}
