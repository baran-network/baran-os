package testutil

import (
	"fmt"
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// StartNATS starts an embedded NATS server with JetStream enabled on a random
// port. It registers a cleanup function that shuts down the server when the
// test completes. Returns the running server and a connected nats.Conn.
func StartNATS(t *testing.T) (*natsserver.Server, *nats.Conn) {
	t.Helper()

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1, // random port
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}

	s, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create NATS server: %v", err)
	}

	s.Start()

	if !s.ReadyForConnections(5_000_000_000) { // 5s
		t.Fatal("NATS server not ready for connections")
	}

	nc, err := nats.Connect(fmt.Sprintf("nats://127.0.0.1:%d", opts.Port))
	if err != nil {
		s.Shutdown()
		t.Fatalf("failed to connect to NATS: %v", err)
	}

	t.Cleanup(func() {
		nc.Close()
		s.Shutdown()
	})

	return s, nc
}
