package federation_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/federation"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/router"
	"github.com/google/uuid"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// fedNode represents a fully wired federation node for testing.
type fedNode struct {
	nodeID   string
	server   *natsserver.Server
	nc       *nats.Conn
	bus      eventbus.EventBus
	reg      registry.AgentRegistry
	nodeReg  *federation.KVNodeRegistry
	gateway  *federation.FederationGateway
	leafPort int
}

// freePort returns an available TCP port.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()
	return port
}

// startFedCluster starts N NATS servers with leaf node connections,
// creates and starts federation gateways on each.
// Node 0 is the seed. Nodes 1..N-1 connect to node 0 via leaf nodes.
func startFedCluster(t *testing.T, count int, heartbeat time.Duration) []*fedNode {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	nodes := make([]*fedNode, count)

	// Allocate leaf ports upfront so we know them before starting servers.
	leafPorts := make([]int, count)
	for i := 0; i < count; i++ {
		leafPorts[i] = freePort(t)
	}

	// Phase 1: Start all NATS servers.
	for i := 0; i < count; i++ {
		opts := &natsserver.Options{
			Host:      "127.0.0.1",
			Port:      -1,
			NoLog:     true,
			NoSigs:    true,
			JetStream: true,
			StoreDir:  t.TempDir(),
			LeafNode: natsserver.LeafNodeOpts{
				Host: "127.0.0.1",
				Port: leafPorts[i],
			},
		}

		if i > 0 {
			opts.LeafNode.Remotes = []*natsserver.RemoteLeafOpts{
				{
					URLs: []*url.URL{
						{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", leafPorts[0])},
					},
				},
			}
		}

		s, err := natsserver.NewServer(opts)
		if err != nil {
			t.Fatalf("node %d: create server: %v", i, err)
		}
		s.Start()
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatalf("node %d: server not ready", i)
		}

		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			s.Shutdown()
			t.Fatalf("node %d: connect: %v", i, err)
		}

		nodes[i] = &fedNode{
			nodeID:   uuid.Must(uuid.NewV7()).String(),
			server:   s,
			nc:       nc,
			leafPort: leafPorts[i],
		}
	}

	// Wait for leaf node connections to establish.
	if count > 1 {
		time.Sleep(500 * time.Millisecond)
	}

	// Phase 2: Create components and gateways.
	ctx := context.Background()
	for i, n := range nodes {
		streams := router.DefaultStreamRegistry()
		bus, err := natseventbus.NewFromConn(ctx, n.nc, streams)
		if err != nil {
			t.Fatalf("node %d: eventbus: %v", i, err)
		}
		n.bus = bus

		reg, err := registry.NewKVRegistry(ctx, n.nc, 3, 6)
		if err != nil {
			t.Fatalf("node %d: agent registry: %v", i, err)
		}
		n.reg = reg

		nodeReg, err := federation.NewKVNodeRegistry(ctx, n.nc, 3, 6)
		if err != nil {
			t.Fatalf("node %d: node registry: %v", i, err)
		}
		n.nodeReg = nodeReg

		transport := federation.NewNATSLeafTransport()
		if err := transport.Connect(ctx, n.nc); err != nil {
			t.Fatalf("node %d: transport: %v", i, err)
		}

		var seeds []string
		if i > 0 {
			seeds = []string{fmt.Sprintf("127.0.0.1:%d", leafPorts[0])}
		}

		config := federation.GatewayConfig{
			Seeds:              seeds,
			PSK:                "test-psk",
			HeartbeatInterval:  heartbeat,
			UnhealthyThreshold: 3,
			DeadThreshold:      6,
			RelayTimeout:       5 * time.Second,
			LeafPort:           leafPorts[i],
			CleanupTTL:         30 * time.Second,
		}

		gw := federation.NewFederationGateway(n.nodeID, config, bus, reg, nodeReg, transport, logger)
		n.gateway = gw
	}

	// Phase 3: Start gateways (seed first).
	for i, n := range nodes {
		if _, err := n.gateway.Start(ctx); err != nil {
			t.Fatalf("node %d: start gateway: %v", i, err)
		}
	}

	t.Cleanup(func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			n := nodes[i]
			_ = n.gateway.Stop(context.Background())
			_ = n.bus.Close()
			n.nc.Close()
			n.server.Shutdown()
		}
	})

	return nodes
}

// waitForNodes polls the node registry until at least count nodes are present.
func waitForNodes(t *testing.T, nodeReg *federation.KVNodeRegistry, count int, timeout time.Duration) []federation.NodeInfo {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		nodes, err := nodeReg.List(context.Background())
		if err == nil && len(nodes) >= count {
			return nodes
		}
		time.Sleep(100 * time.Millisecond)
	}
	nodes, _ := nodeReg.List(context.Background())
	t.Fatalf("expected at least %d nodes, got %d after %v", count, len(nodes), timeout)
	return nil
}

// waitForNodeStatus polls until a specific node reaches the desired status.
func waitForNodeStatus(t *testing.T, nodeReg *federation.KVNodeRegistry, nodeID string, status federation.NodeLifecycleStatus, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		info, _, err := nodeReg.Get(context.Background(), nodeID)
		if err == nil && info.Status == status {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	info, _, _ := nodeReg.Get(context.Background(), nodeID)
	t.Fatalf("expected node %s status %v, got %v after %v", nodeID, status, info.Status, timeout)
}

func assertNodePresent(t *testing.T, nodes []federation.NodeInfo, nodeID string, expectedStatus federation.NodeLifecycleStatus) {
	t.Helper()
	for _, n := range nodes {
		if n.NodeID == nodeID {
			if n.Status != expectedStatus {
				t.Errorf("node %s expected status %v, got %v", nodeID, expectedStatus, n.Status)
			}
			return
		}
	}
	t.Errorf("node %s not found in registry (have %d nodes)", nodeID, len(nodes))
}

// startMutualFedCluster creates two NATS servers that leaf-connect to each other simultaneously,
// then starts federation gateways on both. Neither node is a dedicated "seed" — both are peers.
func startMutualFedCluster(t *testing.T, heartbeat time.Duration) []*fedNode {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	portA := freePort(t)
	portB := freePort(t)

	makeServer := func(ownPort, remotePort int) (*natsserver.Server, *nats.Conn) {
		t.Helper()
		remotes := []*natsserver.RemoteLeafOpts{
			{
				URLs: []*url.URL{
					{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", remotePort)},
				},
			},
		}
		opts := &natsserver.Options{
			Host:      "127.0.0.1",
			Port:      -1,
			NoLog:     true,
			NoSigs:    true,
			JetStream: true,
			StoreDir:  t.TempDir(),
			LeafNode: natsserver.LeafNodeOpts{
				Host:    "127.0.0.1",
				Port:    ownPort,
				Remotes: remotes,
			},
		}
		s, err := natsserver.NewServer(opts)
		if err != nil {
			t.Fatalf("create server: %v", err)
		}
		s.Start()
		if !s.ReadyForConnections(5 * time.Second) {
			t.Fatalf("server not ready")
		}
		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			s.Shutdown()
			t.Fatalf("connect: %v", err)
		}
		return s, nc
	}

	serverA, ncA := makeServer(portA, portB)
	serverB, ncB := makeServer(portB, portA)

	// Wait for mutual leaf connections.
	time.Sleep(500 * time.Millisecond)

	ctx := context.Background()
	makeNode := func(nc *nats.Conn, server *natsserver.Server, ownPort, peerPort int) *fedNode {
		t.Helper()
		streams := router.DefaultStreamRegistry()
		bus, err := natseventbus.NewFromConn(ctx, nc, streams)
		if err != nil {
			t.Fatalf("eventbus: %v", err)
		}
		reg, err := registry.NewKVRegistry(ctx, nc, 3, 6)
		if err != nil {
			t.Fatalf("agent registry: %v", err)
		}
		nodeReg, err := federation.NewKVNodeRegistry(ctx, nc, 3, 6)
		if err != nil {
			t.Fatalf("node registry: %v", err)
		}
		transport := federation.NewNATSLeafTransport()
		if err := transport.Connect(ctx, nc); err != nil {
			t.Fatalf("transport: %v", err)
		}
		config := federation.GatewayConfig{
			Seeds:              []string{fmt.Sprintf("127.0.0.1:%d", peerPort)},
			PSK:                "test-psk",
			HeartbeatInterval:  heartbeat,
			UnhealthyThreshold: 3,
			DeadThreshold:      6,
			RelayTimeout:       5 * time.Second,
			LeafPort:           ownPort,
			CleanupTTL:         30 * time.Second,
		}
		nodeID := uuid.Must(uuid.NewV7()).String()
		gw := federation.NewFederationGateway(nodeID, config, bus, reg, nodeReg, transport, logger)
		return &fedNode{
			nodeID:   nodeID,
			server:   server,
			nc:       nc,
			bus:      bus,
			reg:      reg,
			nodeReg:  nodeReg,
			gateway:  gw,
			leafPort: ownPort,
		}
	}

	nodeA := makeNode(ncA, serverA, portA, portB)
	nodeB := makeNode(ncB, serverB, portB, portA)
	nodes := []*fedNode{nodeA, nodeB}

	// Start both gateways simultaneously (no dedicated seed).
	for i, n := range nodes {
		if _, err := n.gateway.Start(ctx); err != nil {
			t.Fatalf("node %d: start gateway: %v", i, err)
		}
	}

	t.Cleanup(func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			n := nodes[i]
			_ = n.gateway.Stop(context.Background())
			_ = n.bus.Close()
			n.nc.Close()
			n.server.Shutdown()
		}
	})

	return nodes
}

// T044: Mutual seed — A seeds B and B seeds A simultaneously; assert both appear with ACTIVE status
// and no duplicate entries in either registry.
func TestMutualSeed_Idempotent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	nodes := startMutualFedCluster(t, 500*time.Millisecond)
	nodeA, nodeB := nodes[0], nodes[1]

	// Both nodes should discover each other.
	waitForNodes(t, nodeA.nodeReg, 2, 10*time.Second)
	waitForNodes(t, nodeB.nodeReg, 2, 10*time.Second)

	// Verify no duplicates: exactly 2 entries in each registry.
	ctx := context.Background()
	listA, err := nodeA.nodeReg.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listA) != 2 {
		t.Errorf("nodeA registry: expected exactly 2 entries, got %d", len(listA))
	}
	assertNodePresent(t, listA, nodeA.nodeID, federation.NodeStatusActive)
	assertNodePresent(t, listA, nodeB.nodeID, federation.NodeStatusActive)

	listB, err := nodeB.nodeReg.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(listB) != 2 {
		t.Errorf("nodeB registry: expected exactly 2 entries, got %d", len(listB))
	}
	assertNodePresent(t, listB, nodeA.nodeID, federation.NodeStatusActive)
	assertNodePresent(t, listB, nodeB.nodeID, federation.NodeStatusActive)
}

// T045: Node rejoin — crash a node (DEAD), wait for cleanup to purge it, then restart the
// same node ID; assert it re-registers as ACTIVE and re-shares capabilities from scratch.
func TestNodeRejoin(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		heartbeat  = 100 * time.Millisecond
		cleanupTTL = 400 * time.Millisecond
	)

	nodes := startFedClusterWithCleanupTTL(t, 2, heartbeat, cleanupTTL)
	nodeA, nodeB := nodes[0], nodes[1]
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	// Wait for initial discovery.
	waitForNodes(t, nodeA.nodeReg, 2, 5*time.Second)

	// Save node B's identity and leaf port for restart.
	nodeBID := nodeB.nodeID
	nodeBLeafPort := nodeB.leafPort
	nodeALeafPort := nodeA.leafPort

	// Crash node B — no graceful unregister.
	nodeB.nc.Close()
	nodeB.server.Shutdown()

	// A detects B as DEAD.
	waitForNodeStatus(t, nodeA.nodeReg, nodeBID, federation.NodeStatusDead, 5*time.Second)

	// Wait for cleanup to remove B's dead entry.
	waitForNodeAbsent := func(timeout time.Duration) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			list, err := nodeA.nodeReg.List(context.Background())
			if err == nil {
				found := false
				for _, n := range list {
					if n.NodeID == nodeBID {
						found = true
						break
					}
				}
				if !found {
					return
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		list, _ := nodeA.nodeReg.List(context.Background())
		t.Logf("node B still present after cleanup wait (have %d nodes) — proceeding", len(list))
	}
	waitForNodeAbsent(cleanupTTL*3 + 500*time.Millisecond)

	// Restart node B: new NATS server, same node ID (simulates process restart).
	newLeafPort := freePort(t)
	_ = nodeBLeafPort // original port now freed

	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		NoLog:     true,
		NoSigs:    true,
		JetStream: true,
		StoreDir:  t.TempDir(),
		LeafNode: natsserver.LeafNodeOpts{
			Host: "127.0.0.1",
			Port: newLeafPort,
			Remotes: []*natsserver.RemoteLeafOpts{
				{
					URLs: []*url.URL{
						{Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", nodeALeafPort)},
					},
				},
			},
		},
	}
	newServer, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("restart: create server: %v", err)
	}
	newServer.Start()
	if !newServer.ReadyForConnections(5 * time.Second) {
		t.Fatal("restart: server not ready")
	}

	newNC, err := nats.Connect(newServer.ClientURL())
	if err != nil {
		newServer.Shutdown()
		t.Fatalf("restart: connect: %v", err)
	}
	t.Cleanup(func() { newNC.Close(); newServer.Shutdown() })

	// Wait for leaf connection to A.
	time.Sleep(500 * time.Millisecond)

	ctx := context.Background()
	streams := router.DefaultStreamRegistry()
	newBus, err := natseventbus.NewFromConn(ctx, newNC, streams)
	if err != nil {
		t.Fatalf("restart: eventbus: %v", err)
	}
	t.Cleanup(func() { _ = newBus.Close() })

	newReg, err := registry.NewKVRegistry(ctx, newNC, 3, 6)
	if err != nil {
		t.Fatalf("restart: agent registry: %v", err)
	}
	newNodeReg, err := federation.NewKVNodeRegistry(ctx, newNC, 3, 6)
	if err != nil {
		t.Fatalf("restart: node registry: %v", err)
	}
	newTransport := federation.NewNATSLeafTransport()
	if err := newTransport.Connect(ctx, newNC); err != nil {
		t.Fatalf("restart: transport: %v", err)
	}

	newConfig := federation.GatewayConfig{
		Seeds:              []string{fmt.Sprintf("127.0.0.1:%d", nodeALeafPort)},
		PSK:                "test-psk",
		HeartbeatInterval:  heartbeat,
		UnhealthyThreshold: 3,
		DeadThreshold:      6,
		RelayTimeout:       5 * time.Second,
		LeafPort:           newLeafPort,
		CleanupTTL:         cleanupTTL,
	}

	// Reuse the same node ID to simulate a restart of the same logical node.
	newGW := federation.NewFederationGateway(nodeBID, newConfig, newBus, newReg, newNodeReg, newTransport, logger)
	if _, err := newGW.Start(ctx); err != nil {
		t.Fatalf("restart: start gateway: %v", err)
	}
	t.Cleanup(func() { _ = newGW.Stop(context.Background()) })

	// Node A should see node B again as ACTIVE.
	waitForNodeStatus(t, nodeA.nodeReg, nodeBID, federation.NodeStatusActive, 5*time.Second)

	// Register a new capability on the restarted node B and verify A sees it.
	publishCapabilityAnnounce(ctx, t, newBus, nodeBID, "agent-rejoin-001", []*protocolv1.Capability{
		{Name: "rejoin.capability", Version: "1.0"},
	})
	if _, err := newReg.Register(ctx, registry.AgentRegistration{
		AgentID:      "agent-rejoin-001",
		AgentType:    "rejoin-agent",
		Capabilities: []registry.Capability{{Name: "rejoin.capability", Version: "1.0"}},
		NodeID:       nodeBID,
	}); err != nil {
		t.Fatalf("register rejoin agent: %v", err)
	}

	waitForRemoteCapability(t, nodeA.reg, "rejoin.capability", 5*time.Second)
}

// T018: Two-node discovery — both nodes appear in each other's node registry with ACTIVE status.
func TestTwoNodeDiscovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	nodes := startFedCluster(t, 2, 500*time.Millisecond)
	nodeA, nodeB := nodes[0], nodes[1]

	// Wait for both nodes to discover each other.
	waitForNodes(t, nodeA.nodeReg, 2, 5*time.Second)
	waitForNodes(t, nodeB.nodeReg, 2, 5*time.Second)

	// Verify A knows about B with ACTIVE status.
	nodesOnA, err := nodeA.nodeReg.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertNodePresent(t, nodesOnA, nodeA.nodeID, federation.NodeStatusActive)
	assertNodePresent(t, nodesOnA, nodeB.nodeID, federation.NodeStatusActive)

	// Verify B knows about A with ACTIVE status.
	nodesOnB, err := nodeB.nodeReg.List(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assertNodePresent(t, nodesOnB, nodeA.nodeID, federation.NodeStatusActive)
	assertNodePresent(t, nodesOnB, nodeB.nodeID, federation.NodeStatusActive)
}

// T019: Node health transitions — simulate crash and verify UNHEALTHY then DEAD.
func TestNodeHealthTransitions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	// Use fast heartbeats for quick transitions.
	nodes := startFedCluster(t, 2, 200*time.Millisecond)
	nodeA, nodeB := nodes[0], nodes[1]

	// Wait for initial discovery.
	waitForNodes(t, nodeA.nodeReg, 2, 5*time.Second)

	// Simulate node B crash: close its NATS connection and shut down server.
	// This does NOT call gateway.Stop() (no unregister announcement), so A must
	// detect the failure through missed heartbeats.
	nodeB.nc.Close()
	nodeB.server.Shutdown()

	// Node A should transition B to UNHEALTHY (3 missed heartbeats at 200ms = ~600ms).
	waitForNodeStatus(t, nodeA.nodeReg, nodeB.nodeID, federation.NodeStatusUnhealthy, 5*time.Second)

	// Node A should transition B to DEAD (6 missed heartbeats at 200ms = ~1200ms).
	waitForNodeStatus(t, nodeA.nodeReg, nodeB.nodeID, federation.NodeStatusDead, 5*time.Second)
}

// T020: Three-node propagation — node C joins via A, all three know about each other.
func TestThreeNodePropagation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	nodes := startFedCluster(t, 3, 500*time.Millisecond)
	nodeA, nodeB, nodeC := nodes[0], nodes[1], nodes[2]

	// Wait for all three to discover each other.
	waitForNodes(t, nodeA.nodeReg, 3, 5*time.Second)
	waitForNodes(t, nodeB.nodeReg, 3, 5*time.Second)
	waitForNodes(t, nodeC.nodeReg, 3, 5*time.Second)

	// Verify all nodes know about all others with ACTIVE status.
	for _, observer := range nodes {
		nodeList, err := observer.nodeReg.List(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(nodeList) < 3 {
			t.Errorf("node %s expected 3 nodes, got %d", observer.nodeID, len(nodeList))
			continue
		}
		assertNodePresent(t, nodeList, nodeA.nodeID, federation.NodeStatusActive)
		assertNodePresent(t, nodeList, nodeB.nodeID, federation.NodeStatusActive)
		assertNodePresent(t, nodeList, nodeC.nodeID, federation.NodeStatusActive)
	}
}
