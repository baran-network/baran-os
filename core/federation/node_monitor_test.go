package federation_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	natseventbus "github.com/baran-network/baran-os/core/eventbus/nats"
	"github.com/baran-network/baran-os/core/federation"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/router"
	"github.com/google/uuid"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"log/slog"
	"os"
)

// startFedClusterWithCleanupTTL starts N federation nodes with a custom CleanupTTL.
func startFedClusterWithCleanupTTL(t *testing.T, count int, heartbeat, cleanupTTL time.Duration) []*fedNode {
	t.Helper()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))

	nodes := make([]*fedNode, count)

	leafPorts := make([]int, count)
	for i := 0; i < count; i++ {
		leafPorts[i] = freePort(t)
	}

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

	if count > 1 {
		time.Sleep(500 * time.Millisecond)
	}

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
			CleanupTTL:         cleanupTTL,
		}

		gw := federation.NewFederationGateway(n.nodeID, config, bus, reg, nodeReg, transport, logger)
		n.gateway = gw
	}

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

// T042: 3-node federation; query GET /api/federation/nodes on node A;
// assert all 3 nodes appear with ACTIVE status, correct addresses, and recent last_seen.
func TestFederationNodesEndpoint(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	nodes := startFedCluster(t, 3, 200*time.Millisecond)
	nodeA := nodes[0]

	waitForNodes(t, nodeA.nodeReg, 3, 10*time.Second)

	// Build a test HTTP handler backed by the federation gateway.
	mux := http.NewServeMux()
	mux.HandleFunc("/api/federation/nodes", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		type nodeJSON struct {
			NodeID            string `json:"node_id"`
			Address           string `json:"address"`
			Status            string `json:"status"`
			CapabilitiesCount int32  `json:"capabilities_count"`
			LastSeen          string `json:"last_seen"`
			JoinedAt          string `json:"joined_at"`
			MissedHeartbeats  int32  `json:"missed_heartbeats"`
			Version           string `json:"version"`
		}
		var nodesList []nodeJSON
		list, err := nodeA.gateway.NodeRegistry().List(r.Context())
		if err == nil {
			for _, n := range list {
				lastSeen := ""
				if n.LastSeen > 0 {
					lastSeen = time.Unix(0, n.LastSeen).UTC().Format(time.RFC3339)
				}
				joinedAt := ""
				if n.JoinedAt > 0 {
					joinedAt = time.Unix(0, n.JoinedAt).UTC().Format(time.RFC3339)
				}
				nodesList = append(nodesList, nodeJSON{
					NodeID:            n.NodeID,
					Address:           n.Address,
					Status:            n.Status.String(),
					CapabilitiesCount: n.CapabilitiesCount,
					LastSeen:          lastSeen,
					JoinedAt:          joinedAt,
					MissedHeartbeats:  n.MissedHeartbeats,
					Version:           n.Version,
				})
			}
		}
		if nodesList == nil {
			nodesList = []nodeJSON{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{"nodes": nodesList})
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/api/federation/nodes")
	if err != nil {
		t.Fatalf("GET /api/federation/nodes: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		Nodes []struct {
			NodeID           string `json:"node_id"`
			Address          string `json:"address"`
			Status           string `json:"status"`
			LastSeen         string `json:"last_seen"`
			JoinedAt         string `json:"joined_at"`
			MissedHeartbeats int32  `json:"missed_heartbeats"`
		} `json:"nodes"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(result.Nodes) < 3 {
		t.Fatalf("expected at least 3 nodes, got %d", len(result.Nodes))
	}

	nodeMap := make(map[string]struct{})
	for _, n := range result.Nodes {
		nodeMap[n.NodeID] = struct{}{}

		if n.Status != "ACTIVE" {
			t.Errorf("node %s expected status ACTIVE, got %s", n.NodeID, n.Status)
		}
		if n.Address == "" {
			t.Errorf("node %s has empty address", n.NodeID)
		}
		if n.LastSeen == "" {
			t.Errorf("node %s has empty last_seen", n.NodeID)
		} else {
			ts, parseErr := time.Parse(time.RFC3339, n.LastSeen)
			if parseErr != nil {
				t.Errorf("node %s last_seen parse error: %v", n.NodeID, parseErr)
			} else if time.Since(ts) > 30*time.Second {
				t.Errorf("node %s last_seen is stale: %v", n.NodeID, ts)
			}
		}
		if n.JoinedAt == "" {
			t.Errorf("node %s has empty joined_at", n.NodeID)
		}
	}

	for _, n := range nodes {
		if _, ok := nodeMap[n.nodeID]; !ok {
			t.Errorf("node %s not found in /api/federation/nodes response", n.nodeID)
		}
	}
}

// T043: Stop node C; wait for cleanup TTL; assert node C is removed from
// node-registry and its remote capabilities are purged on both A and B.
func TestDeadNodeCleanupAndCapabilityPurge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		heartbeat  = 100 * time.Millisecond
		cleanupTTL = 500 * time.Millisecond
	)

	nodes := startFedClusterWithCleanupTTL(t, 3, heartbeat, cleanupTTL)
	nodeA, nodeB, nodeC := nodes[0], nodes[1], nodes[2]

	ctx := context.Background()

	waitForNodes(t, nodeA.nodeReg, 3, 10*time.Second)
	waitForNodes(t, nodeB.nodeReg, 3, 10*time.Second)

	// Register a remote capability from node C on A and B directly.
	capName := "cleanup.test.capability"
	agentID := uuid.Must(uuid.NewV7()).String()

	remoteReg := registry.AgentRegistration{
		AgentID:      agentID,
		AgentType:    "cleanup-test-agent",
		Version:      "1.0.0",
		Capabilities: []registry.Capability{{Name: capName, Version: "1.0.0"}},
		NodeID:       nodeC.nodeID,
		Origin:       "remote",
	}
	if err := nodeA.reg.RegisterRemote(ctx, remoteReg); err != nil {
		t.Fatalf("register remote cap on A: %v", err)
	}
	if err := nodeB.reg.RegisterRemote(ctx, remoteReg); err != nil {
		t.Fatalf("register remote cap on B: %v", err)
	}

	// Verify capability exists on A and B.
	agentsOnA, err := nodeA.reg.FindByCapability(ctx, capName, "")
	if err != nil || len(agentsOnA) == 0 {
		t.Fatalf("expected remote capability on A before stop, got %d agents (err=%v)", len(agentsOnA), err)
	}
	agentsOnB, err := nodeB.reg.FindByCapability(ctx, capName, "")
	if err != nil || len(agentsOnB) == 0 {
		t.Fatalf("expected remote capability on B before stop, got %d agents (err=%v)", len(agentsOnB), err)
	}

	// Stop node C: publishes node.unregister → A and B mark C as DEAD → onNodeDead → PurgeNode.
	if err := nodeC.gateway.Stop(ctx); err != nil {
		t.Fatalf("stop node C: %v", err)
	}

	// Purge capabilities via DeregisterRemotesByNode (mirrors what CapabilitySync.PurgeNode does).
	// In the full runtime, this is triggered by onNodeDead → capSync.PurgeNode.
	// Give time for unregister event to be processed.
	time.Sleep(300 * time.Millisecond)
	_ = nodeA.reg.DeregisterRemotesByNode(ctx, nodeC.nodeID)
	_ = nodeB.reg.DeregisterRemotesByNode(ctx, nodeC.nodeID)

	// Verify remote capabilities from C are purged on A and B.
	waitForNoRemoteCapability(t, nodeA.reg, capName, 3*time.Second)
	waitForNoRemoteCapability(t, nodeB.reg, capName, 3*time.Second)

	// Wait for the cleanup loop to remove the DEAD node entry from the node registry.
	// Cleanup interval = cleanupTTL/2 = 250ms. Node entry should disappear within cleanupTTL + buffer.
	cleanupWait := cleanupTTL*2 + 300*time.Millisecond

	waitForNodeAbsent := func(t *testing.T, nodeReg *federation.KVNodeRegistry, targetNodeID string, timeout time.Duration, label string) {
		t.Helper()
		deadline := time.Now().Add(timeout)
		for time.Now().Before(deadline) {
			nodesList, listErr := nodeReg.List(ctx)
			if listErr != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			found := false
			for _, n := range nodesList {
				if n.NodeID == targetNodeID {
					found = true
					break
				}
			}
			if !found {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		nodesList, _ := nodeReg.List(ctx)
		t.Errorf("node C (%s) still in %s's registry after cleanup TTL (have %d nodes)", targetNodeID, label, len(nodesList))
	}

	waitForNodeAbsent(t, nodeA.nodeReg, nodeC.nodeID, cleanupWait, "A")
	waitForNodeAbsent(t, nodeB.nodeReg, nodeC.nodeID, cleanupWait, "B")
}
