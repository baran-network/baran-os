package federation_test

import (
	"context"
	"testing"
	"time"

	"github.com/baran-network/baran-os/core/eventbus"
	"github.com/baran-network/baran-os/core/federation"
	"github.com/baran-network/baran-os/core/registry"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"

	protocolv1 "github.com/baran-network/baran-os/protocol/gen/go/agentosprotocol/v1"
)

// publishCapabilityAnnounce publishes an agent.capability.announce event on the given bus.
func publishCapabilityAnnounce(ctx context.Context, t *testing.T, bus eventbus.EventBus, nodeID, agentID string, caps []*protocolv1.Capability) {
	t.Helper()
	payload := &protocolv1.CapabilityAnnouncePayload{
		AgentId:      agentID,
		Capabilities: caps,
	}
	data, err := proto.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal capability announce: %v", err)
	}
	if err := bus.Publish(ctx, &eventbus.Event{
		ID:          uuid.Must(uuid.NewV7()).String(),
		Type:        "agent.capability.announce",
		SourceNode:  nodeID,
		SourceAgent: "runtime",
		Timestamp:   time.Now().UnixNano(),
		Payload:     data,
	}); err != nil {
		t.Fatalf("publish capability announce: %v", err)
	}
}

// waitForRemoteCapability polls until FindByCapability returns at least one remote result.
func waitForRemoteCapability(t *testing.T, reg registry.AgentRegistry, capName string, timeout time.Duration) []registry.AgentRegistration {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		agents, err := reg.FindByCapability(context.Background(), capName, "")
		if err == nil {
			for _, a := range agents {
				if a.IsRemote() {
					return agents
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	agents, _ := reg.FindByCapability(context.Background(), capName, "")
	t.Fatalf("no remote agent with capability %q found after %v (have %d total)", capName, timeout, len(agents))
	return nil
}

// waitForNoRemoteCapability polls until FindByCapability returns no remote results.
func waitForNoRemoteCapability(t *testing.T, reg registry.AgentRegistry, capName string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		agents, err := reg.FindByCapability(context.Background(), capName, "")
		if err != nil {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		hasRemote := false
		for _, a := range agents {
			if a.IsRemote() {
				hasRemote = true
				break
			}
		}
		if !hasRemote {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("remote agent with capability %q still present after %v", capName, timeout)
}

// T027: Remote agent with fire.detection capability on node A appears on node B with IsRemote()==true.
func TestCapabilitySync_RemoteAgentVisible(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	nodes := startFedCluster(t, 2, 500*time.Millisecond)
	nodeA, nodeB := nodes[0], nodes[1]

	// Wait for discovery to stabilize.
	waitForNodes(t, nodeA.nodeReg, 2, 5*time.Second)

	// Register agent on node A's local registry.
	agentID := "agent-fire-001"
	_, err := nodeA.reg.Register(ctx, registry.AgentRegistration{
		AgentID:   agentID,
		AgentType: "fire-detector",
		Capabilities: []registry.Capability{
			{Name: "fire.detection", Version: "1.0"},
		},
		NodeID: nodeA.nodeID,
	})
	if err != nil {
		t.Fatalf("register agent on node A: %v", err)
	}

	// Publish agent.capability.announce on node A's bus to trigger CapabilitySync.
	publishCapabilityAnnounce(ctx, t, nodeA.bus, nodeA.nodeID, agentID, []*protocolv1.Capability{
		{Name: "fire.detection", Version: "1.0"},
	})

	// Wait for node B to have the remote agent.
	agents := waitForRemoteCapability(t, nodeB.reg, "fire.detection", 5*time.Second)

	// Verify the remote agent has the correct node ID and origin.
	found := false
	for _, a := range agents {
		if a.AgentID == agentID {
			found = true
			if !a.IsRemote() {
				t.Errorf("expected agent to be remote, got Origin=%q", a.Origin)
			}
			if a.NodeID != nodeA.nodeID {
				t.Errorf("expected NodeID=%q, got %q", nodeA.nodeID, a.NodeID)
			}
		}
	}
	if !found {
		t.Errorf("agent %q not found in node B results", agentID)
	}
}

// T028: Remote agent unregisters on node A; node B removes the remote capability within 3s.
func TestCapabilitySync_RemoteAgentUnregisters(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	nodes := startFedCluster(t, 2, 500*time.Millisecond)
	nodeA, nodeB := nodes[0], nodes[1]

	// Wait for discovery.
	waitForNodes(t, nodeA.nodeReg, 2, 5*time.Second)

	// Register and announce agent on node A.
	agentID := "agent-evac-001"
	_, err := nodeA.reg.Register(ctx, registry.AgentRegistration{
		AgentID:   agentID,
		AgentType: "evacuation-planner",
		Capabilities: []registry.Capability{
			{Name: "evacuation.plan", Version: "1.0"},
		},
		NodeID: nodeA.nodeID,
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	publishCapabilityAnnounce(ctx, t, nodeA.bus, nodeA.nodeID, agentID, []*protocolv1.Capability{
		{Name: "evacuation.plan", Version: "1.0"},
	})

	// Wait for node B to see it.
	waitForRemoteCapability(t, nodeB.reg, "evacuation.plan", 5*time.Second)

	// Now unregister: publish empty capability announce (signal from announcer).
	publishCapabilityAnnounce(ctx, t, nodeA.bus, nodeA.nodeID, agentID, nil)

	// Wait for node B to remove the remote capability.
	waitForNoRemoteCapability(t, nodeB.reg, "evacuation.plan", 5*time.Second)
}

// T029: Node A dies (DEAD); node B purges all remote capabilities from node A.
func TestCapabilitySync_NodeDeadPurgesCapabilities(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	ctx := context.Background()
	// Use fast heartbeats so DEAD detection happens quickly.
	nodes := startFedCluster(t, 2, 200*time.Millisecond)
	nodeA, nodeB := nodes[0], nodes[1]

	// Wait for discovery.
	waitForNodes(t, nodeA.nodeReg, 2, 5*time.Second)

	// Register multiple agents on node A and propagate to node B.
	agentIDs := []string{"agent-sensor-001", "agent-sensor-002"}
	for _, agentID := range agentIDs {
		_, err := nodeA.reg.Register(ctx, registry.AgentRegistration{
			AgentID:   agentID,
			AgentType: "sensor",
			Capabilities: []registry.Capability{
				{Name: "smoke.detection", Version: "1.0"},
			},
			NodeID: nodeA.nodeID,
		})
		if err != nil {
			t.Fatalf("register agent %s: %v", agentID, err)
		}
		publishCapabilityAnnounce(ctx, t, nodeA.bus, nodeA.nodeID, agentID, []*protocolv1.Capability{
			{Name: "smoke.detection", Version: "1.0"},
		})
	}

	// Wait for node B to see the remote agents.
	waitForRemoteCapability(t, nodeB.reg, "smoke.detection", 5*time.Second)

	// Crash node A (no graceful unregister).
	nodeA.nc.Close()
	nodeA.server.Shutdown()

	// Wait for node B to detect node A as DEAD.
	// DEAD = 6 missed heartbeats at 200ms = ~1.2s, with buffer.
	waitForNodeStatus(t, nodeB.nodeReg, nodeA.nodeID, federation.NodeStatusDead, 10*time.Second)

	// Verify node B has purged all remote capabilities from node A.
	waitForNoRemoteCapability(t, nodeB.reg, "smoke.detection", 5*time.Second)
}
