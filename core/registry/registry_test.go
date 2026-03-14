package registry_test

import (
	"context"
	"testing"
	"time"

	"github.com/carlosmolina/agent-os/core/registry"
	"github.com/carlosmolina/agent-os/core/testutil"
)

func newTestRegistry(t *testing.T) *registry.KVRegistry {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()
	reg, err := registry.NewKVRegistry(ctx, nc, 3, 6)
	if err != nil {
		t.Fatalf("create registry: %v", err)
	}
	return reg
}

func TestRegisterNewAgent(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	rev, err := reg.Register(ctx, registry.AgentRegistration{
		AgentID:   "agent-001",
		AgentType: "test",
		Version:   "1.0.0",
		Capabilities: []registry.Capability{
			{Name: "detect", Version: "1.0.0"},
		},
		NodeID: "node-1",
	})
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if rev == 0 {
		t.Error("expected non-zero revision")
	}

	got, gotRev, err := reg.Get(ctx, "agent-001")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.AgentID != "agent-001" {
		t.Errorf("got AgentID %q, want %q", got.AgentID, "agent-001")
	}
	if got.Status != registry.StatusActive {
		t.Errorf("got status %v, want ACTIVE", got.Status)
	}
	if gotRev != rev {
		t.Errorf("got revision %d, want %d", gotRev, rev)
	}
}

func TestReRegisterResetsStatus(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	agent := registry.AgentRegistration{
		AgentID:      "agent-re",
		AgentType:    "test",
		Version:      "1.0.0",
		Capabilities: []registry.Capability{{Name: "cap", Version: "1.0.0"}},
		NodeID:       "node-1",
	}

	rev, err := reg.Register(ctx, agent)
	if err != nil {
		t.Fatalf("register: %v", err)
	}

	// Mark as UNHEALTHY.
	_, err = reg.UpdateStatus(ctx, "agent-re", registry.StatusUnhealthy, rev)
	if err != nil {
		t.Fatalf("update status: %v", err)
	}

	// Re-register should reset to ACTIVE.
	_, err = reg.Register(ctx, agent)
	if err != nil {
		t.Fatalf("re-register: %v", err)
	}

	got, _, _ := reg.Get(ctx, "agent-re")
	if got.Status != registry.StatusActive {
		t.Errorf("got status %v after re-register, want ACTIVE", got.Status)
	}
	if got.MissedHeartbeats != 0 {
		t.Errorf("got missed %d, want 0", got.MissedHeartbeats)
	}
}

func TestRegisterValidationFails(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	tests := []struct {
		name  string
		agent registry.AgentRegistration
	}{
		{"missing agent_id", registry.AgentRegistration{AgentType: "t", Capabilities: []registry.Capability{{Name: "c", Version: "1"}}}},
		{"missing agent_type", registry.AgentRegistration{AgentID: "a", Capabilities: []registry.Capability{{Name: "c", Version: "1"}}}},
		{"no capabilities", registry.AgentRegistration{AgentID: "a", AgentType: "t"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := reg.Register(ctx, tt.agent)
			if err == nil {
				t.Error("expected validation error")
			}
		})
	}
}

func TestDeregister(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	if _, err := reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-del", AgentType: "t", Version: "1",
		Capabilities: []registry.Capability{{Name: "c", Version: "1"}},
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := reg.Deregister(ctx, "agent-del"); err != nil {
		t.Fatalf("deregister: %v", err)
	}

	_, _, err := reg.Get(ctx, "agent-del")
	if err != registry.ErrNotFound {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestDeregisterNonexistent(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	if err := reg.Deregister(ctx, "no-such-agent"); err != nil {
		t.Errorf("expected no error for nonexistent agent, got %v", err)
	}
}

func TestListAgents(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	for i := range 3 {
		id := "agent-list-" + string(rune('A'+i))
		if _, err := reg.Register(ctx, registry.AgentRegistration{
			AgentID: id, AgentType: "t", Version: "1",
			Capabilities: []registry.Capability{{Name: "c", Version: "1"}},
		}); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
	}

	agents, err := reg.List(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(agents) != 3 {
		t.Errorf("got %d agents, want 3", len(agents))
	}
}

func TestRecordHeartbeat(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	rev, _ := reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-hb", AgentType: "t", Version: "1",
		Capabilities: []registry.Capability{{Name: "c", Version: "1"}},
	})

	// Set UNHEALTHY first.
	rev, _ = reg.UpdateStatus(ctx, "agent-hb", registry.StatusUnhealthy, rev)

	before := time.Now()
	rev, err := reg.RecordHeartbeat(ctx, "agent-hb", rev)
	if err != nil {
		t.Fatalf("record heartbeat: %v", err)
	}

	got, _, _ := reg.Get(ctx, "agent-hb")
	if got.Status != registry.StatusActive {
		t.Errorf("got status %v, want ACTIVE after heartbeat", got.Status)
	}
	if got.MissedHeartbeats != 0 {
		t.Errorf("got missed %d, want 0", got.MissedHeartbeats)
	}
	if got.LastSeen.Before(before) {
		t.Error("LastSeen was not updated")
	}
	_ = rev
}

func TestFindByCapabilityNameMatch(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	// Register agents with different capabilities.
	reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-A", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "risk-estimation", Version: "1.0.0"}},
	})
	reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-B", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "risk-estimation", Version: "1.2.0"}},
	})
	reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-C", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "evacuation", Version: "1.0.0"}},
	})

	matches, err := reg.FindByCapability(ctx, "risk-estimation", "")
	if err != nil {
		t.Fatalf("FindByCapability: %v", err)
	}
	if len(matches) != 2 {
		t.Fatalf("got %d matches, want 2", len(matches))
	}
	if matches[0].AgentID != "agent-A" || matches[1].AgentID != "agent-B" {
		t.Errorf("got agents %v, want [agent-A, agent-B]", []string{matches[0].AgentID, matches[1].AgentID})
	}
}

func TestFindByCapabilityVersionConstraint(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-v1", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "risk-estimation", Version: "1.0.0"}},
	})
	reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-v2", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "risk-estimation", Version: "2.0.0"}},
	})

	matches, err := reg.FindByCapability(ctx, "risk-estimation", "1.x")
	if err != nil {
		t.Fatalf("FindByCapability: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("got %d matches, want 1", len(matches))
	}
	if matches[0].AgentID != "agent-v1" {
		t.Errorf("got agent %s, want agent-v1", matches[0].AgentID)
	}
}

func TestFindByCapabilityActiveOnly(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	rev, _ := reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-unhealthy", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "detect", Version: "1.0.0"}},
	})
	reg.UpdateStatus(ctx, "agent-unhealthy", registry.StatusUnhealthy, rev)

	reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-active", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "detect", Version: "1.0.0"}},
	})

	matches, err := reg.FindByCapability(ctx, "detect", "")
	if err != nil {
		t.Fatalf("FindByCapability: %v", err)
	}
	if len(matches) != 1 {
		t.Fatalf("got %d matches, want 1", len(matches))
	}
	if matches[0].AgentID != "agent-active" {
		t.Errorf("got agent %s, want agent-active", matches[0].AgentID)
	}
}

func TestFindByCapabilityEmptyResult(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-x", AgentType: "t", Version: "1.0.0",
		Capabilities: []registry.Capability{{Name: "unrelated", Version: "1.0.0"}},
	})

	matches, err := reg.FindByCapability(ctx, "nonexistent", "")
	if err != nil {
		t.Fatalf("FindByCapability: %v", err)
	}
	if len(matches) != 0 {
		t.Errorf("got %d matches, want 0", len(matches))
	}
}

func TestIncrementMissedHeartbeats(t *testing.T) {
	reg := newTestRegistry(t)
	ctx := context.Background()

	rev, _ := reg.Register(ctx, registry.AgentRegistration{
		AgentID: "agent-miss", AgentType: "t", Version: "1",
		Capabilities: []registry.Capability{{Name: "c", Version: "1"}},
	})

	// Increment to threshold of 3 → UNHEALTHY.
	var status registry.AgentLifecycleStatus
	for range 3 {
		var err error
		status, rev, err = reg.IncrementMissedHeartbeats(ctx, "agent-miss", rev)
		if err != nil {
			t.Fatalf("increment: %v", err)
		}
	}
	if status != registry.StatusUnhealthy {
		t.Errorf("after 3 missed: got %v, want UNHEALTHY", status)
	}

	// Increment 3 more → DEAD at threshold 6.
	for range 3 {
		var err error
		status, rev, err = reg.IncrementMissedHeartbeats(ctx, "agent-miss", rev)
		if err != nil {
			t.Fatalf("increment: %v", err)
		}
	}
	if status != registry.StatusDead {
		t.Errorf("after 6 missed: got %v, want DEAD", status)
	}
}
