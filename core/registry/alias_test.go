package registry_test

import (
	"context"
	"testing"

	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/testutil"
)

func newAliasRegistry(t *testing.T, nodeID string) registry.AliasRegistry {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ar, err := registry.NewKVAliasRegistry(context.Background(), nc, nodeID, nil)
	if err != nil {
		t.Fatalf("NewKVAliasRegistry: %v", err)
	}
	return ar
}

func TestAliasRegistry_AddAndResolve(t *testing.T) {
	ctx := context.Background()
	ar := newAliasRegistry(t, "node-1")

	// Create alias: acme.risk_eval ↔ betacorp.risk_assessment
	mapping, err := ar.AddAlias(ctx, "acme.risk_eval", "betacorp.risk_assessment")
	if err != nil {
		t.Fatalf("AddAlias: %v", err)
	}
	if mapping.ID == "" {
		t.Fatal("expected non-empty alias ID")
	}
	if mapping.Source != "acme.risk_eval" {
		t.Errorf("unexpected source: %q", mapping.Source)
	}
	if mapping.Target != "betacorp.risk_assessment" {
		t.Errorf("unexpected target: %q", mapping.Target)
	}

	// Resolve from source: must include both names.
	resolved, err := ar.Resolve(ctx, "acme.risk_eval")
	if err != nil {
		t.Fatalf("Resolve from source: %v", err)
	}
	assertContains(t, resolved, "acme.risk_eval", "betacorp.risk_assessment")

	// Resolve from target: bidirectional — must also find source.
	resolved, err = ar.Resolve(ctx, "betacorp.risk_assessment")
	if err != nil {
		t.Fatalf("Resolve from target: %v", err)
	}
	assertContains(t, resolved, "acme.risk_eval", "betacorp.risk_assessment")
}

func TestAliasRegistry_ChainResolution(t *testing.T) {
	ctx := context.Background()
	ar := newAliasRegistry(t, "node-1")

	// A ↔ B, B ↔ C → resolving A should yield [A, B, C]
	if _, err := ar.AddAlias(ctx, "a.cap", "b.cap"); err != nil {
		t.Fatalf("AddAlias A-B: %v", err)
	}
	if _, err := ar.AddAlias(ctx, "b.cap", "c.cap"); err != nil {
		t.Fatalf("AddAlias B-C: %v", err)
	}

	resolved, err := ar.Resolve(ctx, "a.cap")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	assertContains(t, resolved, "a.cap", "b.cap", "c.cap")
}

func TestAliasRegistry_CircularDetection(t *testing.T) {
	ctx := context.Background()
	ar := newAliasRegistry(t, "node-1")

	// A ↔ B, then attempt B → A (which closes the cycle).
	if _, err := ar.AddAlias(ctx, "x.cap", "y.cap"); err != nil {
		t.Fatalf("AddAlias: %v", err)
	}

	// Adding y.cap → x.cap should fail because x.cap is already reachable from y.cap.
	_, err := ar.AddAlias(ctx, "y.cap", "x.cap")
	if err == nil {
		t.Fatal("expected cycle detection error, got nil")
	}
}

func TestAliasRegistry_SelfAlias(t *testing.T) {
	ctx := context.Background()
	ar := newAliasRegistry(t, "node-1")

	_, err := ar.AddAlias(ctx, "nlp.summarization", "nlp.summarization")
	if err == nil {
		t.Fatal("expected error for self-alias, got nil")
	}
}

func TestAliasRegistry_RemoveAlias(t *testing.T) {
	ctx := context.Background()
	ar := newAliasRegistry(t, "node-1")

	mapping, err := ar.AddAlias(ctx, "a.cap", "b.cap")
	if err != nil {
		t.Fatalf("AddAlias: %v", err)
	}

	// Before removal, b.cap is reachable from a.cap.
	resolved, _ := ar.Resolve(ctx, "a.cap")
	assertContains(t, resolved, "b.cap")

	// Remove alias.
	if err := ar.RemoveAlias(ctx, mapping.ID); err != nil {
		t.Fatalf("RemoveAlias: %v", err)
	}

	// After removal, only a.cap is returned.
	resolved, _ = ar.Resolve(ctx, "a.cap")
	if len(resolved) != 1 || resolved[0] != "a.cap" {
		t.Errorf("expected [a.cap] after removal, got %v", resolved)
	}
}

func TestAliasRegistry_RemoveAlias_Idempotent(t *testing.T) {
	ctx := context.Background()
	ar := newAliasRegistry(t, "node-1")

	// Removing a nonexistent alias should not error.
	if err := ar.RemoveAlias(ctx, "nonexistent-id"); err != nil {
		t.Fatalf("RemoveAlias on nonexistent ID: %v", err)
	}
}

func TestAliasRegistry_ListAliases(t *testing.T) {
	ctx := context.Background()
	ar := newAliasRegistry(t, "node-1")

	if _, err := ar.AddAlias(ctx, "a.cap", "b.cap"); err != nil {
		t.Fatalf("AddAlias: %v", err)
	}
	if _, err := ar.AddAlias(ctx, "c.cap", "d.cap"); err != nil {
		t.Fatalf("AddAlias: %v", err)
	}

	all, err := ar.ListAliases(ctx, "")
	if err != nil {
		t.Fatalf("ListAliases: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("expected 2 aliases, got %d", len(all))
	}

	// Filter by node.
	filtered, err := ar.ListAliases(ctx, "node-1")
	if err != nil {
		t.Fatalf("ListAliases (filtered): %v", err)
	}
	if len(filtered) != 2 {
		t.Errorf("expected 2 aliases for node-1, got %d", len(filtered))
	}

	// Filter by unknown node → empty.
	none, err := ar.ListAliases(ctx, "node-99")
	if err != nil {
		t.Fatalf("ListAliases (unknown node): %v", err)
	}
	if len(none) != 0 {
		t.Errorf("expected 0 aliases for unknown node, got %d", len(none))
	}
}

func TestAliasRegistry_Snapshot_ApplySnapshot(t *testing.T) {
	ctx := context.Background()

	_, nc1 := testutil.StartNATS(t)
	_, nc2 := testutil.StartNATS(t)

	ar1, err := registry.NewKVAliasRegistry(ctx, nc1, "node-1", nil)
	if err != nil {
		t.Fatalf("NewKVAliasRegistry ar1: %v", err)
	}
	ar2, err := registry.NewKVAliasRegistry(ctx, nc2, "node-2", nil)
	if err != nil {
		t.Fatalf("NewKVAliasRegistry ar2: %v", err)
	}

	// Add alias in ar1.
	if _, err := ar1.AddAlias(ctx, "acme.foo", "betacorp.bar"); err != nil {
		t.Fatalf("AddAlias: %v", err)
	}

	// Snapshot ar1 and apply to ar2.
	snapshot, err := ar1.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}
	if err := ar2.ApplySnapshot(ctx, snapshot); err != nil {
		t.Fatalf("ApplySnapshot: %v", err)
	}

	// ar2 should now resolve the alias.
	resolved, err := ar2.Resolve(ctx, "acme.foo")
	if err != nil {
		t.Fatalf("Resolve on ar2: %v", err)
	}
	assertContains(t, resolved, "acme.foo", "betacorp.bar")
}

// assertContains checks that all want strings are present in got.
func assertContains(t *testing.T, got []string, want ...string) {
	t.Helper()
	set := make(map[string]struct{}, len(got))
	for _, s := range got {
		set[s] = struct{}{}
	}
	for _, w := range want {
		if _, ok := set[w]; !ok {
			t.Errorf("expected %q in %v", w, got)
		}
	}
}
