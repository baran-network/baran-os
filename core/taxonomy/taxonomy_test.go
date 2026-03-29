package taxonomy_test

import (
	"context"
	"testing"

	"github.com/baran-network/baran-os/core/registry"
	"github.com/baran-network/baran-os/core/taxonomy"
	"github.com/baran-network/baran-os/core/testutil"
)

// newTaxonomyRegistry creates a KVRegistry with the standard taxonomy catalog.
func newTaxonomyRegistry(t *testing.T) *registry.KVRegistry {
	t.Helper()
	_, nc := testutil.StartNATS(t)
	ctx := context.Background()
	reg, err := registry.NewKVRegistryWithCatalog(ctx, nc, 3, 6, taxonomy.NewStandardCatalog())
	if err != nil {
		t.Fatalf("create taxonomy registry: %v", err)
	}
	return reg
}

// TestCatalogLookup verifies exact and missing lookups.
func TestCatalogLookup(t *testing.T) {
	cat := taxonomy.NewStandardCatalog()

	entry := cat.Lookup("nlp.summarization")
	if entry == nil {
		t.Fatal("expected entry for nlp.summarization")
	}
	if entry.Category != "nlp" || entry.Action != "summarization" {
		t.Errorf("unexpected entry: %+v", entry)
	}

	missing := cat.Lookup("nonexistent.capability")
	if missing != nil {
		t.Error("expected nil for unknown capability")
	}
}

// TestCatalogQuery verifies hierarchical glob queries.
func TestCatalogQuery(t *testing.T) {
	cat := taxonomy.NewStandardCatalog()

	nlpEntries := cat.Query("nlp.*")
	if len(nlpEntries) != 8 {
		t.Errorf("expected 8 nlp entries, got %d", len(nlpEntries))
	}
	for _, e := range nlpEntries {
		if e.Category != "nlp" {
			t.Errorf("unexpected category %q in nlp.* query", e.Category)
		}
	}

	all := cat.Query("*.*")
	if len(all) != 48 {
		t.Errorf("expected 48 total entries, got %d", len(all))
	}
}

// TestCatalogValidate verifies standard, vendor, and invalid capability names.
func TestCatalogValidate(t *testing.T) {
	cat := taxonomy.NewStandardCatalog()

	if err := cat.Validate("nlp.summarization"); err != nil {
		t.Errorf("expected valid standard capability, got: %v", err)
	}

	if err := cat.Validate("acme.wildfire.risk_assessment"); err != nil {
		t.Errorf("expected valid vendor capability, got: %v", err)
	}

	if err := cat.Validate("custom_thing"); err == nil {
		t.Error("expected error for single-segment capability")
	}

	if err := cat.Validate("nlp.nonexistent"); err == nil {
		t.Error("expected error for nlp.nonexistent (nlp is a reserved category)")
	}
}

// TestCatalogAutoMap verifies that standard capabilities get taxonomy fields filled in.
func TestCatalogAutoMap(t *testing.T) {
	cat := taxonomy.NewStandardCatalog()

	cap := &taxonomy.Capability{Name: "nlp.summarization"}
	if err := cat.AutoMap(cap); err != nil {
		t.Fatalf("auto-map: %v", err)
	}
	if cap.Category != "nlp" || cap.Action != "summarization" {
		t.Errorf("unexpected auto-map result: category=%q action=%q", cap.Category, cap.Action)
	}
	if len(cap.InputTypes) == 0 {
		t.Error("expected input_types to be populated")
	}
}

// TestTaxonomyRegistrationAndDiscovery verifies the full US1 scenario:
// register agent with nlp.summarization, query nlp.* and exact nlp.summarization.
func TestTaxonomyRegistrationAndDiscovery(t *testing.T) {
	reg := newTaxonomyRegistry(t)
	ctx := context.Background()

	// Register agent with a standard taxonomy capability.
	_, err := reg.Register(ctx, registry.AgentRegistration{
		AgentID:   "agent-nlp-001",
		AgentType: "nlp-agent",
		Version:   "1.0.0",
		Capabilities: []registry.Capability{
			{Name: "nlp.summarization", Version: "1.0.0"},
		},
		NodeID: "node-1",
	})
	if err != nil {
		t.Fatalf("register agent: %v", err)
	}

	// Exact match.
	exact, err := reg.FindByCapability(ctx, "nlp.summarization", "")
	if err != nil {
		t.Fatalf("find by exact capability: %v", err)
	}
	if len(exact) != 1 || exact[0].AgentID != "agent-nlp-001" {
		t.Errorf("unexpected exact matches: %+v", exact)
	}

	// Verify auto-mapping populated taxonomy fields.
	if exact[0].Capabilities[0].Category != "nlp" {
		t.Errorf("expected category=nlp, got %q", exact[0].Capabilities[0].Category)
	}
	if exact[0].Capabilities[0].Action != "summarization" {
		t.Errorf("expected action=summarization, got %q", exact[0].Capabilities[0].Action)
	}

	// Hierarchical glob: nlp.* should return the same agent.
	glob, err := reg.FindByCapability(ctx, "nlp.*", "")
	if err != nil {
		t.Fatalf("find by glob capability: %v", err)
	}
	if len(glob) != 1 || glob[0].AgentID != "agent-nlp-001" {
		t.Errorf("unexpected glob matches: %+v", glob)
	}
}

// TestFullCatalogQuery verifies that the capability-catalog KV is seeded with 48 entries.
func TestFullCatalogQuery(t *testing.T) {
	cat := taxonomy.NewStandardCatalog()
	all := cat.Query("*.*")
	if len(all) != 48 {
		t.Errorf("expected 48 standard catalog entries, got %d", len(all))
	}
}

// TestInvalidCapabilityRejected verifies that single-segment and reserved-category
// capabilities are rejected at registration time.
func TestInvalidCapabilityRejected(t *testing.T) {
	reg := newTaxonomyRegistry(t)
	ctx := context.Background()

	cases := []struct {
		name       string
		capability string
	}{
		// Single segment — fails rule 4 (must have ≥2 segments).
		{"single_segment", "single_word"},
		// First segment is a standard category but action doesn't exist in catalog.
		{"reserved_category", "nlp.nonexistent_action"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := reg.Register(ctx, registry.AgentRegistration{
				AgentID:   "agent-bad-" + tc.name,
				AgentType: "bad-agent",
				Version:   "1.0.0",
				Capabilities: []registry.Capability{
					{Name: tc.capability, Version: "1.0.0"},
				},
				NodeID: "node-1",
			})
			if err == nil {
				t.Errorf("expected error registering with %q, got nil", tc.capability)
			}
		})
	}
}
