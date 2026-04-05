// Package taxonomy provides a structured capability catalog with hierarchical
// dot-notation names, vendor namespace support, and alias resolution.
package taxonomy

import (
	"fmt"
	"path"
	"strings"
)

const catalogVersion = "v1.0"

// TaxonomyCategory represents a top-level capability grouping.
type TaxonomyCategory struct {
	Name        string
	Description string
}

// TaxonomyEntry represents a single well-known capability in the standard catalog.
type TaxonomyEntry struct {
	Name           string
	Category       string
	Action         string
	Description    string
	InputTypes     []string
	OutputTypes    []string
	CatalogVersion string
}

// Capability mirrors the registry capability, used by AutoMap.
type Capability struct {
	Name        string
	Version     string
	Description string
	Parameters  map[string]string
	Category    string
	Action      string
	InputTypes  []string
	OutputTypes []string
}

// Catalog provides access to the standard capability taxonomy.
type Catalog interface {
	// Lookup returns a taxonomy entry by exact name. Returns nil if not found.
	Lookup(name string) *TaxonomyEntry

	// Query returns all entries matching a glob pattern (e.g., "nlp.*").
	Query(pattern string) []TaxonomyEntry

	// Categories returns all top-level categories.
	Categories() []TaxonomyCategory

	// Version returns the catalog version string.
	Version() string

	// Validate checks if a capability name is valid (standard or vendor).
	// Returns nil if valid, error with guidance if invalid.
	Validate(capabilityName string) error

	// IsStandardCategory returns true if the name is a known standard category.
	IsStandardCategory(name string) bool

	// AutoMap populates Category, Action, InputTypes, OutputTypes from the
	// catalog for a standard capability. No-op for vendor capabilities.
	AutoMap(cap *Capability) error
}

// standardCatalog is the in-memory implementation backed by catalog_data.go.
type standardCatalog struct {
	entries    map[string]TaxonomyEntry
	categories map[string]TaxonomyCategory
}

// NewStandardCatalog returns the standard v1.0 capability catalog.
func NewStandardCatalog() Catalog {
	c := &standardCatalog{
		entries:    make(map[string]TaxonomyEntry, len(standardEntries)),
		categories: make(map[string]TaxonomyCategory, len(standardCategories)),
	}
	for _, e := range standardEntries {
		c.entries[e.Name] = e
	}
	for _, cat := range standardCategories {
		c.categories[cat.Name] = cat
	}
	return c
}

func (c *standardCatalog) Lookup(name string) *TaxonomyEntry {
	e, ok := c.entries[name]
	if !ok {
		return nil
	}
	return &e
}

func (c *standardCatalog) Query(pattern string) []TaxonomyEntry {
	var results []TaxonomyEntry
	for _, e := range standardEntries {
		matched, err := path.Match(pattern, e.Name)
		if err != nil {
			// Invalid pattern — treat as no match.
			continue
		}
		if matched {
			results = append(results, e)
		}
	}
	return results
}

func (c *standardCatalog) Categories() []TaxonomyCategory {
	result := make([]TaxonomyCategory, len(standardCategories))
	copy(result, standardCategories)
	return result
}

func (c *standardCatalog) Version() string {
	return catalogVersion
}

func (c *standardCatalog) IsStandardCategory(name string) bool {
	_, ok := c.categories[name]
	return ok
}

func (c *standardCatalog) Validate(capabilityName string) error {
	// Rule 1: exact match in standard catalog → valid standard capability.
	if _, ok := c.entries[capabilityName]; ok {
		return nil
	}

	parts := strings.Split(capabilityName, ".")

	// Rule 2: single segment or first segment is a standard category → reject.
	if len(parts) < 2 {
		return fmt.Errorf("capability %q is invalid: must be a standard catalog entry (e.g., %q) or a vendor namespace with at least 2 dot-separated segments (e.g., %q)",
			capabilityName, "nlp.summarization", "acme.myskill")
	}

	if c.IsStandardCategory(parts[0]) {
		return fmt.Errorf("capability %q is invalid: %q is a reserved standard category; use a catalog entry like %q.%s or a different vendor namespace",
			capabilityName, parts[0], parts[0], parts[1])
	}

	// Rule 3: ≥2 segments, first is not a standard category → valid vendor capability.
	return nil
}

func (c *standardCatalog) AutoMap(cap *Capability) error {
	entry, ok := c.entries[cap.Name]
	if !ok {
		// Vendor capability — no-op.
		return nil
	}
	cap.Category = entry.Category
	cap.Action = entry.Action
	if len(cap.InputTypes) == 0 {
		cap.InputTypes = append([]string(nil), entry.InputTypes...)
	}
	if len(cap.OutputTypes) == 0 {
		cap.OutputTypes = append([]string(nil), entry.OutputTypes...)
	}
	return nil
}
