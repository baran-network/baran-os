package taxonomy

// Validator validates capability names against the standard catalog.
// It is a thin wrapper around Catalog.Validate and Catalog.AutoMap for use
// in the registry layer, which holds a Catalog reference.
type Validator struct {
	catalog Catalog
}

// NewValidator creates a Validator backed by the given catalog.
func NewValidator(catalog Catalog) *Validator {
	return &Validator{catalog: catalog}
}

// Validate checks whether capabilityName is a valid standard or vendor capability.
// Returns nil on success, a descriptive error on failure.
func (v *Validator) Validate(capabilityName string) error {
	return v.catalog.Validate(capabilityName)
}

// AutoMap fills Category, Action, InputTypes, and OutputTypes on cap if it
// matches a standard catalog entry. No-op for vendor capabilities.
func (v *Validator) AutoMap(cap *Capability) error {
	return v.catalog.AutoMap(cap)
}
