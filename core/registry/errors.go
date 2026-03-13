package registry

import "errors"

// ErrNotFound is returned when an agent is not found in the registry.
var ErrNotFound = errors.New("agent not found")

// ErrCASConflict is returned when a CAS operation fails due to a revision mismatch.
var ErrCASConflict = errors.New("CAS conflict: revision mismatch")

// ErrValidation is returned when registration data fails validation.
var ErrValidation = errors.New("validation error")
