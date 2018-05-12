package datadog

import (
	"fmt"
)

// APIError wraps backend
// metric system errors.
type APIError struct {
	request string
	err     string
}

// Error implements the error
// interface for APIError.
func (e *APIError) Error() string {
	return fmt.Sprintf("API error [%s]: %s", e.request, e.err)
}

// PartialResults types are returned
// when incomplete broker metrics or
// metadata is returned.
type PartialResults struct {
	err string
}

// Error implements the error
// interface for PartialResults.
func (e *PartialResults) Error() string {
	return e.err
}
