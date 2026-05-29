package kafkametrics

import (
	"fmt"
)

// APIError wraps backend
// metric system errors.
type APIError struct {
	Request string
	Message string
}

// Error implements the error
// interface for APIError.
func (e *APIError) Error() string {
	return fmt.Sprintf("API error [%s]: %s", e.Request, e.Message)
}

// NoResults types are returned
// when incomplete broker metrics or
// metadata is returned.
type NoResults struct {
	Message string
}

// Error implements the error
// interface for PartialResults.
func (e *NoResults) Error() string {
	return e.Message
}

// PartialResults types are returned
// when incomplete broker metrics or
// metadata is returned.
type PartialResults struct {
	Message string
}

// Error implements the error
// interface for PartialResults.
func (e *PartialResults) Error() string {
	return e.Message
}
