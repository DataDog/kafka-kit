package kafkaadmin

import (
	"fmt"
)

// ErrSetThrottle is a generic error for SetThrottle.
type ErrSetThrottle struct {
	Message string
}

func (e ErrSetThrottle) Error() string {
	return fmt.Sprintf("failed to set throttles: %s", e.Message)
}

// ErrRemoveThrottle is a generic error for RemoveThrottle.
type ErrRemoveThrottle struct {
	Message string
}

func (e ErrRemoveThrottle) Error() string {
	return fmt.Sprintf("failed to remove throttles: %s", e.Message)
}
