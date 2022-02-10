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
