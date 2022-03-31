package kafkaadmin

import (
	"fmt"
)

// ErrSetThrottle is a generic error for SetThrottle.
type ErrSetThrottle struct{ Message string }

func (e ErrSetThrottle) Error() string {
	return fmt.Sprintf("failed to set throttles: %s", e.Message)
}

// ErrRemoveThrottle is a generic error for RemoveThrottle.
type ErrRemoveThrottle struct{ Message string }

func (e ErrRemoveThrottle) Error() string {
	return fmt.Sprintf("failed to remove throttles: %s", e.Message)
}

// ErrorFetchingMetadata is an error encountered fetching Kafka cluster metadata.
type ErrorFetchingMetadata struct{ Message string }

func (e ErrorFetchingMetadata) Error() string {
	return fmt.Sprintf("error fetching metadata: %s", e.Message)
}
