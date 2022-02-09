package kafkaadmin

import (
	"context"
)

// ThrottleConfig holds SetThrottle configs.
type ThrottleConfig struct{}

// SetThrottle takes a ThrottleConfig and sets the underlying throttle configs
// accordingly. A throttle is a combination of topic throttled replicas configs
// and broker inbound/outbound throttle configs.
func (c Client) SetThrottle(ctx context.Context, cfg ThrottleConfig) error {
	return nil
}
