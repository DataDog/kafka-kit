package kafkaadmin

import (
	"context"
	"fmt"
)

// ThrottleConfig holds SetThrottle configs.
type ThrottleConfig struct {
	// Topics is a list of all topics that require throttled replica configs.
	Topics []string
	// Brokers is a mapping of broker ID to BrokerThrottleConfig.
	Brokers map[int]BrokerThrottleConfig
}

// BrokerThrottleConfig defines an inbound and outbound throttle rate in bytes
// to be applied to a broker.
type BrokerThrottleConfig struct {
	InboundLimitBytes  float64
	OutboundLimitBytes float64
}

// SetThrottle takes a ThrottleConfig and sets the underlying throttle configs
// accordingly. A throttle is a combination of topic throttled replicas configs
// and broker inbound/outbound throttle configs.
func (c Client) SetThrottle(ctx context.Context, cfg ThrottleConfig) error {
	// 1) Map ThrottleConfig to ckg ConfigResource.

	// 2) Get topic configs.
	topicDynamicConfigs, err := c.DynamicConfigMapForResources(ctx, "topic", cfg.Topics)
	if err != nil {
		return err
	}

	fmt.Printf("%+v\n", topicDynamicConfigs)

	// 3) Populate new configs.

	// 4) Get broker configs.

	// 2) Get topic configs.
	var brokerIDs []string
	for id := range cfg.Brokers {
		brokerIDs = append(brokerIDs, fmt.Sprintf("%d", id))
	}

	brokerDynamicConfigs, err := c.DynamicConfigMapForResources(ctx, "broker", brokerIDs)
	if err != nil {
		return err
	}

	fmt.Printf("%+v\n", brokerDynamicConfigs)

	// 5) Populate broker configs.

	// 6) Apply.
	return nil
}
