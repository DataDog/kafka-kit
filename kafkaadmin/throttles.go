package kafkaadmin

import (
	"context"
	"fmt"
	"strconv"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

const (
	brokerTXThrottleCfgName        = "leader.replication.throttled.rate"
	brokerRXThrottleCfgName        = "follower.replication.throttled.rate"
	topicThrottledLeadersCfgName   = "leader.replication.throttled.replicas"
	topicThrottledFollowersCfgName = "leader.replication.throttled.replicas"
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
	// Get the named topic dynamic configs.
	topicGetDynamicConfigs, err := c.GetDynamicConfigs(ctx, "topic", cfg.Topics)
	if err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	// Get the named broker ID dynamic configs.
	var brokerIDs []string
	for id := range cfg.Brokers {
		brokerIDs = append(brokerIDs, fmt.Sprintf("%d", id))
	}

	brokerGetDynamicConfigs, err := c.GetDynamicConfigs(ctx, "broker", brokerIDs)
	if err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	// Build a new configuration set.
	var throttleConfigs []kafka.ConfigResource

	// Update the fetched configs to include the desired new configs.
	for _, topic := range cfg.Topics {
		// We need to update the leader and follower throttle replicas list.
		for _, cfgName := range []string{topicThrottledLeadersCfgName, topicThrottledFollowersCfgName} {
			err := topicGetDynamicConfigs.AddConfig(topic, cfgName, "*")
			if err != nil {
				return ErrSetThrottle{Message: err.Error()}
			}
		}
	}

	// Update the broker configs to the desired new configs.
	for brokerID, throttleRates := range cfg.Brokers {
		var err error

		// String values.
		id := strconv.Itoa(brokerID)
		txRate := fmt.Sprintf("%f", throttleRates.OutboundLimitBytes)
		rxRate := fmt.Sprintf("%f", throttleRates.InboundLimitBytes)

		// Write configs.
		err = brokerGetDynamicConfigs.AddConfig(id, brokerTXThrottleCfgName, txRate)
		if err != nil {
			return ErrSetThrottle{Message: err.Error()}
		}
		err = brokerGetDynamicConfigs.AddConfig(id, brokerRXThrottleCfgName, rxRate)
		if err != nil {
			return ErrSetThrottle{Message: err.Error()}
		}
	}

	// Merge all configs into the global configuration set.

	fmt.Printf("%+v\n", topicGetDynamicConfigs)
	fmt.Printf("%+v\n", brokerGetDynamicConfigs)
	fmt.Printf("%+v\n", throttleConfigs)

	return nil
}
