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
	InboundLimitBytes  int
	OutboundLimitBytes int
}

// SetThrottle takes a ThrottleConfig and sets the underlying throttle configs
// accordingly. A throttle is a combination of topic throttled replicas configs
// and broker inbound/outbound throttle configs.
func (c Client) SetThrottle(ctx context.Context, cfg ThrottleConfig) error {
	// Get the named topic dynamic configs.
	topicDynamicConfigs, err := c.GetDynamicConfigs(ctx, "topic", cfg.Topics)
	if err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	// Get the named broker ID dynamic configs.
	var brokerIDs []string
	for id := range cfg.Brokers {
		brokerIDs = append(brokerIDs, fmt.Sprintf("%d", id))
	}

	brokerDynamicConfigs, err := c.GetDynamicConfigs(ctx, "broker", brokerIDs)
	if err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	// Update the fetched configs to include the desired new configs.
	if err := populateTopicConfigs(cfg.Topics, topicDynamicConfigs); err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	// Update the broker configs to the desired new configs.
	if err := populateBrokerConfigs(cfg.Brokers, brokerDynamicConfigs); err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	// Build a new configuration set.
	var throttleConfigs []kafka.ConfigResource

	// Merge all configs into the global configuration set.
	for i, resourceConfig := range []ResourceConfigs{topicDynamicConfigs, brokerDynamicConfigs} {
		for name, configs := range resourceConfig {
			// StringMapToConfigEntries
			c := kafka.ConfigResource{
				Name:   name,
				Config: kafka.StringMapToConfigEntries(configs, kafka.AlterOperationSet),
			}

			// Assign the type to the respective config class according to the index.
			switch i {
			case 0:
				c.Type = topicResourceType
			case 1:
				c.Type = brokerResourceType
			}

			throttleConfigs = append(throttleConfigs, c)
		}
	}

	// Apply the configs.
	// TODO(jamie) review whether the kafak.SetAdminIncremental AlterConfigsAdminOption
	// actually works here.
	if _, err = c.c.AlterConfigs(ctx, throttleConfigs); err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	return nil
}

func populateTopicConfigs(topics []string, configs ResourceConfigs) error {
	for _, topic := range topics {
		// We need to update the leader and follower throttle replicas list.
		for _, cfgName := range []string{topicThrottledLeadersCfgName, topicThrottledFollowersCfgName} {
			err := configs.AddConfig(topic, cfgName, "*")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func populateBrokerConfigs(brokers map[int]BrokerThrottleConfig, configs ResourceConfigs) error {
	for brokerID, throttleRates := range brokers {
		var err error

		// String values.
		id := strconv.Itoa(brokerID)
		txRate := fmt.Sprintf("%d", throttleRates.OutboundLimitBytes)
		rxRate := fmt.Sprintf("%d", throttleRates.InboundLimitBytes)

		// Write configs.
		err = configs.AddConfig(id, brokerTXThrottleCfgName, txRate)
		if err != nil {
			return err
		}
		err = configs.AddConfig(id, brokerRXThrottleCfgName, rxRate)
		if err != nil {
			return err
		}
	}

	return nil
}
