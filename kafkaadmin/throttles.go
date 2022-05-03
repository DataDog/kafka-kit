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
	topicThrottledFollowersCfgName = "follower.replication.throttled.replicas"
)

// SetThrottleConfig holds SetThrottle configs.
type SetThrottleConfig struct {
	// Topics is a list of all topics that require throttled replica configs.
	Topics []string
	// Brokers is a mapping of broker ID to BrokerThrottleConfig.
	Brokers map[int]BrokerThrottleConfig
}

// RemoveThrottleConfig holds lists of all topics and brokers to remove throttles
// from.
type RemoveThrottleConfig struct {
	Topics  []string
	Brokers []int
}

// BrokerThrottleConfig defines an inbound and outbound throttle rate in bytes
// to be applied to a broker.
type BrokerThrottleConfig struct {
	InboundLimitBytes  int
	OutboundLimitBytes int
}

// SetThrottle takes a SetThrottleConfig and sets the underlying throttle configs
// accordingly. A throttle is a combination of topic throttled replicas configs
// and broker inbound/outbound throttle configs.
func (c Client) SetThrottle(ctx context.Context, cfg SetThrottleConfig) error {
	var topicDynamicConfigs, brokerDynamicConfigs ResourceConfigs
	var err error

	// Get the named topic dynamic configs.
	if len(cfg.Topics) > 0 {
		topicDynamicConfigs, err = c.GetDynamicConfigs(ctx, "topic", cfg.Topics)
		if err != nil {
			return ErrSetThrottle{Message: err.Error()}
		}
	}

	// Get the named broker ID dynamic configs.
	if len(cfg.Brokers) > 0 {
		var brokerIDs []string
		for id := range cfg.Brokers {
			brokerIDs = append(brokerIDs, fmt.Sprintf("%d", id))
		}

		brokerDynamicConfigs, err = c.GetDynamicConfigs(ctx, "broker", brokerIDs)
		if err != nil {
			return ErrSetThrottle{Message: err.Error()}
		}
	}

	// Update the fetched configs to include the desired new configs.
	if err := populateTopicThrottleConfigs(cfg.Topics, topicDynamicConfigs); err != nil {
		return ErrSetThrottle{Message: err.Error()}
	}

	// Update the broker configs to the desired new configs.
	if err := populateBrokerThrottleConfigs(cfg.Brokers, brokerDynamicConfigs); err != nil {
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

	// Apply the configs in sequence.
	for _, config := range throttleConfigs {
		// TODO(jamie) perform these in batch once the 'Only one ConfigResource of
		// type BROKER is allowed per call' error is no longer encountered.
		// TODO(jamie) review whether the kafka.SetAdminIncremental AlterConfigsAdminOption
		// actually works here.
		if _, err = c.c.AlterConfigs(ctx, []kafka.ConfigResource{config}); err != nil {
			return ErrSetThrottle{Message: err.Error()}
		}
	}

	return nil
}

// RemoveThrottle takes a RemoveThrottleConfig that includes an optionally specified
// list of brokers and topics to remove all throttle configurations from.
func (c Client) RemoveThrottle(ctx context.Context, cfg RemoveThrottleConfig) error {
	var topicDynamicConfigs, brokerDynamicConfigs ResourceConfigs
	var err error

	// Get the named topic dynamic configs.
	if len(cfg.Topics) > 0 {
		topicDynamicConfigs, err = c.GetDynamicConfigs(ctx, "topic", cfg.Topics)
		if err != nil {
			return ErrRemoveThrottle{Message: err.Error()}
		}
	}

	// Get the named broker ID dynamic configs.
	if len(cfg.Brokers) > 0 {
		var brokerIDs []string
		for _, id := range cfg.Brokers {
			brokerIDs = append(brokerIDs, fmt.Sprintf("%d", id))
		}

		brokerDynamicConfigs, err = c.GetDynamicConfigs(ctx, "broker", brokerIDs)
		if err != nil {
			return ErrRemoveThrottle{Message: err.Error()}
		}
	}

	// Update the fetched configs to include the desired new configs.
	if err := clearTopicThrottleConfigs(topicDynamicConfigs); err != nil {
		return ErrRemoveThrottle{Message: err.Error()}
	}

	// Update the broker configs to the desired new configs.
	if err := clearBrokerThrottleConfigs(brokerDynamicConfigs); err != nil {
		return ErrRemoveThrottle{Message: err.Error()}
	}

	// Build a new configuration set.
	var throttleConfigs []kafka.ConfigResource

	// Merge all configs into the global configuration set.
	for i, resourceConfig := range []ResourceConfigs{topicDynamicConfigs, brokerDynamicConfigs} {
		for name, configs := range resourceConfig {
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

	// Apply the configs in sequence.
	for _, config := range throttleConfigs {
		// TODO(jamie) perform these in batch once the 'Only one ConfigResource of
		// type BROKER is allowed per call' error is no longer encountered.
		// TODO(jamie) review whether the kafka.SetAdminIncremental AlterConfigsAdminOption
		// actually works here.
		if _, err = c.c.AlterConfigs(ctx, []kafka.ConfigResource{config}); err != nil {
			return ErrRemoveThrottle{Message: err.Error()}
		}
	}

	return nil
}

// populateTopicThrottleConfigs takes a list of topics that should have a throttle config
// set along with a ResourceConfigs. We need both; the provided ResourceConfigs
// will only include topics that have at least one preexisting dynamic config.
// If the topic from the topics list exists in the ResourceConfigs, we append the
// throttle config. If it doesn't exist, we create the entry.
func populateTopicThrottleConfigs(topics []string, configs ResourceConfigs) error {
	// Remove any topics in the ResourceConfigs that aren't in the topics list.
	nameSet := map[string]struct{}{}
	for _, t := range topics {
		nameSet[t] = struct{}{}
	}

	for t := range configs {
		if _, present := nameSet[t]; !present {
			delete(configs, t)
		}
	}

	// Update the configs.
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

// clearTopicThrottleConfigs takes a ResourceConfigs and searches for topics with
// any throttle replicas configuration. If the configuration exists, it's cleared.
// Otherwise the topic is removed from the ResourceConfigs as a configuration
// update does not need to be sent.
func clearTopicThrottleConfigs(configs ResourceConfigs) error {
	for topic, config := range configs {
		_, hasLeaderCfg := config[topicThrottledLeadersCfgName]
		_, hasFollowersCfg := config[topicThrottledFollowersCfgName]

		// If either are set, remove the keys so they can be reset to the Kafka
		// default.
		if hasLeaderCfg || hasFollowersCfg {
			delete(config, topicThrottledLeadersCfgName)
			delete(config, topicThrottledFollowersCfgName)
		} else {
			// If we have neither leader nor follower config, we don't need to send
			// a configuration update at all.
			delete(configs, topic)
		}
	}

	return nil
}

// populateBrokerThrottleConfigs takes a map of BrokerThrottleConfig for brokers that should
// have a throttle config set along with a ResourceConfigs. We need both; the provided
// ResourceConfigs will only include brokers that have at least one preexisting
// dynamic config. If the broker from the map exists in the ResourceConfigs, we
// append the throttle config. If it doesn't exist, we create the entry.
func populateBrokerThrottleConfigs(brokers map[int]BrokerThrottleConfig, configs ResourceConfigs) error {
	// Remove any brokers in the ResourceConfigs that don't have a BrokerThrottleConfig.
	for idStr := range configs {
		id, _ := strconv.Atoi(idStr)
		if _, present := brokers[id]; !present {
			delete(configs, idStr)
		}
	}

	for brokerID, throttleRates := range brokers {
		var err error

		// String values.
		id := strconv.Itoa(brokerID)
		txRate := fmt.Sprintf("%d", throttleRates.OutboundLimitBytes)
		rxRate := fmt.Sprintf("%d", throttleRates.InboundLimitBytes)

		// Write configs. We skip any zero configs which are interpreted as unset.
		if throttleRates.OutboundLimitBytes != 0 {
			err = configs.AddConfig(id, brokerTXThrottleCfgName, txRate)
			if err != nil {
				return err
			}
		}
		if throttleRates.InboundLimitBytes != 0 {
			err = configs.AddConfig(id, brokerRXThrottleCfgName, rxRate)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// clearTopicThrottleConfigs takes a ResourceConfigs and searches for brokers with
// any throttle replicas configuration. If the configuration exists, it's cleared.
// Otherwise the broker is removed from the ResourceConfigs as a configuration
// update does not need to be sent.
func clearBrokerThrottleConfigs(configs ResourceConfigs) error {
	for broker, config := range configs {
		_, hasLeaderCfg := config[brokerTXThrottleCfgName]
		_, hasFollowersCfg := config[brokerRXThrottleCfgName]

		// If either are set, remove the keys so they can be reset to the Kafka
		// default.
		if hasLeaderCfg || hasFollowersCfg {
			delete(config, brokerTXThrottleCfgName)
			delete(config, brokerRXThrottleCfgName)
		} else {
			// If we have neither leader nor follower config, we don't need to send
			// a configuration update at all.
			delete(configs, broker)
		}
	}

	return nil
}
