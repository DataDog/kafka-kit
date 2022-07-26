package kafkaadmin

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	topicResourceType, _  = kafka.ResourceTypeFromString("topic")
	brokerResourceType, _ = kafka.ResourceTypeFromString("broker")
)

// ResourceConfigs is a map of resource name to a map of configuration name
// and configuration value
// Example: map["my_topic"]map["retention.ms"] = "4000000"
type ResourceConfigs map[string]map[string]string

// GetDynamicConfigs takes a kafka resource type (ie topic, broker) and
// list of names and returns a ResourceConfigs for all dynamic configurations
// discovered for each resource by name.
func (c Client) GetDynamicConfigs(ctx context.Context, kind string, names []string) (ResourceConfigs, error) {
	return c.getConfigs(ctx, kind, names, true)
}

// GetConfigs takes a kafka resource type (ie topic, broker) and list of names
// and returns a ResourceConfigs for all configurations discovered for each
// resource by name. Nil configs are excluded.
func (c Client) GetConfigs(ctx context.Context, kind string, names []string) (ResourceConfigs, error) {
	return c.getConfigs(ctx, kind, names, false)
}

func (c Client) getConfigs(ctx context.Context, kind string, names []string, onlyDynamic bool) (ResourceConfigs, error) {
	var ckgType kafka.ResourceType
	switch kind {
	case "topic":
		ckgType = topicResourceType
	case "broker":
		ckgType = brokerResourceType
	default:
		return nil, fmt.Errorf("invalid resource type")
	}

	if len(names) == 0 {
		return nil, fmt.Errorf("no resource names provided")
	}

	// Populate the results into the ResourceConfigs.
	var results = make(ResourceConfigs)

	// Fetch the config for each resource sequentially.
	// TODO(jamie) do this in batch when it becomes possible.
	for _, n := range names {
		// Populate the ConfigResource request.
		cr := kafka.ConfigResource{
			Type: ckgType,
			Name: n,
		}

		// Request.
		resourceConfigs, err := c.c.DescribeConfigs(ctx, []kafka.ConfigResource{cr})
		if err != nil {
			return nil, ErrorFetchingMetadata{err.Error()}
		}

		// Populate results.
		for _, config := range resourceConfigs {
			for _, v := range config.Config {
				switch onlyDynamic {
				// We need to populate only configs that are dynamic.
				case true:
					if v.Source == kafka.ConfigSourceDynamicTopic || v.Source == kafka.ConfigSourceDynamicBroker {
						results.AddConfigEntry(config.Name, v)
					}
				// Otherwise we populate all configs.
				default:
					results.AddConfigEntry(config.Name, v)
				}
			}
		}
	}

	return results, nil
}

// AddConfig takes a resource name and populates the config key to the specified
// value.
func (rc ResourceConfigs) AddConfig(name, key, value string) error {
	if name == "" || key == "" || value == "" {
		return fmt.Errorf("all parameters must be non-empty")
	}

	if _, ok := rc[name]; !ok {
		rc[name] = make(map[string]string)
	}

	rc[name][key] = value

	return nil
}

// AddConfigEntry takes a resource name (ie a broker ID or topic name) and a
// kafka.ConfigEntryResult. It populates ResourceConfigs with the provided
// ConfigEntryResult for the respective resource by name.
func (rc ResourceConfigs) AddConfigEntry(name string, config kafka.ConfigEntryResult) error {
	if config.Name == "" {
		return fmt.Errorf("empty ConfigEntryResult name")
	}

	if config.Value == "" {
		return fmt.Errorf("empty ConfigEntryResult value")
	}

	if _, ok := rc[name]; !ok {
		rc[name] = make(map[string]string)
	}

	rc[name][config.Name] = config.Value

	return nil
}
