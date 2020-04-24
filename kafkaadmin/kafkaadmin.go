// Package kafkaadmin wraps Kafka admin API calls.
package kafkaadmin

import (
	"context"
	"fmt"

	"github.com/DataDog/kafka-kit/registry/admin"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type FactoryFunc func(conf *kafka.ConfigMap) (*kafka.AdminClient, error)

type Client struct {
	c *kafka.AdminClient
}

// Config holds Client configuration parameters.
type Config struct {
	BootstrapServers string
	SSLEnabled       bool
	SSLCALocation    string
}

// NewClient returns a new Client.
func NewClient(cfg Config) (*Client, error) {
	return newClient(cfg, kafka.NewAdminClient)
}

// NewClientWithFactory returns a new Client using a factory func for the kafkaAdminClient
func NewClientWithFactory(cfg Config, factory FactoryFunc) (*Client, error) {
	return newClient(cfg, factory)
}

func newClient(cfg Config, factory FactoryFunc) (*Client, error) {
	c := &Client{}

	kafkaCfg := &kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	}

	if cfg.SSLEnabled {
		kafkaCfg.SetKey("security.protocol", "SSL")
		if cfg.SSLCALocation == "" {
			return nil, fmt.Errorf("kafka SSL is enabled but SSLCALocation was not provided")
		}
		kafkaCfg.SetKey("ssl.ca.location", cfg.SSLCALocation)
	}

	k, err := factory(kafkaCfg)
	c.c = k

	if err != nil {
		err = fmt.Errorf("[librdkafka] %s", err)
	}
	return c, err
}

// Close closes the Client.
func (c Client) Close() {
	c.c.Close()
}

// CreateTopic creates a topic.
func (c Client) CreateTopic(ctx context.Context, cfg admin.CreateTopicConfig) error {
	spec := kafka.TopicSpecification{
		Topic:             cfg.Name,
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ReplicaAssignment: cfg.ReplicaAssignment,
		Config:            cfg.Config,
	}

	// ReplicaAssignment and ReplicationFactor are
	// mutually exclusive.
	if cfg.ReplicaAssignment != nil {
		spec.ReplicationFactor = 0
	}

	topic := []kafka.TopicSpecification{spec}

	_, err := c.c.CreateTopics(ctx, topic)

	return err
}
