// Package kafkaadmin wraps Kafka admin API calls.
package kafkaadmin

import (
	"context"

	"github.com/DataDog/kafka-kit/registry/admin"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Client struct {
	c *kafka.AdminClient
}

// Config holds Client configuration parameters.
type Config struct {
	BootstrapServers string
}

// NewClient returns a new Client.
func NewClient(cfg Config) (Client, error) {
	c := Client{}
	k, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	})

	c.c = k

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
