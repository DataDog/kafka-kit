// Package kafkaadmin wraps Kafka admin API calls.
package kafkaadmin

import (
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// Client is a kafkaadmin client.
type Client interface {
	Close()
	CreateTopic(CreateTopicConfig) error
}

type client struct {
	c *kafka.AdminClient
}

// Config holds Client configuration parameters.
type Config struct {
	BootstrapServers string
}

// NewClient returns a new Client.
func NewClient(cfg Config) (Client, error) {
	c := client{}
	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
	})

	c.c = client

	return c, err
}

// Close closes the client.
func (c client) Close() {
	c.c.Close()
}

// CreateTopicConfig holds CreateTopic parameters.
type CreateTopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
}

// CreateTopic creates a topic.
func (c client) CreateTopic(cfg CreateTopicConfig) error {
	return nil
}
