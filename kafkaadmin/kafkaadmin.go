// Package kafkaadmin wraps Kafka admin API calls.
package kafkaadmin

import (
  "github.com/confluentinc/confluent-kafka-go/kafka"
)

// Client is a kafkaadmin client.
type Client interface {
  Close()
  CreateTopic() error
}

type client struct {
  C *kafka.AdminClient
}

// Config holds Client configuration parameters.
type Config struct {
  BootstrapServers string
}

// NewClient returns a new Client.
func NewClient(cfg Config) (*client, error) {
  c := &client{}
  client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": cfg.BootstrapServers,
  })

  c.C = client

  return c, err
}
