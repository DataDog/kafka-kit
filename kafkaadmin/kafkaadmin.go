package kafkaadmin

import (
  "github.com/confluentinc/confluent-kafka-go/kafka"
)

type Client interface {
  Close()
  CreateTopic() error
}

type client struct {
  c *kafka.AdminClient
}

func NewClient() (*client, error) {
  c := &client{}
  client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": "localhost",
  })

  c.c = client

  return c, err
}
