// Package kafkaadmin wraps Kafka admin API calls.
package kafkaadmin

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type KafkaAdmin interface {
	Close()
	CreateTopic(context.Context, CreateTopicConfig) error
	DeleteTopic(context.Context, string) error
}

// NewClient returns a new admin Client.
func NewClient(cfg Config) (KafkaAdmin, error) {
	return newClient(cfg, kafka.NewAdminClient)
}
