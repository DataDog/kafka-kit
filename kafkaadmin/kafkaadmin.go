// Package kafkaadmin provides Kafka administrative functionality.
package kafkaadmin

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaAdmin interface.
type KafkaAdmin interface {
	Close()
	// Topics.
	CreateTopic(context.Context, CreateTopicConfig) error
	DeleteTopic(context.Context, string) error
	// Cluster.
	SetThrottle(context.Context, ThrottleConfig) error
}

// NewClient returns a KafkaAdmin.
func NewClient(cfg Config) (KafkaAdmin, error) {
	return newClient(cfg, kafka.NewAdminClient)
}
