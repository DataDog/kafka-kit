// Package kafkaadmin provides Kafka administrative functionality.
package kafkaadmin

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaAdmin interface.
type KafkaAdmin interface {
	Close()
	CreateTopic(context.Context, CreateTopicConfig) error
	DeleteTopic(context.Context, string) error
	GetTopics() ([]string, error)
	GetTopicState(string) (*TopicState, error)
}

// NewClient returns a KafkaAdmin.
func NewClient(cfg Config) (KafkaAdmin, error) {
	return newClient(cfg, kafka.NewAdminClient)
}
