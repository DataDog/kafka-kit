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
	SetThrottle(context.Context, SetThrottleConfig) error
	RemoveThrottle(context.Context, RemoveThrottleConfig) error
	GetDynamicConfigs(context.Context, string, []string) (ResourceConfigs, error)
}

// NewClient returns a KafkaAdmin.
func NewClient(cfg Config) (KafkaAdmin, error) {
	return newClient(cfg, kafka.NewAdminClient)
}
