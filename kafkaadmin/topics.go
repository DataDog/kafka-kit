package kafkaadmin

import (
	"context"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// CreateTopicConfig holds CreateTopic parameters.
type CreateTopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
	ReplicaAssignment ReplicaAssignment
}

type ReplicaAssignment [][]int32

// CreateTopic creates a topic.
func (c Client) CreateTopic(ctx context.Context, cfg CreateTopicConfig) error {
	spec := kafka.TopicSpecification{
		Topic:             cfg.Name,
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ReplicaAssignment: cfg.ReplicaAssignment,
		Config:            cfg.Config,
	}

	// ReplicaAssignment and ReplicationFactor are mutually exclusive.
	if cfg.ReplicaAssignment != nil {
		spec.ReplicationFactor = 0
	}

	topic := []kafka.TopicSpecification{spec}

	_, err := c.c.CreateTopics(ctx, topic)

	return err
}

// DeleteTopic deletes a topic.
func (c Client) DeleteTopic(ctx context.Context, name string) error {
	_, err := c.c.DeleteTopics(ctx, []string{name})
	return err
}
