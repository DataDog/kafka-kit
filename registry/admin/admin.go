package admin

import (
	"context"
)

// Client is an admin client.
type Client interface {
	Close()
	CreateTopic(context.Context, CreateTopicConfig) error
}

// CreateTopicConfig holds CreateTopic parameters.
type CreateTopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
	ReplicaAssignment [][]int32
}

type Config struct {
	Type string

	// Kafka native admin configs.
	BootstrapServers string
}
