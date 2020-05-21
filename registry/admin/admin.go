package admin

import (
	"context"
)

// Client is an admin client.
type Client interface {
	Close()
	CreateTopic(context.Context, CreateTopicConfig) error
	DeleteTopic(ctx context.Context, topicName string) error
}

type Config struct {
	Type             string
	BootstrapServers string
	SSLCALocation    string
	SecurityProtocol string
	SASLMechanism    string
	SASLUsername     string
	SASLPassword     string
}

// CreateTopicConfig holds CreateTopic parameters.
type CreateTopicConfig struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	Config            map[string]string
	ReplicaAssignment ReplicaAssignment
}

type ReplicaAssignment [][]int32
