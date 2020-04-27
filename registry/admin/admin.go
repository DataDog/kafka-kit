package admin

import (
	"context"
)

// Client is an admin client.
type Client interface {
	Close()
	CreateTopic(context.Context, CreateTopicConfig) error
}

type Config struct {
	Type             string
	BootstrapServers string
	SSLEnabled       bool
	SSLCALocation    string
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
