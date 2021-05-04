package admin

import (
	"context"

	"github.com/DataDog/kafka-kit/v3/kafkaadmin"
)

// Client is an admin client.
type Client interface {
	Close()
	CreateTopic(context.Context, kafkaadmin.CreateTopicConfig) error
	DeleteTopic(context.Context, string) error
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
