package kafkaadmin

import (
	"context"
	"fmt"
	"regexp"
	"time"

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

// ReplicaAssignment is a [][]int32 of partition assignments. The outer slice
// index maps to the partition ID (ie index position 3 describes partition 3
// for the reference topic), the inner slice is an []int32 of broker assignments.
type ReplicaAssignment [][]int32

// TopicStates is a map of topic names to TopicState.
type TopicStates map[string]TopicState

// TopicState describes the current state of a topic.
type TopicState struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
	PartitionStates   map[int]PartitionState
}

// PartitionState describes the state of a partition.
type PartitionState struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
}

// CreateTopic creates a topic.
func (c Client) CreateTopic(ctx context.Context, cfg CreateTopicConfig) error {
	spec := kafka.TopicSpecification{
		Topic:             cfg.Name,
		NumPartitions:     cfg.Partitions,
		ReplicationFactor: cfg.ReplicationFactor,
		ReplicaAssignment: cfg.ReplicaAssignment,
		Config:            cfg.Config,
	}

	// ReplicaAssignment and ReplicationFactor are
	// mutually exclusive.
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

// DescribeTopics takes a []*regexp.Regexp and returns a TopicState for all topics
// with names that match any of the regex patterns specified.
func (c Client) DescribeTopics(ctx context.Context, topics []*regexp.Regexp) (TopicStates, error) {
	// GetMetadata(topic *string, allTopics bool, timeoutMs int) (*Metadata, error)
	var timeout time.Duration
	if dl, set := ctx.Deadline(); set {
		timeout = dl.Sub(time.Now())
	} else {
		timeout = defaultTimeout
	}

	// If we're getting the metadata for <= 5 topics
	md, err := c.c.GetMetadata(nil, true, int(timeout.Milliseconds()))
	if err != nil {
		return nil, err
	}

	fmt.Printf("%+v\n", md)
	return nil, nil
}
