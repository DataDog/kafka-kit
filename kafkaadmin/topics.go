package kafkaadmin

import (
	"context"
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

// NewTopicStates initializes a TopicStates.
func NewTopicStates() TopicStates {
	return make(map[string]TopicState)
}

// NewTopicState initializes a TopicState.
func NewTopicState(name string) TopicState {
	return TopicState{
		Name:            name,
		PartitionStates: make(map[int]PartitionState),
	}
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
	// Use the context deadline remaining budget if set, otherwise use the default
	// timeout value.
	var timeout time.Duration
	if dl, set := ctx.Deadline(); set {
		timeout = dl.Sub(time.Now())
	} else {
		timeout = defaultTimeout
	}

	// Request the cluster metadata.
	md, err := c.c.GetMetadata(nil, true, int(timeout.Milliseconds()))
	if err != nil {
		return nil, ErrorFetchingMetadata{Message: err.Error()}
	}

	return topicStatesFromMetadata(md)
}

func topicStatesFromMetadata(md *kafka.Metadata) (TopicStates, error) {
	if len(md.Topics) == 0 {
		return nil, ErrNoData
	}

	// Extract the topic metadata and populate it into the TopicStates.
	var topicStates = NewTopicStates()

	// For each topic in the global metadata, translate its metadata to a TopicState.
	for topic, topicMeta := range md.Topics {
		topicState := NewTopicState(topic)

		var maxSeenReplicaLen int

		// Scan the partitions and map the states.
		for _, partn := range topicMeta.Partitions {
			// Track the max seen replicas len to infer the replication factor. This may
			// be worth referring to the Kafka code since it is possible to explicitly
			// configure partitions to have variable replica set sizes; perhaps an
			// average makes more sense?
			if maxSeenReplicaLen < len(partn.Replicas) {
				maxSeenReplicaLen = len(partn.Replicas)
			}

			topicState.PartitionStates[int(partn.ID)] = PartitionState{
				ID:       partn.ID,
				Leader:   partn.Leader,
				Replicas: partn.Replicas,
				ISR:      partn.Isrs,
			}
		}

		// Set the general topic attributes.
		topicState.ReplicationFactor = int32(maxSeenReplicaLen)
		topicState.Partitions = int32(len(topicMeta.Partitions))

		// Add the TopicState to the global TopicStates.
		topicStates[topic] = topicState
	}

	return topicStates, nil
}
