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

// DescribeTopics takes a []string of topic names. Topic names can be name literals
// or optional regex. A TopicStates is returned for all matching topics.
func (c Client) DescribeTopics(ctx context.Context, topics []string) (TopicStates, error) {
	md, err := c.getMetadata(ctx)
	if err != nil {
		return nil, err
	}

	// Strip topics that don't match any of the specified names.
	topicNamesRegex, err := stringsToRegex(topics)
	if err != nil {
		return nil, err
	}

	filterMatches(md, topicNamesRegex)

	return TopicStatesFromMetadata(md)
}

// UnderReplicatedTopics returns a TopicStates that only includes under-replicated
// topics.
func (c Client) UnderReplicatedTopics(ctx context.Context) (TopicStates, error) {
	// Fetch all topics.
	topicStates, err := c.DescribeTopics(ctx, []string{".*"})
	if err != nil {
		return nil, err
	}

	return topicStates.UnderReplicated(), nil
}

func (c Client) getMetadata(ctx context.Context) (*kafka.Metadata, error) {
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

	return md, nil
}

func filterMatches(md *kafka.Metadata, re []*regexp.Regexp) {
	for topic := range md.Topics {
		var keep bool
		for _, r := range re {
			if r.MatchString(topic) {
				keep = true
			}
		}
		if !keep {
			delete(md.Topics, topic)
		}
	}
}

func TopicStatesFromMetadata(md *kafka.Metadata) (TopicStates, error) {
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

// List returns a []string of all topic names in the TopicStates.
func (t TopicStates) List() []string {
	var names []string
	for n := range t {
		names = append(names, n)
	}
	return names
}

// Brokers returns a list of all brokers assigned to any partition in the
// TopicState.
func (t TopicState) Brokers() []int {
	var brokers []int
	var seen = map[int32]struct{}{}

	for _, partn := range t.PartitionStates {
		for _, id := range partn.Replicas {
			if _, exists := seen[id]; !exists {
				seen[id] = struct{}{}
				brokers = append(brokers, int(id))
			}
		}
	}

	return brokers
}
