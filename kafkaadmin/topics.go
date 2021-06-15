package kafkaadmin

import (
	"context"
	"regexp"

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

// GetTopics takes a []*regexp.Regexp and returns a []string of all topic names
// that match any of the provided regex.
func (c Client) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	metadata, err := c.c.GetMetadata(nil, true, FetchMetadataTimeoutMs)
	if err != nil {
		return nil, err
	}

	// Get all topics that match all provided topic regexps.
	// Add matches to a slice
	topicsList := []string{}
	for id := range ts {
		for _, topic := range metadata.Topics {
			if ts[id].MatchString(topic.Topic) {
				topicsList = append(topicsList, topic.Topic)
			}
		}

	}

	return topicsList, nil
}

// TopicState represents the partition distribution of a single topic
type TopicState struct {
	Partitions map[int32][]int32
}

// GetTopicState takes a topic name. If the topic exists, the topic state is returned as *TopicState.
func (c Client) GetTopicState(t string) (*TopicState, error) {
	metadata, err := c.c.GetMetadata(&t, false, FetchMetadataTimeoutMs)
	if err != nil {
		return nil, err
	}

	if metadata.Topics[t].Error.Code() != kafka.ErrNoError {
		return nil, metadata.Topics[t].Error
	}

	partitions := make(map[int32][]int32)
	for _, item := range metadata.Topics[t].Partitions {
		partitions[item.ID] = item.Replicas
	}

	return &TopicState{Partitions: partitions}, nil
}

// TopicConfig represents the configuration of a single topic
type TopicConfig struct {
	Config map[string]string
}

// GetTopicConfig takes a topic name. If the topic exists, the topic config is returned as a *TopicConfig.
func (c Client) GetTopicConfig(t string) (*TopicConfig, error) {

	configResult, err := c.c.DescribeConfigs(context.Background(),
		[]kafka.ConfigResource{
			{Type: kafka.ResourceTopic,
				Name:   t,
				Config: nil}})

	if err != nil {
		return nil, err
	}

	if configResult[0].Error.Code() != kafka.ErrNoError {
		return nil, configResult[0].Error
	}

	configs := make(map[string]string)
	for id, v := range configResult[0].Config {
		configs[id] = v.Value
	}

	return &TopicConfig{Config: configs}, nil
}
