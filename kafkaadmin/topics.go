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

//GetTopics takes a []*regexp.Regexp and returns a []string of all topic
//names that match any of the provided regex, and query kafka directrly rather than zk
func (c Client) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	ret, er := c.c.GetMetadata(nil, true, FetchMetadaTimeoutMs)
	if er != nil {
		return nil, er
	}

	topicsList := []string{}
	for topic := range ret.Topics {
		if ts[0].MatchString(topic) {
			topicsList = append(topicsList, topic)
		}

	}
	return topicsList, nil
}

//TopicState struct
type TopicState struct {
	Partitions map[int32][]int32
}

// GetTopicState takes a topic name. If the topic exists,
//the topic state is returned as a *TopicState, this query kafka directly rather than zk
func (c Client) GetTopicState(t string) (*TopicState, error) {
	ret, er := c.c.GetMetadata(&t, false, FetchMetadaTimeoutMs)
	if er != nil {
		return nil, er
	}

	if ret.Topics[t].Error.String() != "Success" {
		return nil, ret.Topics[t].Error
	}

	Replicas := make(map[int32][]int32)
	for _, item := range ret.Topics[t].Partitions {
		Replicas[item.ID] = item.Replicas
	}
	tsf := &TopicState{Partitions: Replicas}

	return tsf, nil
}

type TopicConfig struct {
	Version int
	Config  map[string]string
}

func (c Client) GetTopicConfig(t string) (*TopicConfig, error) {

	ret, er := c.c.DescribeConfigs(context.Background(),
		[]kafka.ConfigResource{
			{Type: kafka.ResourceTopic,
				Name:   t,
				Config: nil}})

	if er != nil {
		return nil, er
	}

	if ret[0].Error.String() != "Success" {
		return nil, ret[0].Error
	}

	configs := make(map[string]string)
	for id, v := range ret[0].Config {
		configs[id] = v.Value
	}

	config := &TopicConfig{
		Version: 1,
		Config:  configs,
	}
	return config, nil
}
