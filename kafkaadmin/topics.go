package kafkaadmin

import (
	"context"
	"fmt"
	"strconv"

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

/*Get all topics, query kafka directrly rather than zk
in: none

out: []string of all topic names
	 error
*/
func (c Client) GetTopics() ([]string, error) {
	var path string
	const TIMEOUT = 10000

	if c.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/topics", c.Prefix)
	} else {
		path = "/brokers/topics"
	}

	ret, er := c.c.GetMetadata(&path, true, TIMEOUT)
	if er != nil {
		return nil, er
	}

	topicsList := []string{}
	for item := range ret.Topics {
		topicsList = append(topicsList, item)
	}

	return topicsList, nil
}

//TopicState struct
type TopicState struct {
	Partitions map[string][]int `json:"partitions"`
}

/* GetTopicState, query kafka directly rather than zk
in: (t string) -  a topic name

out:
	*TopicStateF - return Partitions map
	 error
*/
func (c Client) GetTopicState(t string) (*TopicState, error) {
	var path string //
	const TIMEOUT = 10000

	if c.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/topics/%s", c.Prefix, t)
	} else {
		path = fmt.Sprintf("/brokers/topics/%s", t)
	}

	ret, er := c.c.GetMetadata(&path, true, TIMEOUT)
	if er != nil {
		return nil, er
	}

	if ret.Topics[t].Topic != "" {
		topics := ret.Topics[t]
		temp := make(map[string][]int)

		for _, item := range topics.Partitions {
			var partition []int
			for _, ite := range item.Replicas {
				partition = append(partition, int(ite))
			}
			id := strconv.Itoa(int(item.ID))
			temp[id] = partition
		}

		tsf := &TopicState{Partitions: temp}

		return tsf, nil
	}

	err := fmt.Errorf("Topic %s No Found in: "+path, t)

	return nil, err
}
