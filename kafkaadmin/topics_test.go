package kafkaadmin

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestTopicStatesFromMetadata(t *testing.T) {
	md := fakeKafkaMetadata()

	_, err := topicStatesFromMetadata(md)
	assert.Nil(t, err)
}

func fakeKafkaMetadata() *kafka.Metadata {
	var noErr = kafka.NewError(kafka.ErrNoError, "Success", false)

	return &kafka.Metadata{
		Brokers: []kafka.BrokerMetadata{
			{
				ID:   1001,
				Host: "host-a",
				Port: 9092,
			},
			{
				ID:   1002,
				Host: "host-b",
				Port: 9092,
			},
			{
				ID:   1003,
				Host: "host-c",
				Port: 9092,
			},
		},
		Topics: map[string]kafka.TopicMetadata{
			"test1": {
				Topic: "test1",
				Partitions: []kafka.PartitionMetadata{
					{
						ID:       0,
						Error:    noErr,
						Leader:   1001,
						Replicas: []int32{1001, 1002},
						Isrs:     []int32{1001, 1002},
					},
				},
				Error: noErr,
			},
		},
		OriginatingBroker: kafka.BrokerMetadata{
			ID:   1001,
			Host: "host-a",
			Port: 9092,
		},
	}
}
