package stub

import (
	"github.com/DataDog/kafka-kit/v4/kafkaadmin"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// fakeTopicState takes a topic name and desired number of partitions and returns
// a TopicState. Note that the PartitionStates are left empty; those are to be
// filled as needed in each test.
func fakeTopicState(name string, partitions int32) kafkaadmin.TopicState {
	ts := kafkaadmin.NewTopicState(name)
	ts.Partitions = partitions
	ts.ReplicationFactor = 2
	ts.PartitionStates = map[int]kafkaadmin.PartitionState{}
	for i := int32(0); i < partitions; i++ {
		ts.PartitionStates[int(i)] = kafkaadmin.PartitionState{
			ID: i,
		}
	}

	return ts
}

func fakeKafkaMetadata() kafka.Metadata {
	var noErr = kafka.NewError(kafka.ErrNoError, "Success", false)

	return kafka.Metadata{
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
					{
						ID:       1,
						Error:    noErr,
						Leader:   1002,
						Replicas: []int32{1002},
						Isrs:     []int32{1002},
					},
				},
				Error: noErr,
			},
			"test2": {
				Topic: "test2",
				Partitions: []kafka.PartitionMetadata{
					{
						ID:       0,
						Error:    noErr,
						Leader:   1003,
						Replicas: []int32{1003, 1002},
						Isrs:     []int32{1003, 1002},
					},
					{
						ID:       1,
						Error:    noErr,
						Leader:   1003,
						Replicas: []int32{1002, 1003},
						Isrs:     []int32{1003, 1002},
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
