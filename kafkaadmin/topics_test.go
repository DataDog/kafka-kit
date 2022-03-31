package kafkaadmin

import (
	"testing"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
)

func TestTopicStatesFromMetadata(t *testing.T) {
	// Mock metadata.
	md := fakeKafkaMetadata()
	// Get a TopicStates.
	ts, err := topicStatesFromMetadata(md)
	assert.Nil(t, err)

	// Expected results.
	expected := NewTopicStates()

	// test1 topic.
	test1state := fakeTopicState("test1", 2)
	test1state.setPartitionState(0, []int32{1001, 1002}, []int32{1001, 1002})
	test1state.setPartitionState(1, []int32{1002}, []int32{1002})
	expected["test1"] = test1state

	// test2 topic.
	test2state := fakeTopicState("test2", 2)
	test2state.setPartitionState(0, []int32{1003, 1002}, []int32{1003, 1002})
	test2state.setPartitionState(1, []int32{1002, 1003}, []int32{1003, 1002})
	expected["test2"] = test2state

	assert.Equal(t, expected, ts)
}

// fakeTopicState takes a topic name and desired number of partitions and returns
// a TopicState. Note that the PartitionStates are left empty; those are to be
// filled as needed in each test.
func fakeTopicState(name string, partitions int32) TopicState {
	ts := NewTopicState(name)
	ts.Partitions = partitions
	ts.ReplicationFactor = 2
	ts.PartitionStates = map[int]PartitionState{}
	for i := int32(0); i < partitions; i++ {
		ts.PartitionStates[int(i)] = PartitionState{
			ID: i,
		}
	}

	return ts
}

// setPartitionState takes a partition ID, the desired assigned replica and ISR
// and sets the partition state accordingly
func (t TopicState) setPartitionState(id int32, replicas []int32, isr []int32) {
	ps := t.PartitionStates[int(id)]
	ps.Leader = isr[0]
	ps.Replicas = replicas
	ps.ISR = isr
	t.PartitionStates[int(id)] = ps
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
