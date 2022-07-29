package stub

import (
	"testing"

	"github.com/DataDog/kafka-kit/v4/kafkaadmin"

	"github.com/stretchr/testify/assert"
)

func TestUnderReplicated(t *testing.T) {
	md := fakeKafkaMetadata()
	md.Topics["test2"].Partitions[1].Isrs = []int32{1003}

	ts, err := kafkaadmin.TopicStatesFromMetadata(&md)
	assert.Nil(t, err)

	urp := ts.UnderReplicated()

	expected := kafkaadmin.NewTopicStates()
	expected["test2"] = kafkaadmin.TopicState{
		Name:              "test2",
		Partitions:        2,
		ReplicationFactor: 2,
		PartitionStates: map[int]kafkaadmin.PartitionState{
			0: {
				ID:       0,
				Leader:   1003,
				Replicas: []int32{1003, 1002},
				ISR:      []int32{1003, 1002},
			},
			1: {
				ID:       1,
				Leader:   1003,
				Replicas: []int32{1002, 1003},
				ISR:      []int32{1003},
			},
		},
	}

	assert.Equal(t, expected, urp)
}
