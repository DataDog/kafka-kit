//go:build integration

package kafkaadmin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDescribeTopicsSingle(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	// Fetch the topic automatically created by docker compose.
	ts, err := ka.DescribeTopics(ctx, []string{"test1"})
	assert.Nil(t, err)

	assert.Equal(t, "test1", ts["test1"].Name)
	assert.Equal(t, int32(1), ts["test1"].Partitions)
	assert.Equal(t, int32(3), ts["test1"].ReplicationFactor)
	// The partition states of an automatically created topic are non-deterministic,
	// so we'll just spot check that the data approximately exists.
	pLen := len(ts["test1"].PartitionStates[0].Replicas)
	assert.Equal(t, 3, pLen, "unexpected replicas len")
	assert.Less(t, 1, int(ts["test1"].PartitionStates[0].Leader), "unexpected leader ID")
}
