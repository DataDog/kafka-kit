//go:build integration

package kafkaadmin

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var (
	testIntegrationTestTopicName = "integration-test-topic"
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
	assert.Greater(t, int(ts["test1"].PartitionStates[0].Leader), 1, "unexpected leader ID")
}

func TestDescribeTopicsMulti(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	// Fetch the topic automatically created by docker compose.
	ts, err := ka.DescribeTopics(ctx, []string{".*"})
	assert.Nil(t, err)

	assert.Greater(t, len(ts), 1, "expected multiple topics in TopicStates")
}

func TestCreateTopic(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	// Configs.
	cfg := CreateTopicConfig{
		Name:              testIntegrationTestTopicName,
		Partitions:        2,
		ReplicationFactor: 2,
		Config:            map[string]string{"flush.ms": "1234"},
	}

	err := ka.CreateTopic(ctx, cfg)
	assert.Nil(t, err)

	time.Sleep(250 * time.Millisecond)

	// This assumes that DescribeTopics works.
	ts, err := ka.DescribeTopics(ctx, []string{testIntegrationTestTopicName})
	assert.Nil(t, err)

	// Test that configs were applied.
	assert.Equal(t, testIntegrationTestTopicName, ts[testIntegrationTestTopicName].Name)
	assert.Equal(t, int32(2), ts[testIntegrationTestTopicName].Partitions)
	assert.Equal(t, int32(2), ts[testIntegrationTestTopicName].ReplicationFactor)

	// Check configs.
	// This assumes that GetConfigs works.
	topicConfigs, err := ka.GetConfigs(ctx, "topic", []string{testIntegrationTestTopicName})
	assert.Nil(t, err)

	assert.Equal(t, "1234", topicConfigs[testIntegrationTestTopicName]["flush.ms"])
}

func TestDeleteTopic(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	topic := fmt.Sprintf("%s-to-delete", testIntegrationTestTopicName)

	// Create a temp topic.
	// This assumes CreateTopic works.

	cfg := CreateTopicConfig{
		Name:              topic,
		Partitions:        1,
		ReplicationFactor: 2,
	}

	err := ka.CreateTopic(ctx, cfg)
	assert.Nil(t, err)

	time.Sleep(250 * time.Millisecond)

	// Make sure it was creatd.
	ts, err := ka.DescribeTopics(ctx, []string{topic})
	assert.Nil(t, err)
	assert.Equal(t, 1, len(ts))

	// Delete the topic.
	err = ka.DeleteTopic(ctx, topic)
	assert.Nil(t, err)

	time.Sleep(250 * time.Millisecond)

	// Check that the topic is gone.
	ts, err = ka.DescribeTopics(ctx, []string{topic})
	assert.Equal(t, "no data returned", err.Error())
	assert.Equal(t, 0, len(ts))
}
