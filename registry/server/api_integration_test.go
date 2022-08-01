//go:build integration

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	pb "github.com/DataDog/kafka-kit/v4/registry/registry"

	"github.com/stretchr/testify/assert"
)

func TestCreateTopic(t *testing.T) {
	reg, err := RegistryClient()
	if err != nil {
		t.Fatal(err)
	}

	tests := map[int]*pb.CreateTopicRequest{
		// This should succeed.
		0: {
			Topic: &pb.Topic{Name: "new_topic", Partitions: 1, Replication: 1},
		},
		// This should fail because we're trying to create an existing topic.
		1: {
			Topic: &pb.Topic{Name: "new_topic", Partitions: 1, Replication: 1},
		},
		// This should fail; incomplete request params.
		2: {
			Topic: &pb.Topic{Name: "", Partitions: 1, Replication: 1},
		},
		// This should fail; incomplete request params.
		3: {},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrTopicAlreadyExists,
		2: ErrTopicNameEmpty,
		3: ErrTopicFieldMissing,
	}

	for i := 0; i < len(tests); i++ {
		_, err := reg.CreateTopic(context.Background(), tests[i])
		assert.Equal(t, expectedErrors[i], err, fmt.Sprintf("test %d: %s", i, err))
		// ..."Probabilistic consistency between dependent tests... sure"
		time.Sleep(250 * time.Millisecond)
	}

	// Cleanup.
	for _, topic := range []string{"new_topic", "exists"} {
		reg.DeleteTopic(context.Background(), &pb.TopicRequest{Name: topic})
	}
}

func TestCreateTaggedTopic(t *testing.T) {
	reg, err := RegistryClient()
	if err != nil {
		t.Fatal(err)
	}

	// Tag a broker so we can test partition mapping by tag.
	_, err = reg.TagBroker(context.Background(), &pb.BrokerRequest{Id: 1001, Tag: []string{"key:value"}})
	if err != nil {
		t.Fatal(err)
	}

	// Topics in this test have continued suffix integers in relation to the prior
	// test due to latent handling of deletes in Kafka; it's possible that we try
	// creating a topic here that was marked for deleting but it still exists,
	// inducing flakiness.
	tests := map[int]*pb.CreateTopicRequest{
		// This should succeed.
		0: {
			Topic:            &pb.Topic{Name: "new_topic2", Partitions: 1, Replication: 1},
			TargetBrokerTags: []string{"key:value"},
		},
		// This should fail because we're attempting to map the topic to a tag
		// that isn't present on any brokers.
		1: {
			Topic:            &pb.Topic{Name: "new_topic3", Partitions: 1, Replication: 1},
			TargetBrokerTags: []string{"key:doesnt_exist"},
		},
		// This should fail because we're trying to create more replicas than we
		// have available brokers for.
		2: {
			Topic:            &pb.Topic{Name: "many_partitions", Partitions: 24, Replication: 3},
			TargetBrokerTags: []string{"key:value"},
		},
		3: {
			Topic:            &pb.Topic{Name: "new_topic4", Partitions: 1, Replication: 1},
			TargetBrokerTags: []string{"key:value"},
			TargetBrokerIds:  []uint32{1001},
		},
		4: {
			Topic:           &pb.Topic{Name: "new_topic5", Partitions: 1, Replication: 1},
			TargetBrokerIds: []uint32{1001},
		},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrInsufficientBrokers,
		2: ErrInsufficientBrokers,
		3: nil,
		4: nil,
	}

	for i := 0; i < len(tests); i++ {
		_, err := reg.CreateTopic(context.Background(), tests[i])
		assert.Equal(t, expectedErrors[i], err, fmt.Sprintf("test %d: %s", i, err))
		time.Sleep(250 * time.Millisecond)
	}

	// Cleanup.
	for _, topic := range []string{"new_topic2"} {
		reg.DeleteTopic(context.Background(), &pb.TopicRequest{Name: topic})
	}
}

func TestDeleteTopic(t *testing.T) {
	reg, err := RegistryClient()
	if err != nil {
		t.Fatal(err)
	}

	// Pre-create a topic.
	topicConfig := &pb.CreateTopicRequest{
		Topic: &pb.Topic{Name: "topic_for_delete", Partitions: 1, Replication: 1},
	}

	if _, err := reg.CreateTopic(context.Background(), topicConfig); err != nil {
		t.Fatal(err)
	}

	time.Sleep(250 * time.Millisecond)

	tests := map[int]*pb.TopicRequest{
		0: {
			Name: "topic_for_delete",
		},
		1: {
			Name: "doest_exit",
		},
		2: {
			Name: "",
		},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrTopicNotExist,
		2: ErrTopicNameEmpty,
	}

	for i := 0; i < len(tests); i++ {
		_, err := reg.DeleteTopic(context.Background(), tests[i])
		assert.Equal(t, expectedErrors[i], err, fmt.Sprintf("test %d: %s", i, err))
		time.Sleep(250 * time.Millisecond)
	}
}
