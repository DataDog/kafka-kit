// +build integration

package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/DataDog/kafka-kit/v3/kafkaadmin"
	pb "github.com/DataDog/kafka-kit/v3/registry/api"
)

func TestCreateTopic(t *testing.T) {
	s, err := testIntegrationServer()
	if err != nil {
		t.Fatal(err)
	}

	ka, err := kafkaAdminClient()
	if err != nil {
		t.Fatal(err)
	}

	// Pre-create a topic.
	topicConfig := kafkaadmin.CreateTopicConfig{
		Name:              "exists",
		Partitions:        1,
		ReplicationFactor: 1,
	}

	if err := ka.CreateTopic(context.Background(), topicConfig); err != nil {
		t.Fatal(err)
	}

	tests := map[int]*pb.CreateTopicRequest{
		// This should succeed.
		0: &pb.CreateTopicRequest{
			Topic: &pb.Topic{Name: "new_topic", Partitions: 1, Replication: 1},
		},
		// This should fail because we're trying to create an existing topic.
		1: &pb.CreateTopicRequest{
			Topic: &pb.Topic{Name: "exists", Partitions: 1, Replication: 1},
		},
		// This should fail; incomplete request params.
		2: &pb.CreateTopicRequest{
			Topic: &pb.Topic{Name: "", Partitions: 1, Replication: 1},
		},
		// This should fail; incomplete request params.
		3: &pb.CreateTopicRequest{},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrTopicAlreadyExists,
		2: ErrTopicNameEmpty,
		3: ErrTopicFieldMissing,
	}

	for i := 0; i < len(tests); i++ {
		_, err := s.CreateTopic(context.Background(), tests[i])
		if err != expectedErrors[i] {
			t.Errorf("Expected error '%s' for test %d, got '%s'", expectedErrors[i], i, err)
		}
	}

	// Cleanup.
	for _, topic := range []string{"new_topic", "exists"} {
		ka.DeleteTopic(context.Background(), topic)
	}
}

func TestCreateTaggedTopic(t *testing.T) {
	s, err := testIntegrationServer()
	if err != nil {
		t.Fatal(err)
	}

	ka, err := kafkaAdminClient()
	if err != nil {
		t.Fatal(err)
	}

	// Tag a broker so we can test partition mapping by tag.
	_, err = s.TagBroker(context.Background(), &pb.BrokerRequest{Id: 1001, Tag: []string{"key:value"}})
	if err != nil {
		t.Fatal(err)
	}

	// Topics in this test have continued suffix integers in relation to the prior
	// test due to latent handling of deletes in Kafka; it's possible that we try
	// creating a topic here that was marked for deleting but it still exists,
	// inducing flakiness.
	tests := map[int]*pb.CreateTopicRequest{
		// This should succeed.
		0: &pb.CreateTopicRequest{
			Topic:            &pb.Topic{Name: "new_topic2", Partitions: 1, Replication: 1},
			TargetBrokerTags: []string{"key:value"},
		},
		// This should fail because we're attempting to map the topic to a tag
		// that isn't present on any brokers.
		1: &pb.CreateTopicRequest{
			Topic:            &pb.Topic{Name: "new_topic3", Partitions: 1, Replication: 1},
			TargetBrokerTags: []string{"key:doesnt_exist"},
		},
		// This should fail because we're trying to create more replicas than we
		// have available brokers for.
		2: &pb.CreateTopicRequest{
			Topic:            &pb.Topic{Name: "many_partitions", Partitions: 24, Replication: 3},
			TargetBrokerTags: []string{"key:value"},
		},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrInsufficientBrokers,
		2: ErrInsufficientBrokers,
	}

	for i := 0; i < len(tests); i++ {
		_, err := s.CreateTopic(context.Background(), tests[i])
		if err != expectedErrors[i] {
			t.Errorf("Expected error '%s' for test %d, got '%s'", expectedErrors[i], i, err)
		}
	}

	// Cleanup.
	for _, topic := range []string{"new_topic2"} {
		ka.DeleteTopic(context.Background(), topic)
	}
}

func TestDeleteTopic(t *testing.T) {
	s, err := testIntegrationServer()
	if err != nil {
		t.Fatal(err)
	}

	ka, err := kafkaAdminClient()
	if err != nil {
		t.Fatal(err)
	}

	// Pre-create a topic.
	topicConfig := kafkaadmin.CreateTopicConfig{
		Name:              "topic_for_delete",
		Partitions:        1,
		ReplicationFactor: 1,
	}

	if err := ka.CreateTopic(context.Background(), topicConfig); err != nil {
		t.Fatal(err)
	}

	tests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{
			Name: "topic_for_delete",
		},
		1: &pb.TopicRequest{
			Name: "doest_exit",
		},
		2: &pb.TopicRequest{
			Name: "",
		},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrTopicNotExist,
		2: ErrTopicNameEmpty,
	}

	for i := 0; i < len(tests); i++ {
		_, err := s.DeleteTopic(context.Background(), tests[i])
		if err != expectedErrors[i] {
			t.Errorf("Expected error '%s' for test %d, got '%s'", expectedErrors[i], i, err)
		}
	}
}

// Recursive search.
func allChildren(p string) []string {
	paths := []string{p}

	children, _ := store.ZK.Children(p)
	for _, c := range children {
		paths = append(paths, allChildren(fmt.Sprintf("%s/%s", p, c))...)
	}

	return paths
}
