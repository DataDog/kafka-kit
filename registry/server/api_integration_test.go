// +build integration

package server

import (
	"context"
	"testing"

	pb "github.com/DataDog/kafka-kit/v3/registry/protos"
)

func TestCreateTopic(t *testing.T) {
	s, err := testIntegrationServer()
	if err != nil {
		t.Fatal(err)
	}

	tests := map[int]*pb.CreateTopicRequest{
		0: &pb.CreateTopicRequest{
			Topic: &pb.Topic{Name: "new_topic", Partitions: 1, Replication: 1},
		},
		1: &pb.CreateTopicRequest{
			Topic: &pb.Topic{Name: "new_topic", Partitions: 1, Replication: 1},
		},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrTopicAlreadyExists,
	}

	for i := 0; i < len(tests); i++ {
		_, err := s.CreateTopic(context.Background(), tests[i])
		if err != expectedErrors[i] {
			t.Errorf("Expected error '%s' for test %d, got '%s'", expectedErrors[i], i, err)
		}
	}
}

func TestCreateTaggedTopic(t *testing.T) {
	s, err := testIntegrationServer()
	if err != nil {
		t.Fatal(err)
	}

	// Tag a broker so we can test partition mapping by tag.
	_, err = s.TagBroker(context.Background(), &pb.BrokerRequest{Id: 1001, Tag: []string{"key:value"}})
	if err != nil {
		t.Fatal(err)
	}

	tests := map[int]*pb.CreateTopicRequest{
		0: &pb.CreateTopicRequest{
			Topic:            &pb.Topic{Name: "new_topic2", Partitions: 1, Replication: 1},
			TargetBrokerTags: []string{"key:value"},
		},
		1: &pb.CreateTopicRequest{
			Topic:            &pb.Topic{Name: "new_topic3", Partitions: 1, Replication: 1},
			TargetBrokerTags: []string{"key:doesnt_exist"}},
	}

	expectedErrors := map[int]error{
		0: nil,
		1: ErrInsufficientBrokers,
	}

	for i := 0; i < len(tests); i++ {
		_, err := s.CreateTopic(context.Background(), tests[i])
		if err != expectedErrors[i] {
			t.Errorf("Expected error '%s' for test %d, got '%s'", expectedErrors[i], i, err)
		}
	}
}

func TestDeleteTopic(t *testing.T) {
	s, err := testIntegrationServer()
	if err != nil {
		t.Fatal(err)
	}

	tests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{
			Name: "new_topic",
		},
	}

	expectedErrors := map[int]error{
		0: nil,
	}

	for i := 0; i < len(tests); i++ {
		_, err := s.DeleteTopic(context.Background(), tests[i])
		if err != expectedErrors[i] {
			t.Errorf("Expected error '%s' for test %d, got '%s'", expectedErrors[i], i, err)
		}
	}
}
