package server

import (
	"context"
	"testing"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

func TestGetTopics(t *testing.T) {
	s := mockServer()

	tests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{},
		1: &pb.TopicRequest{Name: "test_topic"},
		2: &pb.TopicRequest{Tag: []string{"partitions:5"}},
	}

	expected := map[int][]string{
		0: []string{"test_topic", "test_topic2"},
		1: []string{"test_topic"},
		2: []string{"test_topic", "test_topic2"},
	}

	for i, req := range tests {
		resp, err := s.GetTopics(context.Background(), req)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if resp.Topics == nil {
			t.Errorf("Expected a non-nil TopicResponse.Topics field")
		}

		topics := TopicSet(resp.Topics).Names()

		if !stringsEqual(expected[i], topics) {
			t.Errorf("Expected Topic list %s, got %s", expected[i], topics)
		}
	}
}

func TestListTopics(t *testing.T) {
	s := mockServer()

	tests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{},
		1: &pb.TopicRequest{Name: "test_topic"},
		2: &pb.TopicRequest{Tag: []string{"partitions:5"}},
	}

	expected := map[int][]string{
		0: []string{"test_topic", "test_topic2"},
		1: []string{"test_topic"},
		2: []string{"test_topic", "test_topic2"},
	}

	for i, req := range tests {
		resp, err := s.ListTopics(context.Background(), req)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if resp.Names == nil {
			t.Errorf("Expected a non-nil TopicResponse.Topics field")
		}

		topics := resp.Names

		if !stringsEqual(expected[i], topics) {
			t.Errorf("Expected Topic list %s, got %s", expected[i], topics)
		}
	}
}
