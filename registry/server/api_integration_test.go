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
		1: &pb.CreateTopicRequest{
			Topic: &pb.Topic{Name: "new_topic", Partitions: 1, Replication: 1},
		},
	}

	expectedErrors := map[int]error{
		1: nil,
	}

	for i, req := range tests {
		_, err := s.CreateTopic(context.Background(), req)
		if err != expectedErrors[i] {
			t.Errorf("Expected error '%s' for test %d, got '%s'", expectedErrors[i], i, err)
		}
	}
}
