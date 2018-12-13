package server

import (
	"context"
	"testing"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

func TestGetBrokers(t *testing.T) {
	s := mockServer()

	tests := map[int]*pb.BrokerRequest{
		0: &pb.BrokerRequest{},
		1: &pb.BrokerRequest{Id: 1002},
		2: &pb.BrokerRequest{Tag: []string{"rack:a"}},
	}

	expected := map[int]idList{
		0: idList{1001, 1002, 1003, 1004, 1005},
		1: idList{1002},
		2: idList{1001, 1004},
	}

	for i, req := range tests {
		resp, err := s.GetBrokers(context.Background(), req)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if resp.Brokers == nil {
			t.Errorf("Expected a non-nil BrokerResponse.Brokers field")
		}

		brokers := BrokerSet(resp.Brokers).IDs()

		if !intsEqual(expected[i], brokers) {
			t.Errorf("Expected broker list %v, got %v", expected[i], brokers)
		}
	}
}

func TestListBrokers(t *testing.T) {
	s := mockServer()

	tests := map[int]*pb.BrokerRequest{
		0: &pb.BrokerRequest{},
		1: &pb.BrokerRequest{Id: 1002},
		2: &pb.BrokerRequest{Tag: []string{"rack:a"}},
	}

	expected := map[int]idList{
		0: idList{1001, 1002, 1003, 1004, 1005},
		1: idList{1002},
		2: idList{1001, 1004},
	}

	for i, req := range tests {
		resp, err := s.ListBrokers(context.Background(), req)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if resp.Ids == nil {
			t.Errorf("Expected a non-nil BrokerResponse.Ids field")
		}

		brokers := resp.Ids

		if !intsEqual(expected[i], brokers) {
			t.Errorf("Expected broker list %v, got %v", expected[i], brokers)
		}
	}
}
