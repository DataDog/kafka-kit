package server

import (
	"context"
	"testing"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

func TestGetBrokers(t *testing.T) {
	s := testServer()

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
	s := testServer()

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

func TestCustomTagBrokerFilter(t *testing.T) {
	s := testServer()

	s.Tags.Store.SetTags(
		KafkaObject{Type: "broker", ID: "1001"},
		TagSet{"customtag": "customvalue"},
	)

	s.Tags.Store.SetTags(
		KafkaObject{Type: "broker", ID: "1002"},
		TagSet{
			"customtag":  "customvalue",
			"customtag2": "customvalue2",
		},
	)

	tests := map[int]*pb.BrokerRequest{
		0: &pb.BrokerRequest{Tag: []string{"customtag:customvalue"}},
		1: &pb.BrokerRequest{Tag: []string{"customtag2:customvalue2"}},
		2: &pb.BrokerRequest{Tag: []string{"nomatches:forthistag"}},
	}

	expected := map[int]idList{
		0: idList{1001, 1002},
		1: idList{1002},
		2: idList{},
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

func TestBrokerMappings(t *testing.T) {
	s := testServer()

	tests := map[int]*pb.BrokerRequest{
		0: &pb.BrokerRequest{Id: 1002},
	}

	expected := map[int][]string{
		0: []string{"test_topic", "test_topic2"},
	}

	for i, req := range tests {
		resp, err := s.BrokerMappings(context.Background(), req)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if resp.Names == nil {
			t.Errorf("Expected a non-nil TopicResponse.Names field")
		}

		if !stringsEqual(expected[i], resp.Names) {
			t.Errorf("Expected Topic list %s, got %s", expected[i], resp.Names)
		}
	}

	// Test invalid ID.
	req := &pb.BrokerRequest{Id: 1010}
	_, err := s.BrokerMappings(context.Background(), req)

	if err != ErrBrokerNotExist {
		t.Errorf("Unexpected error: %s", err)
	}

	// Test no ID.
	req = &pb.BrokerRequest{}
	_, err = s.BrokerMappings(context.Background(), req)

	if err != ErrBrokerIDEmpty {
		t.Errorf("Unexpected error: %s", err)
	}
}
