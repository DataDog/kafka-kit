package server

import (
	"context"
	"testing"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

func TestGetTopics(t *testing.T) {
	s := testServer()

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
	s := testServer()

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

func TestCustomTagTopicFilter(t *testing.T) {
	s := testServer()

	s.Tags.Store.SetTags(
		KafkaObject{Type: "topic", ID: "test_topic"},
		TagSet{"customtag": "customvalue"},
	)

	s.Tags.Store.SetTags(
		KafkaObject{Type: "topic", ID: "test_topic2"},
		TagSet{
			"customtag":  "customvalue",
			"customtag2": "customvalue2",
		},
	)

	tests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{Tag: []string{"customtag:customvalue"}},
		1: &pb.TopicRequest{Tag: []string{"customtag2:customvalue2"}},
		2: &pb.TopicRequest{Tag: []string{"nomatches:forthistag"}},
	}

	expected := map[int][]string{
		0: []string{"test_topic", "test_topic2"},
		1: []string{"test_topic2"},
		2: []string{},
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

func TestTagTopic(t *testing.T) {
	s := testServer()

	tests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{Name: "test_topic", Tag: []string{"k:v"}},
		1: &pb.TopicRequest{Tag: []string{"k:v"}},
		2: &pb.TopicRequest{Name: "test_topic", Tag: []string{}},
		3: &pb.TopicRequest{Name: "test_topic"},
		4: &pb.TopicRequest{Name: "test_topic20", Tag: []string{"k:v"}},
	}

	expected := map[int]error{
		0: nil,
		1: ErrTopicNameEmpty,
		2: ErrNilTags,
		3: ErrNilTags,
		4: ErrTopicNotExist,
	}

	for i, req := range tests {
		_, err := s.TagTopic(context.Background(), req)
		if err != expected[i] {
			t.Errorf("[test %d] Expected err '%v', got '%v'", i, expected[i], err)
		}
	}
}

func TestDeleteTopicTags(t *testing.T) {
	s := testServer()

	// Set tags.
	req := &pb.TopicRequest{
		Name: "test_topic",
		Tag:  []string{"k:v", "k2:v2", "k3:v3"},
	}

	_, err := s.TagTopic(context.Background(), req)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	// Delete two tags.
	req = &pb.TopicRequest{
		Name: "test_topic",
		Tag:  []string{"k", "k2"},
	}

	_, err = s.DeleteTopicTags(context.Background(), req)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	// Fetch tags.
	req = &pb.TopicRequest{Name: "test_topic"}
	resp, err := s.GetTopics(context.Background(), req)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	expected := TagSet{"k3": "v3"}
	got := resp.Topics["test_topic"].Tags
	if !expected.Equal(got) {
		t.Errorf("Expected TagSet %v, got %v", expected, got)
	}
}

func TestDeleteTopicTagsFailures(t *testing.T) {
	s := testServer()

	testRequests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{Tag: []string{"key"}},
		1: &pb.TopicRequest{Name: "test_topic"},
		2: &pb.TopicRequest{Name: "test_topic", Tag: []string{}},
		3: &pb.TopicRequest{Name: "test_topic20", Tag: []string{"key"}},
	}

	expected := map[int]error{
		0: ErrTopicNameEmpty,
		1: ErrNilTags,
		2: ErrNilTags,
		3: ErrTopicNotExist,
	}

	for k := range testRequests {
		_, err := s.DeleteTopicTags(context.Background(), testRequests[k])
		if err != expected[k] {
			t.Errorf("Unexpected error '%s', got '%s'", expected[k], err)
		}
	}
}

func TestTopicMappings(t *testing.T) {
	s := testServer()

	tests := map[int]*pb.TopicRequest{
		0: &pb.TopicRequest{Name: "test_topic"},
	}

	expected := map[int][]uint32{
		0: []uint32{1001, 1002, 1003, 1004},
	}

	for i, req := range tests {
		resp, err := s.TopicMappings(context.Background(), req)
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}

		if resp.Ids == nil {
			t.Errorf("Expected a non-nil BrokerResponse.Ids field")
		}

		if !intsEqual(expected[i], resp.Ids) {
			t.Errorf("Expected broker list %v, got %v", expected[i], resp.Ids)
		}
	}

	// Test no topic name.
	req := &pb.TopicRequest{}
	_, err := s.TopicMappings(context.Background(), req)

	if err != ErrTopicNameEmpty {
		t.Errorf("Unexpected error: %s", err)
	}
}
