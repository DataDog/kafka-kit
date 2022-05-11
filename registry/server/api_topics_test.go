package server

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/kafka-kit/v4/kafkazk"
	"github.com/DataDog/kafka-kit/v4/mapper"
	pb "github.com/DataDog/kafka-kit/v4/registry/registry"
)

func TestGetTopics(t *testing.T) {
	s := testServer()

	tests := map[int]*pb.TopicRequest{
		0: {},
		1: {Name: "test_topic"},
		2: {Tag: []string{"partitions:5"}},
		3: {WithReplicas: true},
	}

	// pb.TopicResponse{Topics: topics}
	expectedNames := map[int][]string{
		0: {"test_topic", "test_topic2"},
		1: {"test_topic"},
		2: {"test_topic", "test_topic2"},
		3: {"test_topic", "test_topic2"},
	}

	expectedReplicas := map[int]map[uint32]pb.Replicas{
		3: {
			0: {Ids: []uint32{1000, 1001}},
			1: {Ids: []uint32{1002, 1003}},
			2: {Ids: []uint32{1004, 1005}},
			3: {Ids: []uint32{1006, 1007}},
			4: {Ids: []uint32{1008, 1009}},
		},
	}

	for i, req := range tests {
		resp, err := s.GetTopics(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}

		if resp.Topics == nil {
			t.Errorf("Expected a non-nil TopicResponse.Topics field")
		}

		topics := TopicSet(resp.Topics).Names()

		if !stringsEqual(expectedNames[i], topics) {
			t.Errorf("Expected Topic list %s, got %s", expectedNames[i], topics)
		}

		for _, topic := range resp.Topics {
			v, exist := topic.Configs["retention.ms"]
			if !exist {
				t.Error("Expected 'retention.ms' config key to exist")
			}
			if v != "172800000" {
				t.Errorf("Expected config value '172800000', got '%s'", v)
			}
		}
		if exp, ok := expectedReplicas[i]; ok {
			full := resp.Topics
			for _, topic := range topics {
				for partition, replicas := range full[topic].Replicas {
					assert.ElementsMatch(t, replicas.Ids, exp[partition].Ids, "Unexpected partitions")
				}
			}
		}
	}
}

func TestListTopics(t *testing.T) {
	s := testServer()

	tests := map[int]*pb.TopicRequest{
		0: {},
		1: {Spanning: true, Name: "test_topic"},
		2: {Spanning: true, Tag: []string{"partitions:5"}},
		// These should now fail; it's the same test as the last two cases, but
		// the increase in brokers should cause these topics to fail to satisfy
		// the spanning property.
		3: {Spanning: true, Tag: []string{"partitions:5"}},
		4: {Spanning: true, Tag: []string{"partitions:5"}},
	}

	expected := map[int][]string{
		0: {"test_topic", "test_topic2"},
		1: {"test_topic"},
		2: {"test_topic", "test_topic2"},
		3: {},
		4: {},
	}

	// Order matters.
	for i := 0; i < len(tests); i++ {
		req := tests[i]
		if i == 3 {
			// we need to add a bunch of unused brokers to underlying Kafka/ZK stub
			// to ensure that test cases 3 and 4 fail to return the same results as
			// 1 and 2.
			brokers := map[int]mapper.BrokerMeta{
				1008: {},
				1009: {},
				1010: {},
				1011: {},
				1012: {},
				1013: {},
			}

			s.ZK.(*kafkazk.Stub).AddBrokers(brokers)
		}

		resp, err := s.ListTopics(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}

		if resp.Names == nil {
			t.Errorf("Expected a non-nil TopicResponse.Topics field")
		}

		topics := resp.Names

		if !stringsEqual(expected[i], topics) {
			t.Errorf("Expected topic list %s, got %s", expected[i], topics)
		}
	}
}

func TestReassigningTopics(t *testing.T) {
	s := testServer()

	out, err := s.ReassigningTopics(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(out.Names) != 1 || out.Names[0] != "reassigning_topic" {
		t.Errorf("Unexpected reassigning topic output")
	}
}

func TestUnderReplicated(t *testing.T) {
	s := testServer()

	out, err := s.UnderReplicatedTopics(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(out.Names) != 1 || out.Names[0] != "underreplicated_topic" {
		t.Errorf("Unexpected under replicated topic output")
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
		0: {Tag: []string{"customtag:customvalue"}},
		1: {Tag: []string{"customtag2:customvalue2"}},
		2: {Tag: []string{"nomatches:forthistag"}},
	}

	expected := map[int][]string{
		0: {"test_topic", "test_topic2"},
		1: {"test_topic2"},
		2: {},
	}

	for i, req := range tests {
		resp, err := s.ListTopics(context.Background(), req)
		if err != nil {
			t.Fatal(err)
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
		0: {Name: "test_topic", Tag: []string{"k:v"}},
		1: {Tag: []string{"k:v"}},
		2: {Name: "test_topic", Tag: []string{}},
		3: {Name: "test_topic"},
		4: {Name: "test_topic20", Tag: []string{"k:v"}},
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
		t.Fatal(err)
	}

	// Delete two tags.
	req = &pb.TopicRequest{
		Name: "test_topic",
		Tag:  []string{"k", "k2"},
	}

	_, err = s.DeleteTopicTags(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Fetch tags.
	req = &pb.TopicRequest{Name: "test_topic"}
	resp, err := s.GetTopics(context.Background(), req)
	if err != nil {
		t.Fatal(err)
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
		0: {Tag: []string{"key"}},
		1: {Name: "test_topic"},
		2: {Name: "test_topic", Tag: []string{}},
		3: {Name: "test_topic20", Tag: []string{"key"}},
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
		0: {Name: "test_topic"},
	}

	expected := map[int][]uint32{
		0: {1001, 1002, 1003, 1004},
	}

	for i, req := range tests {
		resp, err := s.TopicMappings(context.Background(), req)
		if err != nil {
			t.Fatal(err)
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
		t.Fatal(err)
	}
}
