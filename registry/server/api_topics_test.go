package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
	"github.com/DataDog/kafka-kit/v4/kafkaadmin/stub"
	pb "github.com/DataDog/kafka-kit/v4/registry/registry"
)

func TestGetTopics(t *testing.T) {
	tests := map[int]*pb.TopicRequest{
		0: {},
		1: {Name: "test1"},
		2: {Tag: []string{"partitions:2"}},
		3: {WithReplicas: true},
	}

	// pb.TopicResponse{Topics: topics}
	expectedNames := map[int][]string{
		0: {"test1", "test2"},
		1: {"test1"},
		2: {"test1", "test2"},
		3: {"test1", "test2"},
	}

	expectedReplicas := map[int]map[string]map[uint32]pb.Replicas{
		3: {
			"test1": {
				0: {Ids: []uint32{1001, 1002}},
				1: {Ids: []uint32{1002}},
			},
			"test2": {
				0: {Ids: []uint32{1003, 1002}},
				1: {Ids: []uint32{1002, 1003}},
			},
		},
	}

	for i, req := range tests {
		s := testServer()

		resp, err := s.GetTopics(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}

		if resp.Topics == nil {
			t.Errorf("[case %d] Expected a non-nil TopicResponse.Topics field", i)
		}

		topics := TopicSet(resp.Topics).Names()

		if !stringsEqual(expectedNames[i], topics) {
			t.Errorf("[case %d] Expected Topic list %s, got %s", i, expectedNames[i], topics)
		}

		for _, topic := range resp.Topics {
			v, exist := topic.Configs["retention.ms"]
			if !exist {
				t.Errorf("[case %d] Expected 'retention.ms' config key to exist", i)
			}
			if v != "172800000" {
				t.Errorf("[case %d] Expected config value '172800000', got '%s'", i, v)
			}
		}

		if exp, ok := expectedReplicas[i]; ok {
			full := resp.Topics
			for _, topic := range topics {
				for partition, replicas := range full[topic].Replicas {
					msg := fmt.Sprintf("[case %d] Unexpected partitions", i)
					assert.ElementsMatch(t, replicas.Ids, exp[topic][partition].Ids, msg)
				}
			}
		}
	}
}

func TestListTopics(t *testing.T) {
	s := testServer()

	// Update the stub for metadata specific to this test.

	bs := s.kafkaadmin.(stub.Client).DumpBrokerstates()
	for i := 1003; i <= 1007; i++ {
		delete(bs, i)
	}
	s.kafkaadmin.(stub.Client).LoadBrokerstates(bs)

	tests := map[int]*pb.TopicRequest{
		0: {},
		1: {Spanning: true, Name: "test1"},
		2: {Spanning: true, Tag: []string{"partitions:2"}},
		// These should now fail; it's the same test as the last two cases, but
		// the increase in brokers should cause these topics to fail to satisfy
		// the spanning property.
		3: {Spanning: true, Name: "test1"},
		4: {Spanning: true, Tag: []string{"partitions:2"}},
	}

	expected := map[int][]string{
		0: {"test1", "test2"},
		1: {"test1"},
		2: {"test1", "test2"},
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
			brokers := kafkaadmin.BrokerStates{
				1008: {},
				1009: {},
				1010: {},
				1011: {},
				1012: {},
				1013: {},
			}

			s.kafkaadmin.(stub.Client).AddBrokers(brokers)
		}

		resp, err := s.ListTopics(context.Background(), req)
		if err != nil {
			t.Fatalf("[case %d] %s", i, err)
		}

		if resp.Names == nil {
			t.Errorf("[case %d] Expected a non-nil TopicResponse.Topics field", i)
		}

		topics := resp.Names

		if !stringsEqual(expected[i], topics) {
			t.Errorf("[case %d] Expected topic list %s, got %s", i, expected[i], topics)
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
		KafkaObject{Type: "topic", ID: "test1"},
		TagSet{"customtag": "customvalue"},
	)

	s.Tags.Store.SetTags(
		KafkaObject{Type: "topic", ID: "test2"},
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
		0: {"test1", "test2"},
		1: {"test2"},
		2: {},
	}

	for i, req := range tests {
		resp, err := s.ListTopics(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}

		if resp.Names == nil {
			t.Errorf("[case %d] Expected a non-nil TopicResponse.Topics field", i)
		}

		topics := resp.Names

		if !stringsEqual(expected[i], topics) {
			t.Errorf("[case %d] Expected Topic list %s, got %s", i, expected[i], topics)
		}
	}
}

func TestTagTopic(t *testing.T) {
	s := testServer()

	tests := map[int]*pb.TopicRequest{
		0: {Name: "test1", Tag: []string{"k:v"}},
		1: {Tag: []string{"k:v"}},
		2: {Name: "test1", Tag: []string{}},
		3: {Name: "test1"},
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
