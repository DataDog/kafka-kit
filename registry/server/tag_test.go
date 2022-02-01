package server

import (
	"sort"
	"testing"

	pb "github.com/DataDog/kafka-kit/v3/registry/registry"

	"github.com/stretchr/testify/assert"
)

func TestTagSetFromObject(t *testing.T) {
	topic := &pb.Topic{
		Name:        "test",
		Partitions:  32,
		Replication: 3,
	}

	th := testTagHandler()
	ts, _ := th.TagSetFromObject(topic)

	if len(ts) != 3 {
		t.Errorf("Expected TagSet len 3, got %d", len(ts))
	}

	expected := map[string]string{
		"name":        "test",
		"partitions":  "32",
		"replication": "3",
	}

	for k, v := range expected {
		if ts[k] != v {
			t.Errorf("Expected value %s for key %s, got %s", v, k, ts[k])
		}
	}
}

func TestTagSetKeys(t *testing.T) {
	type testCase struct {
		input    TagSet
		expected []string
	}

	tests := []testCase{
		// Single KV.
		{
			input:    TagSet{"myKey": "myValue"},
			expected: []string{"myKey"},
		},
		// Multiple KV.
		{
			input:    TagSet{"myKey": "myValue", "myKey2": "myValue2"},
			expected: []string{"myKey", "myKey2"},
		},
	}

	for _, test := range tests {
		results := test.input.Keys()
		sort.Strings(results)
		assert.Equal(t, test.expected, results)
	}
}

func TestTagsKeys(t *testing.T) {
	type testCase struct {
		input    Tags
		expected []string
	}

	tests := []testCase{
		// Single tag.
		{
			input:    Tags{"myKey:myValue"},
			expected: []string{"myKey"},
		},
		// Multiple tags, mixed kv and k.
		{
			input:    Tags{"myKey:myValue", "myKey2"},
			expected: []string{"myKey", "myKey2"},
		},
	}

	for _, test := range tests {
		results := test.input.Keys()
		sort.Strings(results)
		assert.Equal(t, test.expected, results)
	}
}

func TestMatchAll(t *testing.T) {
	ts := TagSet{
		"k1": "v1",
		"k2": "v2",
	}

	tSets := map[int]TagSet{
		1: {"k1": "v1"},
		2: {"k1": "v1", "k2": "v2"},
		3: {"k1": "v1", "k2": "v2", "unrelated": "v3"},
	}

	tests := map[int]bool{
		1: false,
		2: true,
		3: true,
	}

	for i, expected := range tests {
		ts2 := tSets[i]
		if ok := ts2.matchAll(ts); ok != expected {
			t.Errorf("Expected TagSet %v matchAll=%v with %v", ts2, expected, ts)
		}
	}
}

func TestEqual(t *testing.T) {
	tests := map[int][2]TagSet{
		0: {
			{},
			{},
		},
		1: {
			{"key": "value"},
			{"key": "value"},
		},
		2: {
			{"key": "value"},
			{"key": "value", "key2": "value2"},
		},
	}

	expected := map[int]bool{
		0: true,
		1: true,
		2: false,
	}

	for k, v := range tests {
		if v[0].Equal(v[1]) != expected[k] {
			t.Errorf("[test %d] expected TagSet equality '%v', got '%v'",
				k, expected[k], v[0].Equal(v[1]))
		}
	}
}

func TestTags(t *testing.T) {
	tagSet := TagSet{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}

	tags := tagSet.Tags()
	sort.Strings(tags)

	expected := Tags{"k1:v1", "k2:v2", "k3:v3"}

	if len(tags) != 3 {
		t.Errorf("Expected Tags len 3, got %d", len(tags))
	}

	for i := range tags {
		if tags[i] != expected[i] {
			t.Errorf("Got Tags element %s, expected %s", tags[i], expected[i])
		}
	}
}

func TestTagSet(t *testing.T) {
	tags := Tags{"k1:v1", "k2:v2", "k3:v3"}

	ts, err := tags.TagSet()
	if err != nil {
		t.Error("Unexpected error")
	}

	expected := TagSet{
		"k1": "v1",
		"k2": "v2",
		"k3": "v3",
	}

	if len(ts) != len(expected) {
		t.Error("Unexpected TagSet size")
	}

	for k, v := range expected {
		if ts[k] != v {
			t.Errorf("Expected value %s for key %s, got %s", v, k, ts[k])
		}
	}
}

func TestValid(t *testing.T) {
	tests := map[int]string{
		0: "broker",
		1: "topic",
		3: "invalid",
		4: "",
	}

	expected := map[int]bool{
		0: true,
		1: true,
		3: false,
		4: false,
	}

	for i, k := range tests {
		o := KafkaObject{Type: k, ID: "test"}
		if o.Valid() != expected[i] {
			t.Errorf("Expected Valid==%v for KafkaObject Type '%s'", expected[i], k)
		}
	}
}

func TestComplete(t *testing.T) {
	tests := map[int]string{
		0: "broker",
		1: "topic",
		3: "invalid",
		4: "",
	}

	expected := map[int]bool{
		0: true,
		1: true,
		3: false,
		4: false,
	}

	for i, k := range tests {
		o := KafkaObject{Type: k, ID: "test"}
		if o.Valid() != expected[i] {
			t.Errorf("Expected Valid==%v for KafkaObject Type '%s'", expected[i], k)
		}
	}

	// Test no ID.
	o := KafkaObject{Type: "broker"}
	if o.Complete() {
		t.Errorf("Complete should fail if the ID field is unspecified")
	}
}

func TestFilterTopics(t *testing.T) {
	th := testTagHandler()

	topics := TopicSet{
		"test_topic1": &pb.Topic{
			Name:        "test_topic1",
			Partitions:  32,
			Replication: 3,
		},
		"test_topic2": &pb.Topic{
			Name:        "test_topic2",
			Partitions:  32,
			Replication: 2,
		},
		"test_topic3": &pb.Topic{
			Name:        "test_topic3",
			Partitions:  16,
			Replication: 2,
		},
	}

	expected := map[int][]string{
		0: {"test_topic1", "test_topic2", "test_topic3"},
		1: {"test_topic1", "test_topic2"},
		2: {"test_topic2"},
	}

	tests := []Tags{
		{},
		{"partitions:32"},
		{"partitions:32", "replication:2"},
	}

	for i, tags := range tests {
		filtered, err := th.FilterTopics(topics, tags)
		if err != nil {
			t.Fatal(err)
		}

		if !stringsEqual(filtered.Names(), expected[i]) {
			t.Errorf("Expected %s, got %s", expected[i], filtered.Names())
		}
	}
}

func TestFilterBrokers(t *testing.T) {
	th := testTagHandler()

	brokers := BrokerSet{
		1001: &pb.Broker{
			Id:   1001,
			Rack: "rack1",
		},
		1002: &pb.Broker{
			Id:   1002,
			Rack: "rack2",
		},
		1003: &pb.Broker{
			Id:   1003,
			Rack: "rack1",
		},
	}

	expected := map[int][]uint32{
		0: {1001, 1002, 1003},
		1: {1001, 1003},
		2: {1003},
	}

	tests := []Tags{
		{},
		{"rack:rack1"},
		{"rack:rack1", "id:1003"},
	}

	for i, tags := range tests {
		filtered, err := th.FilterBrokers(brokers, tags)
		if err != nil {
			t.Fatal(err)
		}

		if !intsEqual(filtered.IDs(), expected[i]) {
			t.Errorf("Expected %v, got %v", expected[i], filtered.IDs())
		}
	}
}

func TestReservedFields(t *testing.T) {
	rs := GetReservedFields()

	topicExpected := map[string]struct{}{
		"tags":        {},
		"name":        {},
		"partitions":  {},
		"replication": {},
		"configs":     {},
		"replicas":    {},
	}

	brokerExpected := map[string]struct{}{
		"id":                          {},
		"rack":                        {},
		"jmxport":                     {},
		"timestamp":                   {},
		"tags":                        {},
		"listenersecurityprotocolmap": {},
		"endpoints":                   {},
		"host":                        {},
		"port":                        {},
		"version":                     {},
	}

	for i, expected := range []map[string]struct{}{topicExpected, brokerExpected} {
		var typeTest string

		switch i {
		case 0:
			typeTest = "topic"
		case 1:
			typeTest = "broker"
		}

		have := rs[typeTest]

		// Compare expected lengths.
		if len(expected) != len(have) {
			t.Errorf("Expected %d fields for %s, got %d",
				len(expected), typeTest, len(have))
		}

		// Compare fields.
		for f := range expected {
			if _, exist := have[f]; !exist {
				t.Errorf("Expected %s to have field %s", typeTest, f)
			}
		}
	}
}
