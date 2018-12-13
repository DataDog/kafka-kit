package server

import (
	"testing"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

func TestTagSetFromObject(t *testing.T) {
	topic := &pb.Topic{
		Name:        "test",
		Partitions:  32,
		Replication: 3,
	}

	ts := tagSetFromObject(topic)
	if len(ts) != 3 {
		t.Errorf("Expected tagSet len 3, got %d", len(ts))
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

func TestMatchAll(t *testing.T) {
	ts := tagSet{
		"k1": "v1",
		"k2": "v2",
	}

	tSets := map[int]tagSet{
		1: tagSet{"k1": "v1"},
		2: tagSet{"k1": "v1", "k2": "v2"},
		3: tagSet{"k1": "v1", "k2": "v2", "unrelated": "v3"},
	}

	tests := map[int]bool{
		1: false,
		2: true,
		3: true,
	}

	for i, expected := range tests {
		ts2 := tSets[i]
		if ok := ts2.matchAll(ts); ok != expected {
			t.Errorf("Expected tagSet %v matchAll=%v with %v", ts2, expected, ts)
		}
	}
}

// func TestTagSet(t *testing.T) {}
// func TestFilterTopics(t *testing.T) {}
// func TestFilterBrokers(t *testing.T) {}

func TestRestrictedFields(t *testing.T) {
	rs := restrictedFields()

	topicExpected := map[string]struct{}{
		"tags":        struct{}{},
		"name":        struct{}{},
		"partitions":  struct{}{},
		"replication": struct{}{},
	}

	brokerExpected := map[string]struct{}{
		"id":                          struct{}{},
		"rack":                        struct{}{},
		"jmxport":                     struct{}{},
		"timestamp":                   struct{}{},
		"tags":                        struct{}{},
		"listenersecurityprotocolmap": struct{}{},
		"endpoints":                   struct{}{},
		"host":                        struct{}{},
		"port":                        struct{}{},
		"version":                     struct{}{},
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
