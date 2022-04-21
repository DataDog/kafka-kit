package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

func TestAddReplica(t *testing.T) {
	ttr := make(topicThrottledReplicas)

	// Try to add an invalid type.
	err := ttr.addReplica("test", "0", "invalid", "1001")
	if err != errInvalidReplicaType {
		t.Errorf("Expected 'errInvalidReplicaType' error")
	}

	types := []replicaType{"leaders", "followers"}

	// Add valid types; error unexpected.
	for _, typ := range types {
		err := ttr.addReplica("test", "0", replicaType(typ), "1001")
		if err != nil {
			t.Fatal(err)
		}
	}

	// For each type {leaders, followers}, ensure that we have one follower entry.
	for _, typ := range types {
		gotLen := len(ttr["test"][typ])
		if gotLen != 1 {
			t.Errorf("Expected len 1 for ttr[test][%s], got %d", typ, gotLen)
		}
	}

	// Spot check the content.
	if ttr["test"]["leaders"][0] != "0:1001" {
		t.Errorf("Expected output '0:1001', got '%s'", ttr["test"]["leaders"][0])
	}
}

func TestFilter(t *testing.T) {
	zk := &kafkazk.Stub{}
	state, _ := zk.GetTopicState("test_topic")

	topicStates := make(TopicStates)
	topicStates["test_topic"] = *state

	matchID := 1000

	// Our filter func. returns any topic that includes matchID as a replica.
	fn := func(ts kafkazk.TopicState) bool {
		// The stub partition state here is []int{1000,1001}.
		for _, id := range ts.Partitions["0"] {
			if id == matchID {
				return true
			}
		}
		return false
	}

	// We should get back one topic.
	filtered := topicStates.Filter(fn)
	_, match := filtered["test_topic"]
	if len(filtered) != 1 && !match {
		t.Errorf("Expected key 'test_topic'")
	}

	matchID = 9999

	// We should now have no matched topics.
	filtered = topicStates.Filter(fn)
	if len(filtered) != 0 {
		t.Errorf("Expected nil filtered result")
	}
}
