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

func TestGetTopicsWithThrottledBrokers(t *testing.T) {
	rtf := &ReplicationThrottleConfigs{
		zk: &kafkazk.Stub{},
	}

	// Minimally populate the ReplicationThrottleConfigs.
	rtf.brokerOverrides = BrokerOverrides{
		1001: BrokerThrottleOverride{
			ID:                      1001,
			ReassignmentParticipant: false,
			Config: ThrottleOverrideConfig{
				Rate: 50,
			},
		},
		// Topics that include this broker shouldn't be included; the
		// BrokerThrottleOverride.Filter called in getTopicsWithThrottledBrokers
		// excludes any topics mapped to brokers where ReassignmentParticipant
		// == true.
		1002: BrokerThrottleOverride{
			ID:                      1002,
			ReassignmentParticipant: true,
			Config: ThrottleOverrideConfig{
				Rate: 50,
			},
		},
	}

	// Call.
	topicThrottledBrokers, _ := getTopicsWithThrottledBrokers(rtf)

	expected := topicThrottledReplicas{
		"test_topic":  throttled{"followers": brokerIDs{"0:1001"}},
		"test_topic2": throttled{"followers": brokerIDs{"0:1001"}},
	}

	if len(topicThrottledBrokers) != len(expected) {
		t.Fatalf("Expected len %d, got %d", len(expected), len(topicThrottledBrokers))
	}

	for topic := range expected {
		output, exist := topicThrottledBrokers[topic]
		if !exist {
			t.Fatalf("Expected topic '%s' in output", topic)
		}

		got := output["followers"][0]
		expectedOut := expected[topic]["followers"][0]
		if got != expectedOut {
			t.Errorf("Expected followers '%s', got '%s'", expectedOut, got)
		}
	}
}
