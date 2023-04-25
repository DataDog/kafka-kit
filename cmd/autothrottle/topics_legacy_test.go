package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/v4/internal/autothrottle/throttlestore"
	"github.com/DataDog/kafka-kit/v4/kafkazk"
)

func TestLegacyGetTopicsWithThrottledBrokers(t *testing.T) {
	rtf := &ThrottleManager{
		zk: &kafkazk.Stub{},
	}

	// Minimally populate the ThrottleManager.
	rtf.brokerOverrides = throttlestore.BrokerOverrides{
		1001: throttlestore.BrokerThrottleOverride{
			ID:                      1001,
			ReassignmentParticipant: false,
			Config: throttlestore.ThrottleOverrideConfig{
				Rate: 50,
			},
		},
		// Topics that include this broker shouldn't be included; the
		// BrokerThrottleOverride.Filter called in getTopicsWithThrottledBrokers
		// excludes any topics mapped to brokers where ReassignmentParticipant
		// == true.
		1002: throttlestore.BrokerThrottleOverride{
			ID:                      1002,
			ReassignmentParticipant: true,
			Config: throttlestore.ThrottleOverrideConfig{
				Rate: 50,
			},
		},
	}

	// Call.
	topicThrottledBrokers, _ := rtf.legacyGetTopicsWithThrottledBrokers()

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

/*
func TestFilter(t *testing.T) {
	zk := &kafkazk.Stub{}
	state, _ := zk.GetTopicState("test_topic")

	topicStates := make(TopicStates)
	topicStates["test_topic"] = *state

	matchID := 1000

	// Our filter func. returns any topic that includes matchID as a replica.
	fn := func(ts mapper.TopicState) bool {
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
*/
