package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/v3/cmd/autothrottle/internal/throttlestore"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
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
