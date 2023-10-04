package replication

import (
	"testing"

	"github.com/DataDog/kafka-kit/v4/internal/autothrottle/throttlestore"
	"github.com/DataDog/kafka-kit/v4/kafkaadmin/stub"
)

func TestAddReplica(t *testing.T) {
	ttr := make(TopicThrottledReplicas)

	// Try to add an invalid type.
	err := ttr.addReplica("test", "0", "invalid", "1001")
	if err != ErrInvalidReplicaType {
		t.Errorf("Expected 'ErrInvalidReplicaType' error")
	}

	types := []ReplicaType{"leaders", "followers"}

	// Add valid types; error unexpected.
	for _, typ := range types {
		err := ttr.addReplica("test", "0", ReplicaType(typ), "1001")
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

func TestGetTopicsWithThrottledBrokers(t *testing.T) {
	rtf := &ThrottleManager{
		kafkaNativeMode: true,
		ka:              stub.NewClient(),
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
		// BrokerThrottleOverride.Filter called in GetTopicsWithThrottledBrokers
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
	topicThrottledBrokers, _ := rtf.GetTopicsWithThrottledBrokers()

	expected := TopicThrottledReplicas{
		"test1": Throttled{"followers": BrokerIDs{"0:1001"}},
	}

	if len(topicThrottledBrokers) != len(expected) {
		t.Fatalf("Expected len %d, got %d, expected- %v and actual - %v", len(expected), len(topicThrottledBrokers), expected, topicThrottledBrokers)
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
