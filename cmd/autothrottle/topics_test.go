package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/v3/cmd/autothrottle/internal/throttlestore"
	"github.com/DataDog/kafka-kit/v3/kafkaadmin/stub"
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

func TestGetTopicsWithThrottledBrokers(t *testing.T) {
	rtf := &ThrottleManager{
		kafkaNativeMode: true,
		ka:              stub.Client{},
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
	topicThrottledBrokers, _ := rtf.getTopicsWithThrottledBrokers()

	expected := topicThrottledReplicas{
		"test1": throttled{"followers": brokerIDs{"0:1001"}},
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
