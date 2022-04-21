package main

import (
	"testing"
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
