package main

import (
	"testing"
)

func TestAddReplica(t *testing.T) {
	ttr := make(topicThrottledReplicas)

	err := ttr.addReplica("test", "0", "invalid", "1001")
	if err != errInvalidReplicaType {
		t.Errorf("Expected 'errInvalidReplicaType' error")
	}

	for _, typ := range []string{"leaders", "followers"} {
		err := ttr.addReplica("test", "0", replicaType(typ), "1001")
		if err != nil {
			t.Errorf("Unexpected error: %s", err)
		}
	}
}
