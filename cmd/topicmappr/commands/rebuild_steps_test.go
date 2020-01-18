package commands

import (
	"testing"
)

func TestNotInReplicaSet(t *testing.T) {
	rs := []int{1001, 1002, 1003}

	if notInReplicaSet(1001, rs) != false {
		t.Errorf("Expected ID 1001 in replica set")
	}

	if notInReplicaSet(1010, rs) != true {
		t.Errorf("Expected true assertion for ID 1010 in replica set")
	}
}
