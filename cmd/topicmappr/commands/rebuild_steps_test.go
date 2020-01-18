package commands

import (
	"testing"

	"github.com/DataDog/kafka-kit/kafkazk"
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

func TestPhasedReassignment(t *testing.T) {
	zk := kafkazk.Mock{}
	pm1, _ := zk.GetPartitionMap("test_topic")
	pm2 := pm1.Copy()

	phased := phasedReassignment(pm1, pm2)

	// These maps should be equal; phasedReassignment will
	// be a no-op since all of the pm2 leaders == the pm1 leaders.
	if eq, _ := pm2.Equal(phased); !eq {
		t.Errorf("Unexpected PartitionMap inequality")
	}

	// Strip the pm2 leaders.
	for i := range pm2.Partitions {
		pm2.Partitions[i].Replicas = pm2.Partitions[i].Replicas[1:]
	}

	// We should expect the pm1 leaders now.
	phased = phasedReassignment(pm1, pm2)

	// Check for a non no-op.
	if eq, _ := pm2.Equal(phased); eq {
		t.Errorf("Unexpected PartitionMap equality")
	}

	// Validate each partition leader.
	for i := range phased.Partitions {
		phasedOutputLeader := phased.Partitions[i].Replicas[0]
		originalLeader := pm1.Partitions[i].Replicas[0]
		if phasedOutputLeader != originalLeader {
			t.Errorf("Expected leader ID %d in phased output map, got %d", phasedOutputLeader, originalLeader)
		}
	}
}
