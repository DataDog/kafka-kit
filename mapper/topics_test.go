package mapper

import (
	"testing"
)

func TestBrokers(t *testing.T) {
	zk := NewZooKeeperStub()

	state, _ := zk.GetTopicState("")
	// Inject duplicates to ensure the broker list is truly deduplicated.
	state.Partitions["5"] = []int{1000, 1001}

	expected := []int{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009}
	brokers := state.Brokers()

	if !intsEqual(brokers, expected) {
		t.Errorf("Expected brokers list %v, got %v", expected, brokers)
	}
}

func intsEqual(i1, i2 []int) bool {
	if len(i1) != len(i2) {
		return false
	}

	for i := range i1 {
		if i1[i] != i2[i] {
			return false
		}
	}

	return true
}
