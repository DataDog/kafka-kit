package zookeeper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIDs(t *testing.T) {
	c := &mockZooKeeperClient{
		znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
		locks: []string{
			"/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			"/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
		},
	}

	lock := newMockZooKeeperLockWithClient(c)
	locks, _ := lock.locks()

	// Test IDs.
	assert.ElementsMatch(t, locks.IDs(), []int{1, 2}, "Unexpected IDs list")
}
