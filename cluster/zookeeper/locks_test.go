package zookeeper

import (
	//"context"
	"testing"
	//"time"

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

func TestLockPath(t *testing.T) {
	c := &mockZooKeeperClient{
		znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
		locks: []string{
			"_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			"_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
		},
	}

	lock := newMockZooKeeperLockWithClient(c)
	locks, _ := lock.locks()

	expectedLocks := map[int]string{
		1: "/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
		2: "/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
	}

	// Test ID to znode value.
	for id, expectedZnode := range expectedLocks {
		znode, err := locks.LockPath(id)
		if err != nil {
			t.Errorf("Unexepected error: %s", err)
		}
		assert.Equal(t, znode, expectedZnode, "incorrect znode")
	}
}
