package zookeeper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIDs(t *testing.T) {
	c := &mockZooKeeperClient{
		znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
		locks: []fakeLock{
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			},
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
			},
		},
	}

	lock := newMockZooKeeperLockWithClient(c)
	locks, _ := lock.locks()

	// Test IDs.
	assert.ElementsMatch(t, locks.IDs(), []int{1, 2}, "Unexpected IDs list")
}

func TestFirst(t *testing.T) {
	c := &mockZooKeeperClient{
		znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
		locks: []fakeLock{
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			},
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
			},
		},
	}

	lock := newMockZooKeeperLockWithClient(c)

	locks, err := lock.locks()
	assert.Nil(t, err)

	first, err := locks.First()
	assert.Nil(t, err)
	assert.Equal(t, first, 1, "incorrect ID")
}

func TestLockPath(t *testing.T) {
	c := &mockZooKeeperClient{
		znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
		locks: []fakeLock{
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			},
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
			},
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
		assert.Nil(t, err)
		assert.Equal(t, znode, expectedZnode, "incorrect znode")
	}
}

func TestLockAhead(t *testing.T) {
	c := &mockZooKeeperClient{
		znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
		locks: []fakeLock{
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			},
			{
				path: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
			},
		},
	}

	lock := newMockZooKeeperLockWithClient(c)

	locks, err := lock.locks()
	assert.Nil(t, err)

	ahead, err := locks.LockAhead(2)
	assert.Nil(t, err)
	assert.Equal(t, ahead, 1, "incorrect ID")
}
