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
			"/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
			"/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
		},
	}

	lock := newMockZooKeeperLockWithClient(c)

	locks, _ := lock.locks()
	// Test IDs.
	assert.ElementsMatch(t, locks.IDs(), []int{1, 2}, "Unexpected IDs list")
}

// func TestLockPath(t *testing.T) {
//   client := &mockZooKeeperClient{
//     znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
//     locks:             []string{
//       "/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
//       "/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002"
//     },
//   },
// 	lock := newMockZooKeeperLockWithClient(c)
// 	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
//
//   locks.
//
//   locks, _ := lock.locks()
//
//   expectedIDs := []int{1,2}
//   expectedLocks := map[string]string{
//     "1": "/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001",
//     "2": "/locks/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000002",
//   }
//
//   // Test IDs.
//   assert.ElementsMatch(t, locks.IDs(), expectedIDs, "Unexpected IDs list")
//
//   // Test ID to znode value.
//   for id, expectedZnode := expectedLocks {
//
//   }
// }
