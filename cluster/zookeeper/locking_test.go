package zookeeper

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	lock := newMockZooKeeperLock()
	ctx, cf := context.WithTimeout(context.Background(), 1*time.Second)
	_ = cf

	// This lock should succeed normally.
	err := lock.Lock(ctx)
	assert.Nil(t, err)

	// This lock should time out.
	err2 := lock.Lock(ctx)
	assert.Equal(t, err2, ErrLockingTimedOut, "Expected ErrLockingTimedOut")
}

func TestLockSameOwner(t *testing.T) {
	lock := newMockZooKeeperLock()
	ctx, cf := context.WithTimeout(context.Background(), 1*time.Second)
	ctx = context.WithValue(ctx, "owner", "owner")
	_ = cf

	// This lock should succeed normally.
	err := lock.Lock(ctx)
	assert.Nil(t, err)

	// This should also succeed (with a soft error) because we have the same
	// instance, same owner key/value.
	err2 := lock.Lock(ctx)
	assert.Equal(t, err2, ErrAlreadyOwnLock)
}

func TestUnlock(t *testing.T) {
	lock := newMockZooKeeperLock()
	ctx, cf := context.WithTimeout(context.Background(), 1*time.Second)
	_ = cf

	// This lock should succeed normally.
	err := lock.Lock(ctx)
	assert.Nil(t, err)

	// Release the first lock.
	err = lock.Unlock(ctx)
	assert.Nil(t, err)

	// This lock should succeed.
	err = lock.Lock(ctx)
	assert.Nil(t, err)
}

func TestExpireLockAhead(t *testing.T) {
	lock := newMockZooKeeperLock()
	ctx, cf := context.WithTimeout(context.Background(), 60*time.Second)
	_ = cf
	ctx = context.WithValue(ctx, "owner", "test_owner")

	// This lock should succeed normally.
	err := lock.Lock(ctx)
	assert.Nil(t, err)

	// Enter a pending claim. This mimics the initial znode entry in the ZooKeeperLock
	// Lock method. We do this rather than calling the Lock method entirely
	// to exclude other operations that may affect what we really want to test.
	lockPath := fmt.Sprintf("%s/lock-", lock.Path)
	node, _ := lock.c.CreateProtectedEphemeralSequential(lockPath, nil, zk.WorldACL(31))
	id, _ := idFromZnode(node)

	// Check that the lock state has been populated.
	assert.Equal(t, lock.owner, "test_owner")
	assert.Equal(t, lock.lockZnode, "/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001")

	// Get the current lock entries.
	le, _ := lock.locks()

	// Ensure we exceed the mock ZooKeeperLock.TTL of 10ms.
	time.Sleep(30 * time.Millisecond)

	// This scenario should result in an expiry. We have an active lock ID 1
	// from the above Lock() call.
	expired, err := lock.expireLockAhead(le, id)
	assert.Nil(t, err)
	assert.True(t, expired)

	// Refresh the lock entries.
	le, _ = lock.locks()

	// This should now fail; the lock was expired and the only entry is ID 2
	// for the pending claim we entered above.
	expired, err = lock.expireLockAhead(le, id)
	assert.Equal(t, err, ErrExpireLockFailed{message: "unable to determine which lock to enqueue behind"})
	assert.False(t, expired)

	// Check that the lock state has been cleared.
	assert.Nil(t, lock.owner)
	assert.Equal(t, lock.lockZnode, "")
}
