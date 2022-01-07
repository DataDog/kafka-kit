package zookeeper

import (
	"context"
	"testing"
	"time"

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
