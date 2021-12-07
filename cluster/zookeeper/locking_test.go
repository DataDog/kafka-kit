package zookeeper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLock(t *testing.T) {
	lock := newMockZooKeeperLock()
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)

	// This lock should succeed normally.
	err := lock.Lock(ctx)
	assert.Nil(t, err)

	// This lock should time out.
	err2 := lock.Lock(ctx)
	assert.Equal(t, err2, ErrLockingTimedOut, "Expected ErrLockingTimedOut")
}

func TestUnlock(t *testing.T) {
	lock := newMockZooKeeperLock()
	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)

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
