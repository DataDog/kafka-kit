// go:build integration
//go:build integration
// +build integration

package zookeeper

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

const (
	TESTING_ZK_ADDR = "zookeeper:2181"
)

func TestLockIntegration(t *testing.T) {
	cfg := ZooKeeperLockConfig{
		Address: TESTING_ZK_ADDR,
		Path:    "/registry/locks",
	}

	lock, err := NewZooKeeperLock(cfg)
	assert.Nil(t, err)
	lock2, err := NewZooKeeperLock(cfg)
	assert.Nil(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)

	// This lock should succeed normally.
	err = lock.Lock(ctx)
	defer lock.Unlock(ctx)
	assert.Nil(t, err)

	// This lock should time out.
	err2 := lock2.Lock(ctx)
	defer lock2.Unlock(ctx)
	assert.Equal(t, err2, ErrLockingTimedOut)
}

func TestLockTTLIntegration(t *testing.T) {
	cfg := ZooKeeperLockConfig{
		Address: TESTING_ZK_ADDR,
		Path:    "/registry/locks",
		TTL:     1000,
	}

	lock, err := NewZooKeeperLock(cfg)
	assert.Nil(t, err)
	lock2, err := NewZooKeeperLock(cfg)
	assert.Nil(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

	// This lock should succeed normally.
	err = lock.Lock(ctx)
	defer lock.Unlock(ctx)
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)

	// This lock should succeed by purging the now stale lock.
	err2 := lock2.Lock(ctx)
	defer lock2.Unlock(ctx)
	assert.Equal(t, err2, nil)
}

func TestUnlockIntegration(t *testing.T) {
	cfg := ZooKeeperLockConfig{
		Address: TESTING_ZK_ADDR,
		Path:    "/registry/locks",
	}

	lock, err := NewZooKeeperLock(cfg)
	assert.Nil(t, err)
	lock2, err := NewZooKeeperLock(cfg)
	assert.Nil(t, err)

	ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)

	// This lock should succeed normally.
	err = lock.Lock(ctx)
	assert.Nil(t, err)

	// Release the first lock.
	err = lock.Unlock(ctx)
	assert.Nil(t, err)

	// This lock should succeed.
	err = lock2.Lock(ctx)
	assert.Nil(t, err)
	lock2.Unlock(ctx)
}
