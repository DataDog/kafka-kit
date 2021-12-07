package zookeeper

import (
	"context"
	"fmt"

	"github.com/go-zookeeper/zk"
)

// Lock claims a lock.
func (z ZooKeeperLock) Lock(ctx context.Context) error {
	// Lock locally as a cheap way to avoid overhead in the distributed locking
	// backend.
	z.mu.Lock()
	defer z.mu.Unlock()

	// Enter the claim into ZooKeeper.
	lockPath := fmt.Sprintf("%s/lock-", z.Path)
	node, err := z.c.CreateProtectedEphemeralSequential(lockPath, nil, zk.WorldACL(31))
	if err != nil {
		return ErrLockingFailed{message: err.Error()}
	}

	// Get our claim ID.
	thisID, err := idFromZnode(node)
	if err != nil {
		return ErrLockingFailed{message: err.Error()}
	}

	for {
		// Get all current locks.
		locks, err := z.locks()
		if err != nil {
			return ErrLockingFailed{message: err.Error()}
		}

		// Check if we have the first claim.
		firstClaim, _ := locks.First()
		if thisID == firstClaim {
			// We have the lock.
			z.lockZnode, _ = locks.LockPath(thisID)
			return nil
		}

		// If we're here, we don't have the lock; we need to enqueue our wait position
		// by watching the ID immediately ahead of ours.
		lockAhead, err := locks.LockAhead(thisID)
		if err != nil {
			return ErrLockingFailed{message: err.Error()}
		}

		lockAheadPath, err := locks.LockPath(lockAhead)
		if err != nil {
			return ErrLockingFailed{message: err.Error()}
		}

		// Get a ZooKeeper watch on the lock we're waiting on.
		_, _, blockingLockReleased, err := z.c.GetW(lockAheadPath)

		// Race the watch event against the context timeout.
		select {
		// We've timed out.
		case <-ctx.Done():
			return ErrLockingTimedOut
			// Else see if we can get the claim.
		case <-blockingLockReleased:
			continue
		}
	}

	return nil
}

// Unlock releases a lock.
func (z ZooKeeperLock) Unlock(ctx context.Context) error {
	z.mu.Lock()
	defer z.mu.Unlock()

	// We have to get the znode first; the current version is required for
	// the delete request.
	_, s, err := z.c.Get(z.lockZnode)
	if err != nil {
		return ErrUnlockingFailed{message: err.Error()}
	}

	// Issue the delete.
	err = z.c.Delete(z.lockZnode, s.Version)
	if err != nil {
		return ErrUnlockingFailed{message: err.Error()}
	}

	z.lockZnode = ""

	return nil
}
