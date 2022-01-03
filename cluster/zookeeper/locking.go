package zookeeper

import (
	"context"
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

// Lock attemps to acquire a lock. If the lock cannot be acquired by the context
// deadline, the lock attempt times out.
func (z *ZooKeeperLock) Lock(ctx context.Context) error {
	// Check if the context has a lock owner value. If so, check if this owner
	// already has the lock.
	if owner := ctx.Value(z.OwnerKey); owner != nil && owner == z.owner {
		return nil
	}

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

	var interval int
	for {
		// Prevent thrashing.
		interval++
		if interval%5 == 0 {
			time.Sleep(50 * time.Millisecond)
		}

		// Get all current locks.
		locks, err := z.locks()
		if err != nil {
			return ErrLockingFailed{message: err.Error()}
		}

		// Check if we have the first claim.
		firstClaim, _ := locks.First()
		if thisID == firstClaim {
			// We have the lock.
			z.lockZnode, err = locks.LockPath(thisID)

			// Set the owner value if the context OwnerKey is specified.
			if owner := ctx.Value(z.OwnerKey); owner != nil {
				z.owner = owner
			}

			return nil
		}

		// If we're here, we don't have the lock; we need to enqueue our wait position
		// by watching the ID immediately ahead of ours.
		lockAhead, err := locks.LockAhead(thisID)
		if err != nil {
			return ErrLockingFailed{message: err.Error()}
		}

		lockAheadPath, _ := locks.LockPath(lockAhead)

		// Get a ZooKeeper watch on the lock we're waiting on.
		_, _, blockingLockReleased, err := z.c.GetW(lockAheadPath)

		// Race the watch event against the context timeout.
		select {
		// We've timed out.
		case <-ctx.Done():
			// XXX it's critical that we clean up the attempted lock.
			z.deleteLockZnode(node)
			return ErrLockingTimedOut
		// Else see if we can get the claim.
		case <-blockingLockReleased:
			continue
		}
	}
}

// Unlock releases a lock.
func (z *ZooKeeperLock) Unlock(ctx context.Context) error {
	// Check if the context has a lock owner value.
	if owner := ctx.Value(z.OwnerKey); owner != nil && owner != z.owner {
		return ErrNotLockOwner
	}

	if err := z.deleteLockZnode(z.lockZnode); err != nil {
		return ErrUnlockingFailed{message: err.Error()}
	}

	z.lockZnode = ""
	z.owner = nil

	return nil
}

func (z *ZooKeeperLock) deleteLockZnode(p string) error {
	// We have to get the znode first; the current version is required for
	// the delete request.
	_, s, err := z.c.Get(p)
	if err != nil {
		return err
	}

	// Issue the delete.
	err = z.c.Delete(p, s.Version)
	if err != nil {
		return err
	}

	return nil
}
