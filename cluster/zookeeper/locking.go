package zookeeper

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/go-zookeeper/zk"
)

// lockMetadata is internal metadata persisted in the lock znode.
type lockMetadata struct {
	Timestamp time.Time `json:"timestamp"`
	OwnerID   string    `json:"owner_id"`
}

// Lock attemps to acquire a lock. If the lock cannot be acquired by the context
// deadline, the lock attempt times out.
func (z *ZooKeeperLock) Lock(ctx context.Context) error {
	// Check if the context has a lock owner value. If so, check if this owner
	// already has the lock.
	owner := ctx.Value(z.OwnerKey)
	if owner != nil && owner == z.Owner() {
		return ErrAlreadyOwnLock
	}

	// Populate a lockMetadata.
	meta := lockMetadata{
		Timestamp: time.Now(),
		OwnerID:   fmt.Sprintf("%v", owner),
	}
	metaJSON, _ := json.Marshal(meta)

	// Enter the claim into ZooKeeper.
	lockPath := fmt.Sprintf("%s/lock-", z.Path)
	node, err := z.c.CreateProtectedEphemeralSequential(lockPath, metaJSON, zk.WorldACL(31))
	if err != nil {
		return ErrLockingFailed{message: err.Error()}
	}

	// Get our claim ID.
	thisID, err := idFromZnode(node)
	if err != nil {
		z.deleteLockZnode(node)
		return ErrLockingFailed{message: err.Error()}
	}

	var interval int
	var lockWaitingErr error
	for {
		interval++

		// Max failure threshold.
		if interval > 5 && lockWaitingErr != nil {
			z.deleteLockZnode(node)
			return ErrLockingFailed{message: lockWaitingErr.Error()}
		}

		// Prevent thrashing.
		if interval > 1 {
			time.Sleep(50 * time.Millisecond)
		}

		// Get all current locks.
		locks, err := z.locks()
		if err != nil {
			z.deleteLockZnode(node)
			return ErrLockingFailed{message: err.Error()}
		}

		// Check if we have the first claim.
		firstClaim, _ := locks.First()
		if thisID == firstClaim {
			// We have the lock.
			z.mu.Lock()
			// Update the lock znode.
			z.lockZnode, err = locks.LockPath(thisID)
			// Set the owner value if the context OwnerKey is specified.
			if owner := ctx.Value(z.OwnerKey); owner != nil {
				z.owner = owner
			}

			z.mu.Unlock()

			return nil
		}

		// If we're here, we don't have the lock; we need to enqueue our wait position
		// by watching the ID immediately ahead of ours.
		lockAhead, err := locks.LockAhead(thisID)
		if err != nil {
			lockWaitingErr = err
			continue
		}

		// XXX(jamie): determine what we should do here.
		lockAheadPath, err := locks.LockPath(lockAhead)
		if err != nil {
			lockWaitingErr = err
			continue
		}

		// Get a ZooKeeper watch on the lock we're waiting on.
		_, _, blockingLockReleased, err := z.c.GetW(lockAheadPath)
		if err != nil {
			lockWaitingErr = err
			continue
		}

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
	if owner := ctx.Value(z.OwnerKey); owner != nil && owner != z.Owner() {
		return ErrNotLockOwner
	}

	z.mu.Lock()
	defer z.mu.Unlock()

	var err error
	// Retry the znode delete on errors.
	for attempt := 0; attempt < 3; attempt++ {
		if err = z.deleteLockZnode(z.lockZnode); err == nil {
			break
		}
		time.Sleep(125 * time.Millisecond)
	}

	// We still have a non-nil error after the final attempt.
	if err != nil {
		return ErrUnlockingFailed{message: err.Error()}
	}

	z.lockZnode = ""
	z.owner = nil

	return nil
}

// Unlock releases a lock and logs, rather than returning, any errors if encountered.
func (z *ZooKeeperLock) UnlockLogError(ctx context.Context) {
	if err := z.Unlock(ctx); err != nil {
		log.Println(err)
	}
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
