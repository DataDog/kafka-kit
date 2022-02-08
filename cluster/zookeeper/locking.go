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
	Timestamp   time.Time `json:"timestamp"`
	TTLDeadline time.Time `json:"ttl_deadline"`
	OwnerID     string    `json:"owner_id"`
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
		Timestamp:   time.Now(),
		TTLDeadline: time.Now().Add(time.Duration(z.TTL) * time.Millisecond),
		OwnerID:     fmt.Sprintf("%v", owner),
	}
	metaJSON, _ := json.Marshal(meta)

	// Enter the claim into ZooKeeper.
	lockPath := fmt.Sprintf("%s/lock-", z.Path)
	node, err := z.c.CreateProtectedEphemeralSequential(lockPath, metaJSON, zk.WorldACL(31))

	// In all return paths other than the case that we have successfully acquired
	// a lock, it's critical that we remove the claim znode.
	var removeZnodeAtExit bool = true
	defer func() {
		if removeZnodeAtExit {
			z.deleteLockZnode(node)
		}
	}()

	// Handle the error after the cleanup defer is registered. It's likely that
	// 'node' will always be an empty string if there's a non-nil error, anyway.
	if err != nil {
		return ErrLockingFailed{message: err.Error()}
	}

	// Get our claim ID.
	thisID, err := idFromZnode(node)
	if err != nil {
		return ErrLockingFailed{message: err.Error()}
	}

	var interval int
	var lockWaitingErr error
	for {
		interval++

		// Max failure threshold.
		if interval > 5 && lockWaitingErr != nil {
			return ErrLockingFailed{message: lockWaitingErr.Error()}
		}

		// Prevent thrashing.
		if interval > 1 {
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
			z.mu.Lock()

			// Update the lock znode.
			z.lockZnode, err = locks.LockPath(thisID)
			// Set the owner value if the context OwnerKey is specified.
			if owner := ctx.Value(z.OwnerKey); owner != nil {
				z.owner = owner
			}

			// XXX preventing this znode from being terminated is essential.
			removeZnodeAtExit = false

			z.mu.Unlock()

			return nil
		}

		// If we're here, we don't have the lock but can enqueue.

		// First, we'll check if the lock ahead has an expired TTL.
		expiredLock, err := z.expireLockAhead(locks, thisID)
		if err != nil {
			lockWaitingErr = err
			continue
		}

		// If so, restart the iteration to get a refreshed linked list.
		if expiredLock {
			continue
		}

		// Enqueue our wait position by watching the ID immediately ahead of ours.
		blockingLockReleased, err := z.getLockAheadWait(locks, thisID)
		if err != nil {
			lockWaitingErr = err
			continue
		}

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

// expireLockAhead takes an ID and checks if the lock ahead of it has an expired
// TTL. If so, it purges the lock and returns true.
func (z *ZooKeeperLock) expireLockAhead(locks LockEntries, id int) (bool, error) {
	// TTLs aren't being used.
	if z.TTL == 0 {
		return false, nil
	}

	// Get the path to the lock ahead.
	lockAheadPath, err := lockAheadPath(locks, id)
	if err != nil {
		return false, ErrExpireLockFailed{message: err.Error()}
	}

	// Get its metadata.
	dat, _, err := z.c.Get(lockAheadPath)
	if err != nil {
		return false, ErrExpireLockFailed{message: err.Error()}
	}

	// Deserialize.
	var metadata lockMetadata
	if err := json.Unmarshal(dat, &metadata); err != nil {
		return false, ErrExpireLockFailed{message: err.Error()}
	}

	// Check if it's expired.
	if time.Now().Before(metadata.TTLDeadline) {
		return false, nil
	}

	// We can purge the lock.
	if err := z.deleteLockZnode(lockAheadPath); err != nil {
		return false, ErrExpireLockFailed{message: err.Error()}
	}

	// Clear the lock state.
	z.mu.Lock()
	z.lockZnode = ""
	z.owner = nil
	z.mu.Unlock()

	return true, nil
}

// getLockAheadWait takes a lock ID and returns a watch on the lock immediately
// ahead of it.
func (z *ZooKeeperLock) getLockAheadWait(locks LockEntries, id int) (<-chan zk.Event, error) {
	lockAheadPath, err := lockAheadPath(locks, id)
	if err != nil {
		return nil, err
	}

	// Get a ZooKeeper watch on the lock path we're waiting on.
	_, _, watch, err := z.c.GetW(lockAheadPath)
	if err != nil {
		return nil, err
	}

	return watch, nil
}

func lockAheadPath(locks LockEntries, id int) (string, error) {
	// Find the lock ID ahead.
	lockAhead, err := locks.LockAhead(id)
	if err != nil {
		return "", err
	}

	// Get its path.
	return locks.LockPath(lockAhead)
}
