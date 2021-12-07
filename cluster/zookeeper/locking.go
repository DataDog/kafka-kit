package zookeeper

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-zookeeper/zk"
)

// Lock claims a lock.
func (z ZooKeeperLock) Lock(ctx context.Context) error {
	z.mu.Lock()
	defer z.mu.Unlock()

	lockPath := fmt.Sprintf("%s/lock-", z.Path)
	node, err := z.c.CreateProtectedEphemeralSequential(lockPath, nil, zk.WorldACL(31))
	if err != nil {
		return err
	}

	// Get our claim ID.
	thisID, err := idFromZnode(node)
	if err != nil {
		return err
	}

	// Get all IDs.
	claimIDs, err := z.LockIDs()
	if err != nil {
		return err
	}

	// Check if we have the first claim.
	if thisID == claimIDs[0] {
		// We have the lock.
		z.lockID = thisID
		return nil
	}

	return ErrLockingFailed
}

// Unlock releases a lock.
func (z ZooKeeperLock) Unlock(ctx context.Context) error {
	return nil
}

// LockIDs returns all lock IDs in ascending order.
func (z ZooKeeperLock) LockIDs() ([]int, error) {
	var ids []int

	// Get all nodes in the lock path.
	nodes, _, e := z.c.Children(z.Path)
	// Get the int IDs for all locks.
	for _, n := range nodes {
		id, err := idFromZnode(n)
		// Ignore junk entriers.
		if err == ErrInvalidSeqNode {
			continue
		}
		ids = append(ids, id)
	}

	sort.Ints(ids)

	return ids, e
}
