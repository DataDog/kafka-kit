package zookeeper

import (
	"context"
	"fmt"
	"sort"

	"github.com/go-zookeeper/zk"
)

// LockEntries is a container of locks.
type LockEntries struct {
	// Map of lock ID integer to the full znode path.
	m map[int]string
	// List of IDs ascending.
	l []int
}

// Lock claims a lock.
func (z ZooKeeperLock) Lock(ctx context.Context) error {
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
	return nil
}

// locks returns a LockEntries of all current locks.
func (z ZooKeeperLock) locks() (LockEntries, error) {
	var locks = LockEntries{
		m: map[int]string{},
		l: []int{},
	}

	// Get all nodes in the lock path.
	nodes, _, e := z.c.Children(z.Path)
	// Get the int IDs for all locks.
	for _, n := range nodes {
		id, err := idFromZnode(n)
		// Ignore junk entries.
		if err == ErrInvalidSeqNode {
			continue
		}
		// Append the znode to the map.
		locks.m[id] = n
		// Append the ID to the list.
		locks.l = append(locks.l, id)
	}

	sort.Ints(locks.l)

	return locks, e
}

// IDs returns all held lock IDs ascending.
func (le LockEntries) IDs() []int {
	return le.l
}

// First returns the ID with the lowest value.
func (le LockEntries) First() (int, error) {
	if len(le.IDs()) == 0 {
		return 0, fmt.Errorf("no active locks")
	}

	return le.IDs()[0], nil
}

func (le LockEntries) LockPath(id int) (string, error) {
	if path, exists := le.m[id]; exists {
		return path, nil
	}
	return "", fmt.Errorf("lock ID doesn't exist")
}

// LockAhead returns the lock ahead of the ID provided.
func (le LockEntries) LockAhead(id int) (int, error) {
	for i, next := range le.l {
		if next == id && i >= 0 {
			return le.l[i-1], nil
		}
	}

	return 0, fmt.Errorf("unable to determine which lock to enqueue behind")
}
