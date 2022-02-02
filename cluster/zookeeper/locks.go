package zookeeper

import (
	"fmt"
	"sort"
)

// LockEntries is a container of locks.
type LockEntries struct {
	// Map of lock ID integer to the full znode path.
	idToZnode map[int]string
	// List of IDs ascending.
	idList []int
}

// locks returns a LockEntries of all current locks.
func (z *ZooKeeperLock) locks() (LockEntries, error) {
	z.mu.RLock()
	defer z.mu.RUnlock()

	var locks = LockEntries{
		idToZnode: map[int]string{},
		idList:    []int{},
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
		locks.idToZnode[id] = fmt.Sprintf("%s/%s", z.Path, n)
		// Append the ID to the list.
		locks.idList = append(locks.idList, id)
	}

	sort.Ints(locks.idList)

	return locks, e
}

// IDs returns all held lock IDs ascending.
func (le LockEntries) IDs() []int {
	return le.idList
}

// First returns the ID with the lowest value.
func (le LockEntries) First() (int, error) {
	if len(le.IDs()) == 0 {
		return 0, fmt.Errorf("no active locks")
	}

	return le.IDs()[0], nil
}

// LockPath takes a lock ID and returns the znode path.
func (le LockEntries) LockPath(id int) (string, error) {
	if path, exists := le.idToZnode[id]; exists {
		return path, nil
	}
	return "", fmt.Errorf("failed to get lock path; referenced ID doesn't exist")
}

// LockAhead returns the lock ahead of the ID provided.
func (le LockEntries) LockAhead(id int) (int, error) {
	for i, next := range le.idList {
		if next == id && i >= 0 {
			return le.idList[i-1], nil
		}
	}

	return 0, fmt.Errorf("unable to determine which lock to enqueue behind")
}
