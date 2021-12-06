package zookeeper

import (
	"fmt"

	"github.com/go-zookeeper/zk"
)

// Lock claims a lock.
func (z ZooKeeperLock) Lock() error {
	lockPath := fmt.Sprintf("%s/lock-", z.Path)
	node, e := z.c.CreateProtectedEphemeralSequential(lockPath, nil, zk.WorldACL(31))

	thisID, err := idFromZnode(node)
	if err != nil {
		return err
	}

	fmt.Println(thisID)

	return e
}

// Unlock releases a lock.
func (z ZooKeeperLock) Unlock() error {
	return nil
}
