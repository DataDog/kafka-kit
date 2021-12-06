// package zookeeper implements a ZooKeeper based Lock.
package zookeeper

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-zookeeper/zk"
)

// ZooKeeperLock implements a Lock.
type ZooKeeperLock struct {
	c    *zk.Conn
	Path string
}

// ZooKeeperLockConfig holds ZooKeeperLock configurations.
type ZooKeeperLockConfig struct {
	Address string
	Path    string
}

// NewZooKeeperLock returns a ZooKeeperLock.
func NewZooKeeperLock(c ZooKeeperLockConfig) (ZooKeeperLock, error) {
	var zkl = ZooKeeperLock{Path: c.Path}
	var err error

	zkl.c, _, err = zk.Connect([]string{c.Address}, 10*time.Second, zk.WithLogInfo(false))
	if err != nil {
		return zkl, err
	}

	return zkl, zkl.init()
}

// init performs any bootstrapping steps required for a ZooKeeperLock.
func (z ZooKeeperLock) init() error {
	// Get an incremental path ending at the destination locking path. If for
	// example we're provided "/path/to/locks", we want the following:
	// ["/path", "/path/to", "/path/to/locks"].
	nodes := strings.Split(strings.Trim(z.Path, "/"), "/")

	// Create each node.
	for i := range nodes {
		nodePath := fmt.Sprintf("/%s", strings.Join(nodes[:i+1], "/"))
		if _, e := z.c.Create(nodePath, nil, 0, zk.WorldACL(31)); e != nil {
			return e
		}
	}

	return nil
}

// Lock claims a lock.
func (z ZooKeeperLock) Lock() {
	return
}

// Unlock releases a lock.
func (z ZooKeeperLock) Unlock() {
	return
}
