// package zookeeper implements a ZooKeeper based Lock.
package zookeeper

import (
	"fmt"
	"strconv"
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
	var zkl = ZooKeeperLock{}
	var err error

	// Dial zk.
	zkl.c, _, err = zk.Connect([]string{c.Address}, 10*time.Second, zk.WithLogInfo(false))
	if err != nil {
		return zkl, err
	}

	// Sanitize the path.
	zkl.Path = fmt.Sprintf("/%s", strings.Trim(c.Path, "/"))

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
		_, e := z.c.Create(nodePath, nil, 0, zk.WorldACL(31))
		// Ignore ErrNodeExists errors; we're ensuring a pre-defined path exists
		// at every init.
		if e != nil && e != zk.ErrNodeExists {
			return e
		}
	}

	return nil
}

// idFromZnode returns the ZooKeeper sequential ID from a full znode name.
func idFromZnode(s string) (int, error) {
	parts := strings.Split(s, "-")
	ida := parts[len(parts)-1]

	id, err := strconv.Atoi(ida)
	if err != nil {
		return 0, ErrInvalidSeqNode
	}

	return id, nil
}
