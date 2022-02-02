// package zookeeper implements a ZooKeeper based Lock.
package zookeeper

import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

// ZooKeeperLock implements a Lock.
type ZooKeeperLock struct {
	c        ZooKeeperClient
	Path     string
	OwnerKey string
	TTL      int

	// The mutex can't be embedded because ZooKeeperLock also has Lock() / Unlock()
	// methods.
	mu sync.RWMutex
	// When a lock is successfully claimed, we store the full znode path.
	lockZnode string
	// The current owner name.
	owner interface{}
}

// ZooKeeperClient interface.
type ZooKeeperClient interface {
	Children(string) ([]string, *zk.Stat, error)
	Create(string, []byte, int32, []zk.ACL) (string, error)
	CreateProtectedEphemeralSequential(string, []byte, []zk.ACL) (string, error)
	Delete(string, int32) error
	Get(string) ([]byte, *zk.Stat, error)
	GetW(string) ([]byte, *zk.Stat, <-chan zk.Event, error)
}

// ZooKeeperLockConfig holds ZooKeeperLock configurations.
type ZooKeeperLockConfig struct {
	// The address of the ZooKeeper cluster.
	Address string
	// The locking path; this is the register that locks are attempting to acquire.
	Path string
	// A non-zero TTL sets a limit (in milliseconds) on how long a lock is possibly
	// valid for. Once this limit is exceeded, any new lock claims can destroy those
	// exceeding their TTL.
	TTL int
	// An optional lock ownership identifier. Context values can be inspected to
	// determine if a lock owner already has the lock. For example, if we specify
	// an OwnerKey configuration value of UserID, any successful lock claim will
	// set the lock owner as the value of UserID from the context received. Any
	// successive calls to Lock() with the same UserID context value will also
	// succeed. As a safety, this is not a distributed feature and is scoped to the
	// ZooKeeperLock instance; attempting to have two processes claim a lock
	// on the same path with the same OwnerKey/value will result in only one lock
	// being granted. This also prevents a concurrent program sharing a ZooKeeperLock
	// from allowing requestors to call Unlock on a lock that it doesn't own.
	OwnerKey string
}

// NewZooKeeperLock returns a ZooKeeperLock.
func NewZooKeeperLock(c ZooKeeperLockConfig) (*ZooKeeperLock, error) {
	var zkl = &ZooKeeperLock{
		OwnerKey: c.OwnerKey,
		TTL:      c.TTL,
	}

	var err error
	var nilLog = log.New(ioutil.Discard, "", 0)

	// Dial zk.
	zkl.c, _, err = zk.Connect([]string{c.Address}, 10*time.Second, zk.WithLogger(nilLog))
	if err != nil {
		return zkl, err
	}

	// Sanitize the path.
	zkl.Path = fmt.Sprintf("/%s", strings.Trim(c.Path, "/"))

	return zkl, zkl.init()
}

// NewZooKeeperLock takes a ZooKeeperLockConfig and ZooKeeperClient and returns
// a ZooKeeperLock. Any initialization (such as path creation) should be performed
// outside of this initializer.
func NewZooKeeperLockWithClient(cfg ZooKeeperLockConfig, zkc ZooKeeperClient) (*ZooKeeperLock, error) {
	return &ZooKeeperLock{
		c:    zkc,
		Path: fmt.Sprintf("/%s", strings.Trim(cfg.Path, "/")),
	}, nil
}

// Owner returns the current lock owner.
func (z *ZooKeeperLock) Owner() interface{} {
	z.mu.RLock()
	defer z.mu.RUnlock()
	return z.owner
}

// init performs any bootstrapping steps required for a ZooKeeperLock.
func (z *ZooKeeperLock) init() error {
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
