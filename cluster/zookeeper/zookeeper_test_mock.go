package zookeeper

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/go-zookeeper/zk"
)

type mockZooKeeperClient struct {
	mu                sync.Mutex
	lockZnode         string
	owner             string
	ttl               int
	znodeNameTemplate string
	locks             []fakeLock
	nextID            int32
	path              string
}

type fakeLock struct {
	path string
	data []byte
}

func newMockZooKeeperLock() *ZooKeeperLock {
	return &ZooKeeperLock{
		c: &mockZooKeeperClient{
			znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
			locks:             []fakeLock{},
			nextID:            0,
			path:              "/locks",
		},
		OwnerKey: "owner",
		Path:     "/locks",
		TTL:      10,
	}
}

func newMockZooKeeperLockWithClient(c *mockZooKeeperClient) *ZooKeeperLock {
	return &ZooKeeperLock{
		c:        c,
		Path:     "/locks",
		OwnerKey: "owner",
	}
}

func (m *mockZooKeeperClient) Children(s string) ([]string, *zk.Stat, error) {
	var names []string
	for _, lock := range m.locks {
		names = append(names, strings.Trim(lock.path, m.path))
	}
	return names, nil, nil
}

func (m *mockZooKeeperClient) Create(s string, b []byte, i int32, a []zk.ACL) (string, error) {
	return "", nil
}

func (m *mockZooKeeperClient) CreateProtectedEphemeralSequential(s string, b []byte, a []zk.ACL) (string, error) {
	// Mimic the sequential znode naming scheme. If s == "/locks/lock-", we want
	// "/locks/_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-000000001"
	parts := strings.Split(s, "/")

	var l = fakeLock{
		path: fmt.Sprintf("/%s/%s%d", parts[1], m.znodeNameTemplate, atomic.AddInt32(&m.nextID, 1)),
		data: b,
	}

	// Store the fake lock name.
	m.locks = append(m.locks, l)

	return l.path, nil
}

func (m *mockZooKeeperClient) Delete(s string, i int32) error {
	var l []fakeLock
	for _, e := range m.locks {
		if e.path != s {
			l = append(l, e)
		}
	}
	m.locks = l

	return nil
}

func (m *mockZooKeeperClient) Get(s string) ([]byte, *zk.Stat, error) {
	var lock fakeLock
	for _, l := range m.locks {
		if l.path == s {
			lock = l
		}
	}
	return lock.data, &zk.Stat{Version: 1}, nil
}

func (m *mockZooKeeperClient) GetW(s string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return nil, nil, nil, nil
}
