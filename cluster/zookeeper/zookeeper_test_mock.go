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
	znodeNameTemplate string
	locks             []string
	nextID            int32
	path              string
}

func newMockZooKeeperLock() *ZooKeeperLock {
	return &ZooKeeperLock{
		c: &mockZooKeeperClient{
			znodeNameTemplate: "_c_979cb11f40bb3dbc6908edeaac8f2de1-lock-00000000",
			locks:             []string{},
			nextID:            0,
			path:              "/locks",
		},
		Path: "/locks",
	}
}

func newMockZooKeeperLockWithClient(c *mockZooKeeperClient) *ZooKeeperLock {
	return &ZooKeeperLock{
		c:    c,
		Path: "/locks",
	}
}

func (m *mockZooKeeperClient) Children(s string) ([]string, *zk.Stat, error) {
	var names []string
	for _, lock := range m.locks {
		names = append(names, strings.Trim(lock, m.path))
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
	fakeZnode := fmt.Sprintf("/%s/%s%d", parts[1], m.znodeNameTemplate, atomic.AddInt32(&m.nextID, 1))
	// Store the fake lock name.
	m.locks = append(m.locks, fakeZnode)

	return fakeZnode, nil
}

func (m *mockZooKeeperClient) Delete(s string, i int32) error {
	var l []string
	for _, e := range m.locks {
		if e != s {
			l = append(l, e)
		}
	}
	m.locks = l

	return nil
}

func (m *mockZooKeeperClient) Get(s string) ([]byte, *zk.Stat, error) {
	return nil, &zk.Stat{Version: 1}, nil
}

func (m *mockZooKeeperClient) GetW(s string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return nil, nil, nil, nil
}
