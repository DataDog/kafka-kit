package zookeeper

import (
	"fmt"
	"strings"

	"github.com/go-zookeeper/zk"
)

type mockZooKeeperClient struct{}

func newMockZooKeeperLockWithClient(cfg ZooKeeperLockConfig, zkc ZooKeeperClient) *ZooKeeperLock {
	return &ZooKeeperLock{
		c:    mockZooKeeperClient{},
		Path: fmt.Sprintf("/%s", strings.Trim(cfg.Path, "/")),
	}
}

func (m mockZooKeeperClient) Children(s string) ([]string, *zk.Stat, error) {
	return nil, nil, nil
}

func (m mockZooKeeperClient) Create(s string, b []byte, i int32, a []zk.ACL) (string, error) {
	return "", nil
}

func (m mockZooKeeperClient) CreateProtectedEphemeralSequential(s string, b []byte, a []zk.ACL) (string, error) {
	return "", nil
}

func (m mockZooKeeperClient) Delete(s string, i int32) error {
	return nil
}

func (m mockZooKeeperClient) Get(s string) ([]byte, *zk.Stat, error) {
	return nil, nil, nil
}

func (m mockZooKeeperClient) GetW(s string) ([]byte, *zk.Stat, <-chan zk.Event, error) {
	return nil, nil, nil, nil
}
