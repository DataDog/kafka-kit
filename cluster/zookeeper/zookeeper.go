package zookeeper

import (
	"github.com/go-zookeeper/zk"
)

type ZooKeeperLock struct {
	c *zk.Conn
}

func NewZooKeeperLock() (ZooKeeperLock, error) {
	return ZooKeeperLock{}, nil
}

func NewZooKeeperLockWithClient(c *zk.Conn) (ZooKeeperLock, error) {
	return ZooKeeperLock{c: c}, nil
}

func (z ZooKeeperLock) Lock() {
	return
}

func (z ZooKeeperLock) Unlock() {
	return
}
