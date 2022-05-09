// This file is entirely for tests, but isn't defined as a _test file due to use
// of the stubs in other packages.

package mapper

import (
	"errors"
)

var (
	errNotExist = errors.New("znode doesn't exist")
)

// Stub stubs the Handler interface.
type Stub struct {
	bmm  BrokerMetaMap
	data map[string]*StubZnode
}

// StubZnode stubs a ZooKeeper znode.
type StubZnode struct {
	value    []byte
	version  int32
	children map[string]*StubZnode
}

// NewZooKeeperStub returns a stub ZooKeeper.
func NewZooKeeperStub() *Stub {
	return &Stub{
		bmm: BrokerMetaMap{
			1001: &BrokerMeta{Rack: "a"},
			1002: &BrokerMeta{Rack: "b"},
			1003: &BrokerMeta{Rack: ""},
			1004: &BrokerMeta{Rack: "a"},
			1005: &BrokerMeta{Rack: "b"},
			1007: &BrokerMeta{Rack: ""},
		},
		data: map[string]*StubZnode{},
	}
}

// GetAllBrokerMeta stubs GetAllBrokerMeta.
func (zk *Stub) GetAllBrokerMeta(withMetrics bool) (BrokerMetaMap, []error) {
	b := zk.bmm.Copy()

	if withMetrics {
		m, _ := zk.GetBrokerMetrics()

		for bid := range b {
			b[bid].StorageFree = m[bid].StorageFree
		}
	}

	return b, nil
}

// GetBrokerMetrics stubs GetBrokerMetrics.
func (zk *Stub) GetBrokerMetrics() (BrokerMetricsMap, error) {
	bm := BrokerMetricsMap{
		1001: &BrokerMetrics{StorageFree: 2000.00},
		1002: &BrokerMetrics{StorageFree: 4000.00},
		1003: &BrokerMetrics{StorageFree: 6000.00},
		1004: &BrokerMetrics{StorageFree: 8000.00},
		1005: &BrokerMetrics{StorageFree: 10000.00},
		1007: &BrokerMetrics{StorageFree: 12000.00},
	}

	return bm, nil
}

// GetAllPartitionMeta stubs GetAllPartitionMeta.
func (zk *Stub) GetAllPartitionMeta() (PartitionMetaMap, error) {
	pm := NewPartitionMetaMap()
	pm["test_topic"] = map[int]*PartitionMeta{}

	pm["test_topic"][0] = &PartitionMeta{Size: 1000.00}
	pm["test_topic"][1] = &PartitionMeta{Size: 1500.00}
	pm["test_topic"][2] = &PartitionMeta{Size: 2000.00}
	pm["test_topic"][3] = &PartitionMeta{Size: 2500.00}
	pm["test_topic"][4] = &PartitionMeta{Size: 2200.00}
	pm["test_topic"][5] = &PartitionMeta{Size: 4000.00}

	return pm, nil
}

// GetPartitionMap stubs GetPartitionMap.
func (zk *Stub) GetPartitionMap(t string) (*PartitionMap, error) {
	p := &PartitionMap{
		Version: 1,
		Partitions: PartitionList{
			Partition{Topic: t, Partition: 0, Replicas: []int{1001, 1002}},
			Partition{Topic: t, Partition: 1, Replicas: []int{1002, 1001}},
			Partition{Topic: t, Partition: 2, Replicas: []int{1003, 1004, 1001}},
			Partition{Topic: t, Partition: 3, Replicas: []int{1004, 1003, 1002}},
		},
	}

	return p, nil
}

// GetTopicState stubs GetTopicState.
func (zk *Stub) GetTopicState(t string) (*TopicState, error) {
	_ = t

	ts := &TopicState{
		Partitions: map[string][]int{
			"0": {1000, 1001},
			"1": {1002, 1003},
			"2": {1004, 1005},
			"3": {1006, 1007},
			"4": {1008, 1009},
		},
	}

	return ts, nil
}
