// This file is entirely for tests, but isn't defined as a _test file due to
// use of the stubs in other packages.

package kafkazk

import (
	"errors"
	"regexp"
	"strings"
	"time"
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
func NewZooKeeperStub() Handler {
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

// Many of these methods aren't complete stubs as they haven't been needed.

// GetReassignments stubs GetReassignments.
func (zk *Stub) GetReassignments() Reassignments {
	r := Reassignments{
		"reassigning_topic": map[int][]int{
			0: {1003, 1000, 1002},
			1: {1005, 1010},
		},
	}
	return r
}

func (zk *Stub) GetUnderReplicated() ([]string, error) {
	return []string{"underreplicated_topic"}, nil
}

func (zk *Stub) GetPendingDeletion() ([]string, error) {
	return []string{"deleting_topic"}, nil
}

// Create stubs Create.
func (zk *Stub) Create(p, d string) error {
	return zk.Set(p, d)
}

// CreateSequential stubs CreateSequential.
func (zk *Stub) CreateSequential(a, b string) error {
	_, _ = a, b
	return nil
}

// Exists stubs Exists.
func (zk *Stub) Exists(p string) (bool, error) {
	_, err := zk.Get(p)
	if err == errNotExist {
		return false, nil
	}

	return true, nil
}

// Set stubs Set.
func (zk *Stub) Set(p, d string) error {
	pathTrimmed := strings.Trim(p, "/")
	paths := strings.Split(pathTrimmed, "/")
	var current *StubZnode

	current = zk.data[paths[0]]
	if current == nil {
		current = &StubZnode{children: map[string]*StubZnode{}}
		zk.data[paths[0]] = current
	}

	var path string
	for _, path = range paths[1:] {
		next, exist := current.children[path]
		if !exist {
			next = &StubZnode{children: map[string]*StubZnode{}}
			current.children[path] = next
		}
		current = next
	}

	current.value = []byte(d)
	current.version++

	return nil
}

// Get stubs Get.
func (zk *Stub) Get(p string) ([]byte, error) {
	pathTrimmed := strings.Trim(p, "/")
	paths := strings.Split(pathTrimmed, "/")
	var current *StubZnode

	if current = zk.data[paths[0]]; current == nil {
		return nil, errNotExist
	}

	for _, path := range paths[1:] {
		next := current.children[path]
		if next == nil {
			return nil, errNotExist
		}
		current = next
	}

	return current.value, nil
}

// Delete stubs Delete.
func (zk *Stub) Delete(p string) error {
	pathTrimmed := strings.Trim(p, "/")
	paths := strings.Split(pathTrimmed, "/")
	var current *StubZnode

	if current = zk.data[paths[0]]; current == nil {
		return errNotExist
	}

	for i, path := range paths[1:] {
		next := current.children[path]
		if next == nil {
			return errNotExist
		}

		if i == len(paths)-2 {
			delete(current.children, path)
			return nil
		}

		current = next
	}

	return nil
}

// Children stubs children.
func (zk *Stub) Children(p string) ([]string, error) {
	pathTrimmed := strings.Trim(p, "/")
	paths := strings.Split(pathTrimmed, "/")
	children := []string{}
	var current *StubZnode

	if current = zk.data[paths[0]]; current == nil {
		return nil, errNotExist
	}

	for i, path := range paths[1:] {
		next := current.children[path]
		if next == nil {
			return nil, errNotExist
		}

		if i == len(paths)-2 {
			for k := range next.children {
				children = append(children, k)
			}

			return children, nil
		}

		current = next
	}

	return nil, errNotExist
}

func (zk *Stub) NextInt(p string) (int32, error) {
	pathTrimmed := strings.Trim(p, "/")
	paths := strings.Split(pathTrimmed, "/")
	var current *StubZnode

	if current = zk.data[paths[0]]; current == nil {
		return 0, errNotExist
	}

	for _, path := range paths[1:] {
		next := current.children[path]
		if next == nil {
			return 0, errNotExist
		}
		current = next
	}

	v := current.version
	current.version++

	return v, nil
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

// GetTopicStateISR stubs GetTopicStateISR.
func (zk *Stub) GetTopicStateISR(t string) (TopicStateISR, error) {
	_ = t

	return TopicStateISR{
		"0": PartitionState{Leader: 1000, ISR: []int{1000, 1002}},
		"1": PartitionState{Leader: 1002, ISR: []int{1002, 1003}},
		"2": PartitionState{Leader: 1004, ISR: []int{1004, 1005}},
		"3": PartitionState{Leader: 1006, ISR: []int{1006, 1007}},
		"4": PartitionState{Leader: 1008, ISR: []int{1008, 1009}},
	}, nil
}

// Close stubs Close.
func (zk *Stub) Close() {
	return
}

// Ready stubs Ready.
func (zk *Stub) Ready() bool {
	return true
}

// InitRawClient stubs InitRawClient.
func (zk *Stub) InitRawClient() error {
	return nil
}

// UpdateKafkaConfig stubs UpdateKafkaConfig.
func (zk *Stub) UpdateKafkaConfig(c KafkaConfig) ([]bool, error) {
	_ = c
	return []bool{}, nil
}

// GetTopics stubs GetTopics.
func (zk *Stub) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	t := []string{"test_topic", "test_topic2"}

	match := map[string]bool{}
	// Get all topics that match all
	// provided topic regexps.
	for _, topicRe := range ts {
		for _, topic := range t {
			if topicRe.MatchString(topic) {
				match[topic] = true
			}
		}
	}

	// Add matches to a slice.
	matched := []string{}
	for topic := range match {
		matched = append(matched, topic)
	}

	return matched, nil
}

// GetTopicConfig stubs GetTopicConfig.
func (zk *Stub) GetTopicConfig(t string) (*TopicConfig, error) {
	return &TopicConfig{
		Version: 1,
		Config: map[string]string{
			"retention.ms":                            "172800000",
			"leader.replication.throttled.replicas":   "0:1001,0:1002",
			"follower.replication.throttled.replicas": "0:1003,0:1004",
		},
	}, nil
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

// MaxMetaAge stubs MaxMetaAge.
func (zk *Stub) MaxMetaAge() (time.Duration, error) {
	return time.Since(time.Now()), nil
}
