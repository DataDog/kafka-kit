package kafkazk

import (
	"regexp"
	"time"
)

// Mock mocks the Handler interface.
type Mock struct{}

// Many of these methods aren't complete
// mocks as they haven't been needed.

// GetReassignments mocks GetReassignments.
func (zk *Mock) GetReassignments() Reassignments {
	r := Reassignments{
		"mock": map[int][]int{
			0: []int{1003, 1004},
			1: []int{1005, 1010},
		},
	}
	return r
}

func (zk *Mock) GetPendingDeletion() ([]string, error) {
	return []string{"deleting_topic"}, nil
}

// Create mocks Create.
func (zk *Mock) Create(a, b string) error {
	_, _ = a, b
	return nil
}

// CreateSequential mocks CreateSequential.
func (zk *Mock) CreateSequential(a, b string) error {
	_, _ = a, b
	return nil
}

// Exists mocks Exists.
func (zk *Mock) Exists(a string) (bool, error) {
	_ = a
	return true, nil
}

// Set mocks Set.
func (zk *Mock) Set(a, b string) error {
	_, _ = a, b
	return nil
}

// Get mocks Get.
func (zk *Mock) Get(a string) ([]byte, error) {
	_ = a
	return []byte{}, nil
}

// Delete mocks Delete.
func (zk *Mock) Delete(a string) error {
	_ = a
	return nil
}

// Children mocks children.
func (zk *Mock) Children(a string) ([]string, error) {
	return nil, nil
}

// GetTopicState mocks GetTopicState.
func (zk *Mock) GetTopicState(t string) (*TopicState, error) {
	_ = t

	ts := &TopicState{
		Partitions: map[string][]int{
			"0": []int{1000, 1001},
			"1": []int{1002, 1003},
			"2": []int{1004, 1005},
			"3": []int{1006, 1007},
			"4": []int{1008, 1009},
		},
	}

	return ts, nil
}

// GetTopicStateISR mocks GetTopicStateISR.
func (zk *Mock) GetTopicStateISR(t string) (TopicStateISR, error) {
	_ = t

	ts := TopicStateISR{
		"0": PartitionState{Leader: 1000, ISR: []int{1000, 1002}},
		"1": PartitionState{Leader: 1002, ISR: []int{1002, 1003}},
		"2": PartitionState{Leader: 1004, ISR: []int{1004, 1005}},
		"3": PartitionState{Leader: 1006, ISR: []int{1006, 1007}},
		"4": PartitionState{Leader: 1008, ISR: []int{1008, 1009}},
	}

	return ts, nil
}

// Close mocks Close.
func (zk *Mock) Close() {
	return
}

// Ready mocks Ready.
func (zk *Mock) Ready() bool {
	return true
}

// InitRawClient mocks InitRawClient.
func (zk *Mock) InitRawClient() error {
	return nil
}

// UpdateKafkaConfig mocks UpdateKafkaConfig.
func (zk *Mock) UpdateKafkaConfig(c KafkaConfig) (bool, error) {
	_ = c
	return true, nil
}

// GetTopics mocks GetTopics.
func (zk *Mock) GetTopics(ts []*regexp.Regexp) ([]string, error) {
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

// GetTopicConfig mocks GetTopicConfig.
func (zk *Mock) GetTopicConfig(t string) (*TopicConfig, error) {
	return &TopicConfig{
		Version: 1,
		Config: map[string]string{
			"leader.replication.throttled.replicas":   "0:1001,0:1002",
			"follower.replication.throttled.replicas": "0:1003,0:1004",
		},
	}, nil
}

// GetAllBrokerMeta mocks GetAllBrokerMeta.
func (zk *Mock) GetAllBrokerMeta(withMetrics bool) (BrokerMetaMap, []error) {
	b := BrokerMetaMap{
		1001: &BrokerMeta{Rack: "a"},
		1002: &BrokerMeta{Rack: "b"},
		1003: &BrokerMeta{Rack: "c"},
		1004: &BrokerMeta{Rack: "a"},
		1005: &BrokerMeta{Rack: "b"},
	}

	if withMetrics {
		m, _ := zk.GetBrokerMetrics()

		for bid := range b {
			b[bid].StorageFree = m[bid].StorageFree
		}
	}

	return b, nil
}

// GetBrokerMetrics mocks GetBrokerMetrics.
func (zk *Mock) GetBrokerMetrics() (BrokerMetricsMap, error) {
	bm := BrokerMetricsMap{
		1001: &BrokerMetrics{StorageFree: 2000.00},
		1002: &BrokerMetrics{StorageFree: 4000.00},
		1003: &BrokerMetrics{StorageFree: 6000.00},
		1004: &BrokerMetrics{StorageFree: 8000.00},
		1005: &BrokerMetrics{StorageFree: 10000.00},
	}

	return bm, nil
}

// GetAllPartitionMeta mocks GetAllPartitionMeta.
func (zk *Mock) GetAllPartitionMeta() (PartitionMetaMap, error) {
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

// GetPartitionMap mocks GetPartitionMap.
func (zk *Mock) GetPartitionMap(t string) (*PartitionMap, error) {
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

// MaxMetaAge mocks MaxMetaAge.
func (zk *Mock) MaxMetaAge() (time.Duration, error) {
	return time.Since(time.Now()), nil
}
