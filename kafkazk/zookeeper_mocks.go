package kafkazk

import (
	"regexp"
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
			1: []int{1005, 1006},
		},
	}
	return r
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

// Close mocks Close.
func (zk *Mock) Close() {
	return
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
func (zk *Mock) GetAllBrokerMeta(withMetrics bool) (BrokerMetaMap, error) {
	_ = withMetrics

	b := BrokerMetaMap{
		1001: &BrokerMeta{Rack: "a"},
		1002: &BrokerMeta{Rack: "b"},
		1003: &BrokerMeta{Rack: "c"},
		1004: &BrokerMeta{Rack: "a"},
		1005: &BrokerMeta{Rack: "b"},
	}

	return b, nil
}

func (zk *Mock) GetAllPartitionMeta() (PartitionMetaMap, error) {
	return nil, nil
}

// GetPartitionMap mocks GetPartitionMap.
func (zk *Mock) GetPartitionMap(t string) (*PartitionMap, error) {
	p := &PartitionMap{
		Version: 1,
		Partitions: partitionList{
			Partition{Topic: t, Partition: 0, Replicas: []int{1001, 1002}},
			Partition{Topic: t, Partition: 1, Replicas: []int{1002, 1001}},
			Partition{Topic: t, Partition: 2, Replicas: []int{1003, 1004, 1001}},
			Partition{Topic: t, Partition: 3, Replicas: []int{1004, 1003, 1002}},
		},
	}

	return p, nil
}
