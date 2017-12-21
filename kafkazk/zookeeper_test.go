package kafkazk

import (
	//"testing"
	"regexp"
)

// zkmock implements a mock zkhandler.
type zkmock struct{}

func (z *zkmock) GetReassignments() Reassignments {
	r := Reassignments{
		"test_topic": map[int][]int{
			2: []int{1003, 1004},
			3: []int{1004, 1003},
		},
	}
	return r
}

func (z *zkmock) GetTopics(ts []*regexp.Regexp) ([]string, error) {
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

func (z *zkmock) GetAllBrokerMeta() (BrokerMetaMap, error) {
	b := BrokerMetaMap{
		1001: &BrokerMeta{Rack: "a"},
		1002: &BrokerMeta{Rack: "b"},
		1003: &BrokerMeta{Rack: "c"},
		1004: &BrokerMeta{Rack: "a"},
	}

	return b, nil
}

func (z *zkmock) getPartitionMap(t string) (*PartitionMap, error) {
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

// func TestGetReassignments(t *testing.T) {}
// func TestGetTopics(t *testing.T) {}
// func TestGetAllBrokerMeta(t *testing.T) {}
// func TestGetPartitionMap(t *testing.T) {}
