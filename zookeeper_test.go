package main

import (
  //"testing"
  "regexp"
)
/*
// Update with partitions in reassignment.
// We might have this in /admin/reassign_partitions:
// {"version":1,"partitions":[{"topic":"myTopic","partition":14,"replicas":[1039,1044]}]}
// But retrieved this in /brokers/topics/myTopic:
// {"version":1,"partitions":{"14":[1039,1044,1041,1071]}}.
// The latter will be in ts if we're undergoing a partition move, so
// but we need to overwrite it with what's intended (the former).
if re[t] != nil {
  for p, replicas := range re[t] {
    pn := strconv.Itoa(p)
    ts.Partitions[pn] = replicas
  }
}
*/

// zkmock implements a mock zkhandler.
type zkmock struct{}

func (z *zkmock) getReassignments() reassignments {
	r := reassignments{
		"test_topic": map[int][]int{
			2: []int{1003, 1004},
			3: []int{1004, 1003},
		},
	}
	return r
}

func (z *zkmock) getTopics(ts []*regexp.Regexp) ([]string, error) {
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

func (z *zkmock) getAllBrokerMeta() (brokerMetaMap, error) {
	b := brokerMetaMap{
		1001: &BrokerMeta{Rack: "a"},
		1002: &BrokerMeta{Rack: "b"},
		1003: &BrokerMeta{Rack: "c"},
		1004: &BrokerMeta{Rack: "a"},
	}

	return b, nil
}

func (z *zkmock) getPartitionMap(t string) (*partitionMap, error) {
	p := &partitionMap{
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

func testGetBrokerMetaMap() *brokerMetaMap {
  b := &brokerMetaMap{
    1001: &BrokerMeta{Rack: "a"},
    1002: &BrokerMeta{Rack: "b"},
    1003: &BrokerMeta{Rack: "c"},
    1004: &BrokerMeta{Rack: "a"},
  }

  return b
}
