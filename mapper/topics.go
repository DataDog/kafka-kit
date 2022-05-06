package mapper

import (
	"sort"
)

// TopicState is used for unmarshalling ZooKeeper json data from a topic:
// e.g. /brokers/topics/some-topic
type TopicState struct {
	Partitions map[string][]int `json:"partitions"`
}

// Brokers returns a []int of broker IDs from all partitions in the TopicState.
func (ts *TopicState) Brokers() []int {
	var IDs []int
	var seen = map[int]struct{}{}

	for _, replicas := range ts.Partitions {
		for _, id := range replicas {
			if _, exist := seen[id]; !exist {
				seen[id] = struct{}{}
				IDs = append(IDs, id)
			}
		}
	}

	sort.Ints(IDs)
	return IDs
}
