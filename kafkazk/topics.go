package kafkazk

import (
	"sort"
)

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
