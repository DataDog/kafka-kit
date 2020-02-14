package main

import (
	"sort"

	"github.com/DataDog/kafka-kit/kafkametrics"
)

// reassigningBrokers holds several sets of brokers participating
// in all ongoing reassignments.
type reassigningBrokers struct {
	src               map[int]struct{}
	dst               map[int]struct{}
	all               map[int]struct{}
	throttledReplicas topicThrottledReplicas
}

// topicThrottledReplicas is a map of topic names to throttled types.
// This is ultimately populated as follows:
//   map[topic]map[leaders]["0:1001", "1:1002"]
//   map[topic]map[followers]["2:1003", "3:1004"]
type topicThrottledReplicas map[topic]throttled

// throttled is a replica type (leader, follower) to replica list.
type throttled map[replicaType]brokerIDs

// topic name.
type topic string

// leader, follower.
type replicaType string

// Replica broker IDs as a []string.
type brokerIDs []string

// lists returns a sorted []int of broker IDs for the src, dst
// and all brokers from a reassigningBrokers.
func (bm reassigningBrokers) lists() ([]int, []int, []int) {
	srcBrokers := []int{}
	dstBrokers := []int{}
	for n, m := range []map[int]struct{}{bm.src, bm.dst} {
		for b := range m {
			if n == 0 {
				srcBrokers = append(srcBrokers, b)
			} else {
				dstBrokers = append(dstBrokers, b)
			}
		}
	}

	allBrokers := []int{}
	for b := range bm.all {
		allBrokers = append(allBrokers, b)
	}

	// Sort.
	sort.Ints(srcBrokers)
	sort.Ints(dstBrokers)
	sort.Ints(allBrokers)

	return srcBrokers, dstBrokers, allBrokers
}

// incompleteBrokerMetrics takes a []int of all broker IDs involved in
// the current replication event and a kafkametrics.BrokerMetrics. If
// any brokers in the ID list are not found in the BrokerMetrics, our
// metrics are considered incomplete.
func incompleteBrokerMetrics(ids []int, metrics kafkametrics.BrokerMetrics) bool {
	for _, id := range ids {
		if _, exists := metrics[id]; !exists {
			return true
		}
	}

	return false
}
