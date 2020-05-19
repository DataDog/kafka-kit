package main

import (
	"fmt"
	"sort"
	"strconv"

	"github.com/DataDog/kafka-kit/kafkametrics"
	"github.com/DataDog/kafka-kit/kafkazk"
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

// getReassigningBrokers takes a kafakzk.Reassignments and returns a reassigningBrokers,
// which includes a broker list for source, destination, and all brokers
// handling any ongoing reassignments. Additionally, a map of throttled
// replicas by topic is included.
func getReassigningBrokers(r kafkazk.Reassignments, zk kafkazk.Handler) (reassigningBrokers, error) {
	lb := reassigningBrokers{
		// Maps of src and dst brokers used as sets.
		src: map[int]struct{}{},
		dst: map[int]struct{}{},
		all: map[int]struct{}{},
		// A map for each topic with a list throttled leaders and followers.
		// This is used to write the topic config throttled brokers lists.
		throttledReplicas: topicThrottledReplicas{},
	}

	// Get topic data for each topic undergoing a reassignment.
	for t := range r {
		topic := topic(t)
		lb.throttledReplicas[topic] = make(throttled)
		lb.throttledReplicas[topic]["leaders"] = []string{}
		lb.throttledReplicas[topic]["followers"] = []string{}
		tstate, err := zk.GetTopicStateISR(t)
		if err != nil {
			return lb, fmt.Errorf("Error fetching topic data: %s", err.Error())
		}

		// For each partition, compare the current ISR leader to the brokers being
		// assigned in the reassignments. The current leaders will be sources,
		// new brokers in the assignment list (but not in the current ISR state)
		// will be destinations.
		for p := range tstate {
			partn, _ := strconv.Atoi(p)
			if reassigning, exists := r[t][partn]; exists {
				// Source brokers.
				leader := tstate[p].Leader
				// In offline partitions, the leader value is set to -1. Skip.
				if leader != -1 {
					lb.src[leader] = struct{}{}
					// Append to the throttle list.
					leaders := lb.throttledReplicas[topic]["leaders"]
					lb.throttledReplicas[topic]["leaders"] = append(leaders, fmt.Sprintf("%d:%d", partn, leader))
				}

				// Dest brokers.
				for _, b := range reassigning {
					// Checks:  not -1 (offline/missing), not in the curent ISR state.
					// XXX(jamie): out of sync but previously existing brokers would
					// show here as well. May want to consider whether those should
					// be dynamically throttled as if they're part of a reassignemnt.
					if b != -1 && !inSlice(b, tstate[p].ISR) {
						lb.dst[b] = struct{}{}
						followers := lb.throttledReplicas[topic]["followers"]
						lb.throttledReplicas[topic]["followers"] = append(followers, fmt.Sprintf("%d:%d", partn, b))
					}
				}
			}
		}
	}

	lb.all = mergeMaps(lb.src, lb.dst)

	return lb, nil
}

// mergeMaps takes two maps and merges them.
func mergeMaps(a map[int]struct{}, b map[int]struct{}) map[int]struct{} {
	m := map[int]struct{}{}

	// Merge from each.
	for k := range a {
		m[k] = struct{}{}
	}

	for k := range b {
		m[k] = struct{}{}
	}

	return m
}

func roleFromIndex(i int) string {
	if i == 0 {
		return "leader"
	}

	return "follower"
}

func inSlice(id int, s []int) bool {
	found := false
	for _, i := range s {
		if id == i {
			found = true
		}
	}

	return found
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
