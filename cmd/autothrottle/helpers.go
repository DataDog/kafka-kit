package main

import (
	"bytes"
	"sort"

	"github.com/mrmuggymuggy/kafka-kit/kafkametrics"
)

// bmapBundle holds several maps
// used as sets. Reduces return params
// for mapsFromReassigments.
type bmapBundle struct {
	src       map[int]struct{}
	dst       map[int]struct{}
	all       map[int]struct{}
	throttled map[string]map[string][]string
}

// lists returns a []int of broker IDs for the
// src, dst and all bmapBundle maps.
func (bm bmapBundle) lists() ([]int, []int, []int) {
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

// sliceToString takes []string and
// returns a comma delimited string.
func sliceToString(l []string) string {
	var b bytes.Buffer
	for n, i := range l {
		b.WriteString(i)
		if n < len(l)-1 {
			b.WriteString(",")
		}
	}

	return b.String()
}
