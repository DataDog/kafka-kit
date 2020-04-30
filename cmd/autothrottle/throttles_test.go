package main

import (
	"sort"
	"testing"

	"github.com/DataDog/kafka-kit/kafkazk"
)

func TestLists(t *testing.T) {
	b := mockReassigningBrokers()

	src, dst, all := b.lists()

	srcExpected := []int{1000, 1001, 1002, 1003, 1004}
	dstExpected := []int{1005, 1006, 1007, 1008, 1009}
	allExpected := []int{1000, 1001, 1002, 1003, 1004, 1005, 1006, 1007, 1008, 1009}

	for n := range src {
		if src[n] != srcExpected[n] {
			t.Errorf("Expected ID %d, got %d", srcExpected[n], src[n])
		}
	}

	for n := range dst {
		if dst[n] != dstExpected[n] {
			t.Errorf("Expected ID %d, got %d", dstExpected[n], dst[n])
		}
	}

	for n := range all {
		if all[n] != allExpected[n] {
			t.Errorf("Expected ID %d, got %d", allExpected[n], all[n])
		}
	}
}

func mockReassigningBrokers() reassigningBrokers {
	b := reassigningBrokers{
		src:               map[int]struct{}{},
		dst:               map[int]struct{}{},
		all:               map[int]struct{}{},
		throttledReplicas: topicThrottledReplicas{},
	}

	for i := 1000; i < 1010; i++ {
		if i < 1005 {
			b.src[i] = struct{}{}
		} else {
			b.dst[i] = struct{}{}
		}
		b.all[i] = struct{}{}
	}

	b.throttledReplicas["mock"] = throttled{}

	b.throttledReplicas["mock"]["leaders"] = brokerIDs{
		"0:1000",
		"1:1001",
		"2:1002",
		"3:1003",
		"4:1004",
	}

	b.throttledReplicas["mock"]["followers"] = brokerIDs{
		"0:1005",
		"1:1006",
		"2:1007",
		"3:1008",
		"4:1009",
	}

	return b
}

// func TestUpdateReplicationThrottle(t *testing.T) {}

func TestGetReassigningBrokers(t *testing.T) {
	zk := &kafkazk.Mock{}

	re := zk.GetReassignments()
	bmaps, _ := getReassigningBrokers(re, zk)

	srcExpected := []int{1000, 1002}
	dstExpected := []int{1003, 1005, 1010}
	allExpected := []int{1000, 1002, 1003, 1005, 1010}

	// Inclusion checks.

	for _, b := range srcExpected {
		if _, exists := bmaps.src[b]; !exists {
			t.Errorf("Expected ID %d not in map", b)
		}
	}

	for _, b := range dstExpected {
		if _, exists := bmaps.dst[b]; !exists {
			t.Errorf("Expected ID %d not in map", b)
		}
	}

	for _, b := range allExpected {
		if _, exists := bmaps.all[b]; !exists {
			t.Errorf("Expected ID %d not in map", b)
		}
	}

	// False inclusion checks.

	for b := range bmaps.src {
		if !inSlice(b, srcExpected) {
			t.Errorf("Unexpected src ID %d", b)
		}
	}

	for b := range bmaps.dst {
		if !inSlice(b, dstExpected) {
			t.Errorf("Unexpected dst ID %d", b)
		}
	}

	for b := range bmaps.all {
		if !inSlice(b, allExpected) {
			t.Errorf("Unexpected all ID %d", b)
		}
	}

	// Check throttled strings.

	expectedThrottledLeaders := []string{"0:1000", "1:1002"}
	expectedThrottledFollowers := []string{"0:1003", "1:1005", "1:1010"}

	throttledList := bmaps.throttledReplicas["mock"]["leaders"]
	sort.Strings(throttledList)
	for n, s := range throttledList {
		if s != expectedThrottledLeaders[n] {
			t.Errorf("Expected leader string '%s', got '%s'", expectedThrottledLeaders[n], s)
		}
	}

	throttledList = bmaps.throttledReplicas["mock"]["followers"]
	sort.Strings(throttledList)
	for n, s := range throttledList {
		if s != expectedThrottledFollowers[n] {
			t.Errorf("Expected follower string '%s', got '%s'", expectedThrottledFollowers[n], s)
		}
	}
}

func TestBrokerReplicationCapacities(t *testing.T) {
	zk := &kafkazk.Mock{}
	reassignments := zk.GetReassignments()
	reassigningBrokers, _ := getReassigningBrokers(reassignments, zk)

	lim, _ := NewLimits(NewLimitsConfig{
		Minimum:            20,
		SourceMaximum:      90,
		DestinationMaximum: 80,
		CapacityMap:        map[string]float64{"mock": 200.00},
	})

	rtc := &ReplicationThrottleConfigs{
		reassignments:          reassignments,
		previouslySetThrottles: replicationCapacityByBroker{1000: throttleByRole{float64ptr(20)}},
		limits:                 lim,
	}

	bm := mockBrokerMetrics()
	brc, _ := brokerReplicationCapacities(rtc, reassigningBrokers, bm)

	expected := map[int][2]*float64{
		1000: [2]*float64{float64ptr(126.00), nil},
		1002: [2]*float64{float64ptr(108.00), nil},
		1003: [2]*float64{nil, float64ptr(96.00)},
		1004: [2]*float64{nil, float64ptr(96.00)},
		1005: [2]*float64{nil, float64ptr(20.00)},
		1010: [2]*float64{nil, float64ptr(64.00)},
	}

	for id, got := range brc {
		for i := range got {
			role := roleFromIndex(i)

			if got[i] == nil {
				if expected[id][i] != nil {
					t.Errorf("Expected rate %.2f, got nil for ID %d role %s", *expected[id][i], id, role)
				}
				continue
			}

			if *got[i] != *expected[id][i] {
				t.Errorf("Expected rate %.2f, got %.2f for ID %d role %s",
					*expected[id][i], *got[i], id, role)
			}
		}
	}
}

func float64ptr(f float64) *float64 {
	return &f
}

// func TestApplyTopicThrottles(t *testing.T) {}
// func TestApplyBrokerThrottles(t *testing.T) {}
// func TestRemoveAllThrottles(t *testing.T) {}

// Covered in TestgetReassigningBrokers
// func TestMergeMaps(t *testing.T) {}

func TestSliceToString(t *testing.T) {
	in := []string{}
	out := sliceToString(in)

	if out != "" {
		t.Errorf("Expected empty string, got '%s'", out)
	}

	in = []string{"one", "two"}
	out = sliceToString(in)

	if out != "one,two" {
		t.Errorf("Expected string 'one,two', got '%s'", out)
	}
}
