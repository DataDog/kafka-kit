package main

import (
	"fmt"
	"sort"
	"testing"

	"github.com/DataDog/kafka-kit/kafkametrics"
	"github.com/DataDog/kafka-kit/kafkazk"
)

func TestmaxSrcNetTX(t *testing.T) {
	reassigning := mockgetReassigningBrokers()

	b := reassigning.maxSrcNetTX()
	if b.ID != 1004 {
		t.Errorf("Expected broker ID 1004, got %d", b.ID)
	}
}

func mockgetReassigningBrokers() getReassigningBrokers {
	r := getReassigningBrokers{
		Src: []*kafkametrics.Broker{},
		Dst: []*kafkametrics.Broker{},
	}

	for i := 0; i < 5; i++ {
		b := &kafkametrics.Broker{
			ID:           1000 + i,
			Host:         fmt.Sprintf("host%d", i),
			InstanceType: "mock",
			NetTX:        float64(80 + i),
		}

		r.Src = append(r.Src, b)
		r.Dst = append(r.Dst, b)
	}

	return r
}

func TestLists(t *testing.T) {
	b := mockreassigningBrokers()

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

func mockreassigningBrokers() reassigningBrokers {
	b := reassigningBrokers{
		src:       map[int]struct{}{},
		dst:       map[int]struct{}{},
		all:       map[int]struct{}{},
		throttled: map[string]map[string][]string{},
	}

	for i := 1000; i < 1010; i++ {
		if i < 1005 {
			b.src[i] = struct{}{}
		} else {
			b.dst[i] = struct{}{}
		}
		b.all[i] = struct{}{}
	}

	b.throttled["mock"] = map[string][]string{}

	b.throttled["mock"]["leaders"] = []string{
		"0:1000",
		"1:1001",
		"2:1002",
		"3:1003",
		"4:1004",
	}

	b.throttled["mock"]["followers"] = []string{
		"0:1005",
		"1:1006",
		"2:1007",
		"3:1008",
		"4:1009",
	}

	return b
}

// func TestUpdateReplicationThrottle(t *testing.T) {}

func TestgetReassigningBrokers(t *testing.T) {
	zk := &kafkazk.Mock{}

	re := zk.GetReassignments()
	bmaps, _ := getReassigningBrokers(re, zk)

	srcExpected := []int{1000, 1002}
	dstExpected := []int{1003, 1004, 1005, 1010}
	allExpected := []int{1000, 1002, 1003, 1004, 1005, 1010}

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
	expectedThrottledFollowers := []string{"0:1003", "0:1004", "1:1005", "1:1010"}

	throttledList := bmaps.throttled["mock"]["leaders"]
	sort.Strings(throttledList)
	for n, s := range throttledList {
		if s != expectedThrottledLeaders[n] {
			t.Errorf("Expected leader string '%s', got '%s'", expectedThrottledLeaders[n], s)
		}
	}

	throttledList = bmaps.throttled["mock"]["followers"]
	sort.Strings(throttledList)
	for n, s := range throttledList {
		if s != expectedThrottledFollowers[n] {
			t.Errorf("Expected follower string '%s', got '%s'", expectedThrottledFollowers[n], s)
		}
	}
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

func TestRepCapacityByMetrics(t *testing.T) {
	// Setup.
	c := NewLimitsConfig{
		Minimum: 20,
		Maximum: 90,
		CapacityMap: map[string]float64{
			"mock": 120.00,
		},
	}

	l, _ := NewLimits(c)

	rtc := &ReplicationThrottleConfigs{
		limits: l,
		throttles: map[int]float64{
			1004: 80.00,
		},
	}

	reassigning := mockreassigningBrokers()

	km := &kafkametrics.Mock{}
	bm, _ := km.GetMetrics()

	// Test normal scenario.
	cap, curr, _, _ := repCapacityByMetrics(rtc, reassigning, bm)
	if cap != 86.40 {
		t.Errorf("Expected capacity of 86.40, got %.2f", cap)
	}

	if curr != 80.00 {
		t.Errorf("Expected current capacity of 80.00, got %.2f", curr)
	}

	// Test with missing instance type.
	delete(rtc.limits, "mock")
	_, _, _, err := repCapacityByMetrics(rtc, reassigning, bm)
	if err.Error() != "Unknown instance type" {
		t.Errorf("Expected error 'Unknown instance type', got '%s'", err.Error())
	}

	// Test with a missing broker in the broker metrics.
	delete(bm, 1004)
	_, _, _, err = repCapacityByMetrics(rtc, reassigning, bm)
	if err.Error() != "Broker 1004 not found in broker metrics" {
		t.Errorf("Expected error 'Broker 1004 not found in broker metrics', got '%s'", err.Error())
	}
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
