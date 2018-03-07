package main

import (
	"fmt"
	"testing"

	"github.com/DataDog/topicmappr/kafkametrics"
	"github.com/DataDog/topicmappr/kafkazk"
)

func TestHighestSrcNetTX(t *testing.T) {
	reassigning := mockReassigningBrokers()

	b := reassigning.highestSrcNetTX()
	if b.ID != 1004 {
		t.Errorf("Expected broker ID 1004, got %d", b.ID)
	}
}

func mockReassigningBrokers() ReassigningBrokers {
	r := ReassigningBrokers{
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
	b := mockBmapBundle()

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

func mockBmapBundle() bmapBundle {
	b := bmapBundle{
		src:       map[int]interface{}{},
		dst:       map[int]interface{}{},
		all:       map[int]interface{}{},
		throttled: map[string]map[string][]string{},
	}

	for i := 1000; i < 1010; i++ {
		if i < 1005 {
			b.src[i] = nil
		} else {
			b.dst[i] = nil
		}
		b.all[i] = nil
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

func TestMapsFromReassigments(t *testing.T) {
	zk := &kafkazk.ZKMock{}

	re := zk.GetReassignments()
	bmaps, _ := mapsFromReassigments(re, zk)

	srcExpected := []int{1000, 1001, 1002, 1003}
	dstExpected := []int{1003, 1004, 1005, 1006}
	allExpected := []int{1000, 1001, 1002, 1003, 1004, 1005, 1006}

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

	expectedThrottledLeaders := []string{"0:1000", "0:1001", "1:1002", "1:1003"}
	expectedThrottledFollowers := []string{"0:1003", "0:1004", "1:1005", "1:1006"}

	for n, s := range bmaps.throttled["mock"]["leaders"] {
		if s != expectedThrottledLeaders[n] {
			t.Errorf("Expected leader string '%s', got '%s'", expectedThrottledLeaders[n], s)
		}
	}

	for n, s := range bmaps.throttled["mock"]["followers"] {
		if s != expectedThrottledFollowers[n] {
			t.Errorf("Expected follower string '%s', got '%s'", expectedThrottledFollowers[n], s)
		}
	}
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

	rtm := &ReplicationThrottleMeta{
		limits: l,
		throttles: map[int]float64{
			1004: 80.00,
		},
	}

	bmb := mockBmapBundle()

	km := &kafkametrics.KafkaMetricsMock{}
	bm, _ := km.GetMetrics()

	// Test normal scenario.
	cap, curr, _, _ := repCapacityByMetrics(rtm, bmb, bm)
	if cap != 86.40 {
		t.Errorf("Expected capacity of 86.40, got %.2f", cap)
	}

	if curr != 80.00 {
		t.Errorf("Expected current capacity of 80.00, got %.2f", curr)
	}
}

// func TestApplyTopicThrottles(t *testing.T) {}
// func TestApplyBrokerThrottles(t *testing.T) {}
// func TestRemoveAllThrottles(t *testing.T) {}

// Covered in TestMapsFromReassigments
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
