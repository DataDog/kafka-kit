package replication

import (
	"sort"
	"testing"

	"github.com/DataDog/kafka-kit/v4/kafkametrics"
	"github.com/DataDog/kafka-kit/v4/kafkazk"
)

func TestGetReassigningBrokers(t *testing.T) {
	zk := &kafkazk.Stub{}

	re := zk.GetReassignments()
	bmaps, _ := GetReassigningBrokers(re, zk)

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

	throttledList := bmaps.throttledReplicas["stub"]["leaders"]
	sort.Strings(throttledList)
	for n, s := range throttledList {
		if s != expectedThrottledLeaders[n] {
			t.Errorf("Expected leader string '%s', got '%s'", expectedThrottledLeaders[n], s)
		}
	}

	throttledList = bmaps.throttledReplicas["stub"]["followers"]
	sort.Strings(throttledList)
	for n, s := range throttledList {
		if s != expectedThrottledFollowers[n] {
			t.Errorf("Expected follower string '%s', got '%s'", expectedThrottledFollowers[n], s)
		}
	}
}

func TestIncompleteBrokerMetrics(t *testing.T) {
	bm := stubBrokerMetrics()

	ids := []int{1001, 1002, 1003}

	if incompleteBrokerMetrics(ids, bm) {
		t.Errorf("Expected false return val")
	}

	ids = append(ids, 1020)

	if !incompleteBrokerMetrics(ids, bm) {
		t.Errorf("Expected true return val")
	}
}

func TestLists(t *testing.T) {
	b := stubReassigningBrokers()

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

func stubReassigningBrokers() reassigningBrokers {
	b := reassigningBrokers{
		src:               map[int]struct{}{},
		dst:               map[int]struct{}{},
		all:               map[int]struct{}{},
		throttledReplicas: TopicThrottledReplicas{},
	}

	for i := 1000; i < 1010; i++ {
		if i < 1005 {
			b.src[i] = struct{}{}
		} else {
			b.dst[i] = struct{}{}
		}
		b.all[i] = struct{}{}
	}

	b.throttledReplicas["stub"] = Throttled{}

	b.throttledReplicas["stub"]["leaders"] = BrokerIDs{
		"0:1000",
		"1:1001",
		"2:1002",
		"3:1003",
		"4:1004",
	}

	b.throttledReplicas["stub"]["followers"] = BrokerIDs{
		"0:1005",
		"1:1006",
		"2:1007",
		"3:1008",
		"4:1009",
	}

	return b
}

func stubBrokerMetrics() kafkametrics.BrokerMetrics {
	return kafkametrics.BrokerMetrics{
		1000: &kafkametrics.Broker{
			ID:           1000,
			Host:         "host0",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1001: &kafkametrics.Broker{
			ID:           1001,
			Host:         "host1",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1002: &kafkametrics.Broker{
			ID:           1002,
			Host:         "host2",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1003: &kafkametrics.Broker{
			ID:           1003,
			Host:         "host3",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1004: &kafkametrics.Broker{
			ID:           1004,
			Host:         "host4",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1005: &kafkametrics.Broker{
			ID:           1005,
			Host:         "host5",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        180.00,
		},
		1006: &kafkametrics.Broker{
			ID:           1006,
			Host:         "host6",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1007: &kafkametrics.Broker{
			ID:           1007,
			Host:         "host7",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1008: &kafkametrics.Broker{
			ID:           1008,
			Host:         "host8",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1009: &kafkametrics.Broker{
			ID:           1009,
			Host:         "host9",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1010: &kafkametrics.Broker{
			ID:           1010,
			Host:         "host10",
			InstanceType: "stub",
			NetTX:        80.00,
			NetRX:        120.00,
		},
	}
}
