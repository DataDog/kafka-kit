package main

import (
	"fmt"
	"testing"

	"github.com/DataDog/topicmappr/kafkametrics"
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
			t.Errorf("Expected ID %d, got %d\n", srcExpected[n], src[n])
		}
	}

	for n := range dst {
		if dst[n] != dstExpected[n] {
			t.Errorf("Expected ID %d, got %d\n", dstExpected[n], dst[n])
		}
	}

	for n := range all {
		if all[n] != allExpected[n] {
			t.Errorf("Expected ID %d, got %d\n", allExpected[n], all[n])
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
// func TestMapsFromReassigments(t *testing.T) {}
// func TestRepCapacityByMetrics(t *testing.T) {}
// func TestApplyTopicThrottles(t *testing.T) {}
// func TestApplyBrokerThrottles(t *testing.T) {}
// func TestRemoveAllThrottles(t *testing.T) {}
// func TestMergeMaps(t *testing.T) {}
// func TestSliceToString(t *testing.T) {}
