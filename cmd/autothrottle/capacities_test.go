package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/kafkazk"
)

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
