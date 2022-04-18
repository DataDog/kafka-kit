package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

func TestBrokerReplicationCapacities(t *testing.T) {
	zk := &kafkazk.Stub{}
	reassignments := zk.GetReassignments()
	reassigningBrokers, _ := getReassigningBrokers(reassignments, zk)

	lim, _ := NewLimits(NewLimitsConfig{
		Minimum:            20,
		SourceMaximum:      90,
		DestinationMaximum: 80,
		CapacityMap:        map[string]float64{"stub": 200.00},
	})

	rtc := &ThrottleManager{
		reassignments:          reassignments,
		previouslySetThrottles: replicationCapacityByBroker{1000: throttleByRole{float64ptr(20)}},
		limits:                 lim,
	}

	bm := stubBrokerMetrics()
	brc, _ := brokerReplicationCapacities(rtc, reassigningBrokers, bm)

	expected := map[int][2]*float64{
		1000: {float64ptr(126.00), nil},
		1002: {float64ptr(108.00), nil},
		1003: {nil, float64ptr(96.00)},
		1005: {nil, float64ptr(20.00)},
		1010: {nil, float64ptr(64.00)},
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

func TestStoreLeaderCapacity(t *testing.T) {
	capacities := replicationCapacityByBroker{}

	capacities.storeLeaderCapacity(1001, 100)
	out := capacities[1001]

	// Index 0 is the leader position.
	val := out[0]

	if val == nil {
		t.Fatal("Unexpected nil value")
	}

	if *val != 100 {
		t.Errorf("Expected value '100', got '%f'", *val)
	}

	// Index 1 is the follower position.
	val = out[1]

	if val != nil {
		t.Errorf("Expected nil value, got %v", *val)
	}
}

func TestStoreFollowerCapacity(t *testing.T) {
	capacities := replicationCapacityByBroker{}

	capacities.storeFollowerCapacity(1001, 100)
	out := capacities[1001]

	// Index 1 is the follower position.
	val := out[1]

	if val == nil {
		t.Fatal("Unexpected nil value")
	}

	if *val != 100 {
		t.Errorf("Expected value '100', got '%f'", *val)
	}

	// Index 0 is the leader position.
	val = out[0]

	if val != nil {
		t.Errorf("Expected nil value, got %v", *val)
	}
}

func TestReset(t *testing.T) {
	capacities := replicationCapacityByBroker{}
	capacities.setAllRatesWithDefault([]int{1001, 1002, 1003}, 100)

	// Check expected len.
	if len(capacities) != 3 {
		t.Errorf("Expected len 3, got %d", len(capacities))
	}

	// Reset, check len.
	capacities.reset()

	if len(capacities) != 0 {
		t.Errorf("Expected len 0, got %d", len(capacities))
	}
}
