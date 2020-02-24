package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/kafkametrics"
)

func TestIncompleteBrokerMetrics(t *testing.T) {
	bm := kafkametrics.BrokerMetrics{
		1001: &kafkametrics.Broker{
			ID:           1001,
			Host:         "host1",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1002: &kafkametrics.Broker{
			ID:           1002,
			Host:         "host2",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1003: &kafkametrics.Broker{
			ID:           1003,
			Host:         "host3",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
	}

	ids := []int{1001, 1002, 1003}

	if incompleteBrokerMetrics(ids, bm) {
		t.Errorf("Expected false return val")
	}

	ids = append(ids, 1004)

	if !incompleteBrokerMetrics(ids, bm) {
		t.Errorf("Expected true return val")
	}
}
