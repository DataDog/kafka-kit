package main

import (
	"testing"

	"github.com/DataDog/kafka-kit/kafkametrics"
)

func TestIncompleteBrokerMetrics(t *testing.T) {
	bm := mockBrokerMetrics()

	ids := []int{1001, 1002, 1003}

	if incompleteBrokerMetrics(ids, bm) {
		t.Errorf("Expected false return val")
	}

	ids = append(ids, 1020)

	if !incompleteBrokerMetrics(ids, bm) {
		t.Errorf("Expected true return val")
	}
}

func mockBrokerMetrics() kafkametrics.BrokerMetrics {
	return kafkametrics.BrokerMetrics{
		1000: &kafkametrics.Broker{
			ID:           1000,
			Host:         "host0",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
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
		1004: &kafkametrics.Broker{
			ID:           1004,
			Host:         "host4",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1005: &kafkametrics.Broker{
			ID:           1005,
			Host:         "host5",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        180.00,
		},
		1006: &kafkametrics.Broker{
			ID:           1006,
			Host:         "host6",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1007: &kafkametrics.Broker{
			ID:           1007,
			Host:         "host7",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1008: &kafkametrics.Broker{
			ID:           1008,
			Host:         "host8",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1009: &kafkametrics.Broker{
			ID:           1009,
			Host:         "host9",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        80.00,
		},
		1010: &kafkametrics.Broker{
			ID:           1010,
			Host:         "host10",
			InstanceType: "mock",
			NetTX:        80.00,
			NetRX:        120.00,
		},
	}
}
