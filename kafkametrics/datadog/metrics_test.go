package datadog

import (
	"fmt"
	"testing"

	"github.com/DataDog/kafka-kit/v4/kafkametrics"
)

func TestMergeBrokerLists(t *testing.T) {
	var dst = []*kafkametrics.Broker{
		// This broker should exist in the src and be updated.
		{
			ID:           1000,
			Host:         "i-abc0",
			InstanceType: "type0",
			NetTX:        40.50,
			NetRX:        0.00,
		},
		// This broker only exists in dst and should go untouched.
		{
			ID:           1001,
			Host:         "i-abc1",
			InstanceType: "type0",
			NetTX:        60.00,
			NetRX:        40.00,
		},
	}

	var src = []*kafkametrics.Broker{
		{
			ID:           1000,
			Host:         "i-abc0",
			InstanceType: "type0",
			NetTX:        0.00,
			NetRX:        30.00,
		},
		// This broker doesn't exist in dst and should be added.
		{
			ID:           1002,
			Host:         "i-abc2",
			InstanceType: "type0",
			NetTX:        20.00,
			NetRX:        50.00,
		},
	}

	merged := mergeBrokerLists(dst, src)

	var expected = []*kafkametrics.Broker{
		{
			ID:           1000,
			Host:         "i-abc0",
			InstanceType: "type0",
			NetTX:        40.50,
			NetRX:        30.00,
		},
		{
			ID:           1001,
			Host:         "i-abc1",
			InstanceType: "type0",
			NetTX:        60.00,
			NetRX:        40.00,
		},
		{
			ID:           1002,
			Host:         "i-abc2",
			InstanceType: "type0",
			NetTX:        20.00,
			NetRX:        50.00,
		},
	}

	if len(merged) != len(expected) {
		t.Fail()
		t.Logf("Unexpected merged results len")
	}

	for i := range expected {
		if !brokerEqual(merged[i], expected[i]) {
			t.Errorf("Merged broker at index %d has unexpected values", i)
		}
	}
}

func brokerEqual(b0, b1 *kafkametrics.Broker) bool {
	switch {
	case b0.ID != b1.ID,
		b0.Host != b1.Host,
		b0.InstanceType != b1.InstanceType,
		b0.NetTX != b1.NetTX,
		b0.NetRX != b1.NetRX:
		return false
	default:
		return true
	}
}

// This is essentially tested via TestGetHostTagMap
// and TestPopulateFromTagMap.
// func TestBrokerMetricsFromList(t *testing.T) {}

// func TestGetHostTagMap(t *testing.T) {}

func TestPopulateFromTagMap(t *testing.T) {
	b := kafkametrics.BrokerMetrics{}

	// Test with complete input.
	tagMap := stubTagMap()
	err := populateFromTagMap(b, map[string][]string{}, tagMap, "broker_id", "instance-type")
	if err != nil {
		t.Errorf("Unexpected error: %s\n", err)
	}

	// Keep a broker reference
	// for the next test.
	var rndBroker *kafkametrics.Broker

	for id, broker := range b {
		rndBroker = broker
		if broker.ID != id {
			t.Errorf("Expected ID %d, got %d\n", id, broker.ID)
		}
		if broker.InstanceType != "stub" {
			t.Errorf("Expected broker InstanceType stub, got %s\n", broker.InstanceType)
		}
	}

	// Test with incomplete input.
	tagMap[rndBroker] = tagMap[rndBroker][1:]
	err = populateFromTagMap(b, map[string][]string{}, tagMap, "broker_id", "instance-type")
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func stubTagMap() map[*kafkametrics.Broker][]string {
	tm := map[*kafkametrics.Broker][]string{}

	for i := 0; i < 5; i++ {
		bid := 1000 + i
		b := &kafkametrics.Broker{
			ID:           bid,
			Host:         fmt.Sprintf("host%d", i),
			InstanceType: "stub",
			NetTX:        1073741824.00,
		}

		bidTag := fmt.Sprintf("broker_id:%d", bid)
		tm[b] = []string{bidTag, "instance-type:stub"}
	}

	return tm
}

// This tests both tagValFromScope and valFromTags.
func TestTagValFromScope(t *testing.T) {
	series := stubSeries()
	v := tagValFromScope(series[0].GetScope(), "instance-type")

	if v != "stub" {
		t.Errorf("Expected tag val stub, got %s\n", v)
	}
}
