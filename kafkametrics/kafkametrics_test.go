package kafkametrics

import (
	"fmt"
	"testing"

	dd "github.com/zorkian/go-datadog-api"
)

// func TestPostEvent(t *testing.T)  {}
// func TestNewKafkaMetrics(t *testing.T) {}

func TestCreateNetTXQuery(t *testing.T) {
	s := createNetTXQuery("avg:system.net.bytes_sent{service:kafka} by {host}", 300)

	if s != "avg:system.net.bytes_sent{service:kafka} by {host}.rollup(avg, 300)" {
		t.Errorf("Expected avg:system.net.bytes_sent{service:kafka} by {host}.rollup(avg, 300), got %s\n", s)
	}
}

// func TestGetMetrics(t *testing.T) {}

func TestBrokersFromSeries(t *testing.T) {
	// Test with expected input.
	series := mockSeries()
	bs, err := brokersFromSeries(series)

	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	if len(bs) != 5 {
		t.Errorf("Expected broker slice len 5, got %d\n", len(bs))
	}

	// Test with unexpected input.
	series = mockSeriesWithoutPoints()
	bs, err = brokersFromSeries(series)
	if err == nil {
		t.Error("Expected error")
	}

	if len(bs) != 0 {
		t.Errorf("Expected broker slice len 0, got %d\n", len(bs))
	}
}

func mockSeries() []dd.Series {
	ss := []dd.Series{}

	for i := 0; i < 5; i++ {
		scope := fmt.Sprintf("host%d,broker_id:100%d,instance-type:mock", i, i)
		s := dd.Series{
			Scope:  &scope,
			Points: []dd.DataPoint{dd.DataPoint{0, 1073741824.00}},
		}
		ss = append(ss, s)
	}

	return ss
}

func mockSeriesWithoutPoints() []dd.Series {
	ss := []dd.Series{}

	for i := 0; i < 5; i++ {
		scope := fmt.Sprintf("host%d,broker_id:100%d,instance-type:mock", i, i)
		s := dd.Series{
			Scope:  &scope,
			Points: []dd.DataPoint{},
		}
		ss = append(ss, s)
	}

	return ss
}

// func TestBrokerMetricsFromList(t *testing.T) {}
// func TestGetHostTagMap(t *testing.T) {}

func TestPopulateFromTagMap(t *testing.T) {
	b := BrokerMetrics{}

	// Test with complete input.
	tagMap := mockTagMap()
	err := b.populateFromTagMap(tagMap)
	if err != nil {
		t.Errorf("Unexpected error: %s\n", err.Error())
	}

	// Keep a broker reference
	// for the next test.
	var rndBroker *Broker

	for id, broker := range b {
		rndBroker = broker
		if broker.ID != id {
			t.Errorf("Expected ID %d, got %d\n", id, broker.ID)
		}
		if broker.InstanceType != "mock" {
			t.Errorf("Expected broker InstanceType mock, got %s\n", broker.InstanceType)
		}
	}

	// Test with incomplete input.
	tagMap[rndBroker] = tagMap[rndBroker][1:]
	err = b.populateFromTagMap(tagMap)
	if err == nil {
		t.Errorf("Expected error, got nil")
	}
}

func mockTagMap() map[*Broker][]string {
	tm := map[*Broker][]string{}

	for i := 0; i < 5; i++ {
		bid := 1000 + i
		b := &Broker{
			ID:           bid,
			Host:         fmt.Sprintf("host%d", i),
			InstanceType: "mock",
			NetTX:        1073741824.00,
		}

		bidTag := fmt.Sprintf("broker_id:%d", bid)
		tm[b] = []string{bidTag, "instance-type:mock"}
	}

	return tm
}

// This tests both tagValFromScope and valFromTags.
func TestTagValFromScope(t *testing.T) {
	series := mockSeries()
	v := tagValFromScope(series[0].GetScope(), "instance-type")

	if v != "mock" {
		t.Errorf("Expected tag val mock, got %s\n", v)
	}
}
