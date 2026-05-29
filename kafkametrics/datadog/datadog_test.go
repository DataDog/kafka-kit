// Package datadog implements
// a kafkametrics Handler.
package datadog

import (
	"fmt"
	"testing"

	dd "github.com/zorkian/go-datadog-api"
)

// func TestPostEvent(t *testing.T)  {}
// func TestNewHandler(t *testing.T) {}
// func TestGetMetrics(t *testing.T) {}

func TestBrokersFromSeries(t *testing.T) {
	// Test with expected input.
	series := stubSeries()
	bs, err := brokersFromSeries(series, 0)

	if err != nil {
		t.Fatal(err)
	}

	if len(bs) != 5 {
		t.Errorf("Expected broker slice len 5, got %d\n", len(bs))
	}

	// Test with unexpected input.
	series = stubSeriesWithoutPoints()
	bs, err = brokersFromSeries(series, 0)
	if err == nil {
		t.Error("Expected error")
	}

	if len(bs) != 0 {
		t.Errorf("Expected broker slice len 0, got %d\n", len(bs))
	}
}

func stubSeries() []dd.Series {
	ss := []dd.Series{}
	var f1 = 0.00
	var f2 = 1073741824.00

	for i := 0; i < 5; i++ {
		scope := fmt.Sprintf("host%d,broker_id:100%d,instance-type:stub", i, i)
		s := dd.Series{
			Scope:  &scope,
			Points: []dd.DataPoint{{&f1, &f2}},
		}
		ss = append(ss, s)
	}

	return ss
}

func stubSeriesWithoutPoints() []dd.Series {
	ss := []dd.Series{}

	for i := 0; i < 5; i++ {
		scope := fmt.Sprintf("host%d,broker_id:100%d,instance-type:stub", i, i)
		s := dd.Series{
			Scope:  &scope,
			Points: []dd.DataPoint{},
		}
		ss = append(ss, s)
	}

	return ss
}
