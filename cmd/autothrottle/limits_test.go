package main

import (
	"testing"

	"github.com/mrmuggymuggy/kafka-kit/kafkametrics"
)

func TestNewLimits(t *testing.T) {
	c := NewLimitsConfig{}
	c.Minimum = -1

	_, err := NewLimits(c)
	if err == nil {
		t.Error("Expected non-nil error")
	}

	c.Minimum = 10
	c.Maximum = 120

	_, err = NewLimits(c)
	if err == nil {
		t.Error("Expected non-nil error")
	}

	c.Maximum = 80

	_, err = NewLimits(c)
	if err != nil {
		t.Errorf("Unexpected error: %s\n", err.Error())
	}
}

func TestHeadroom(t *testing.T) {
	c := NewLimitsConfig{
		Minimum: 10,
		Maximum: 80,
		CapacityMap: map[string]float64{
			"mock": 100,
		},
	}

	l, _ := NewLimits(c)
	b := &kafkametrics.Broker{
		InstanceType: "mock",
	}

	// [current utilization, current throttle, expected headroom]
	expected := [][3]float64{
		[3]float64{70, 0, 24},
		[3]float64{80, 70, 72},
		[3]float64{110, 70, 40},
		[3]float64{200, 70, 10},
	}

	for n, params := range expected {
		b.NetTX = params[0]
		h, _ := l.headroom(b, params[1])
		if h != params[2] {
			t.Errorf("[test index %d] Expected headroom value of %f, got %f\n", n, params[2], h)
		}
	}
}
