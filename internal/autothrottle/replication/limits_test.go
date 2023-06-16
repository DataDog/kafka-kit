package replication

import (
	"testing"

	"github.com/DataDog/kafka-kit/v4/kafkametrics"
)

func TestNewLimits(t *testing.T) {
	c := NewLimitsConfig{}
	c.Minimum = -1 // Invalid.

	_, err := NewLimits(c)
	if err == nil {
		t.Error("Expected non-nil error")
	}

	c.Minimum = 10
	c.SourceMaximum = 120 // Invalid.

	_, err = NewLimits(c)
	if err == nil {
		t.Error("Expected non-nil error")
	}

	c.SourceMaximum = 80
	c.DestinationMaximum = 80

	_, err = NewLimits(c)
	if err != nil {
		t.Errorf("Unexpected error: %s\n", err.Error())
	}

	c.DestinationMaximum = 120 // Invalid.

	_, err = NewLimits(c)
	if err == nil {
		t.Error("Expected non-nil error")
	}
}

func TestReplicationHeadroom(t *testing.T) {
	c := NewLimitsConfig{
		Minimum:            10,
		SourceMaximum:      80,
		DestinationMaximum: 60,
		CapacityMap: map[string]float64{
			"stub": 100,
		},
	}

	l, _ := NewLimits(c)
	b := &kafkametrics.Broker{
		InstanceType: "stub",
	}

	// Test leader values.

	// [current utilization, current throttle, expected headroom]
	expected := [][3]float64{
		{70, 0, 24},
		{80, 70, 72},
		{110, 70, 40},
		{200, 70, 10},
	}

	for n, params := range expected {
		b.NetTX = params[0]
		h, _ := l.replicationHeadroom(b, "leader", params[1])
		if h != params[2] {
			t.Errorf("[test index %d] Expected headroom value of %f, got %f\n", n, params[2], h)
		}
	}

	// Test follower values.

	// [current utilization, current throttle, expected headroom]
	expected = [][3]float64{
		{70, 0, 18},
		{80, 70, 54},
		{110, 70, 30},
		{200, 70, 10},
	}

	for n, params := range expected {
		b.NetRX = params[0]
		h, _ := l.replicationHeadroom(b, "follower", params[1])
		if h != params[2] {
			t.Errorf("[test index %d] Expected headroom value of %f, got %f\n", n, params[2], h)
		}
	}
}
