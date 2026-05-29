package mapper

import (
	"testing"
)

func TestBrokerMetaCopy(t *testing.T) {
	orig := BrokerMeta{
		StorageFree:                100.5,
		MetricsIncomplete:          false,
		Host:                       "localhost",
		Port:                       9092,
		Rack:                       "a",
		LogMessageFormat:           "0.10",
		InterBrokerProtocolVersion: "0.10",
	}

	cp := orig.Copy()

	var equal bool
	switch {
	// Simple fields.
	case
		orig.StorageFree != cp.StorageFree,
		orig.MetricsIncomplete != cp.MetricsIncomplete,
		orig.Host != cp.Host,
		orig.Rack != cp.Rack,
		orig.Port != cp.Port,
		orig.LogMessageFormat != cp.LogMessageFormat,
		orig.InterBrokerProtocolVersion != cp.InterBrokerProtocolVersion:
		equal = false
	default:
		equal = true
	}

	if !equal {
		t.Log("BrokerMeta invalid copy")
		t.Logf("Have:\n%+v\n", cp)
		t.Logf("Want:\n%+v\n", orig)
		t.Fail()
	}
}
