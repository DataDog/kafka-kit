package kafkazk

import (
	"testing"
)

func TestBrokerMetaCopy(t *testing.T) {
	orig := BrokerMeta{
		StorageFree:                 100.5,
		MetricsIncomplete:           false,
		ListenerSecurityProtocolMap: map[string]string{},
		Endpoints:                   []string{"localhost:9092"},
		Rack:                        "a",
		JMXPort:                     9999,
		Host:                        "localhost",
		Timestamp:                   "123456",
		Port:                        9092,
		Version:                     1,
	}

	copy := orig.Copy()

	var equal bool
	switch {
	// Simple fields.
	case
		orig.StorageFree != copy.StorageFree,
		orig.MetricsIncomplete != copy.MetricsIncomplete,
		orig.Rack != copy.Rack,
		orig.JMXPort != copy.JMXPort,
		orig.Host != copy.Host,
		orig.Timestamp != copy.Timestamp,
		orig.Port != copy.Port,
		orig.Version != copy.Version:
		equal = false
	default:
		equal = true
	}

	// Check the complex types. We could be exhaustive here and also check len
	// first, but a test failure should be understanable here.
	for k, v := range orig.ListenerSecurityProtocolMap {
		if copy.ListenerSecurityProtocolMap[k] != v {
			equal = false
		}
	}

	for i, e := range orig.Endpoints {
		if copy.Endpoints[i] != e {
			equal = false
		}
	}

	if !equal {
		t.Log("BrokerMeta invalid copy")
		t.Logf("Have:\n%+v\n", copy)
		t.Logf("Want:\n%+v\n", orig)
		t.Fail()
	}
}
