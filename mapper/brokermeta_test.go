package mapper

/*
import (
	"testing"
)

func TestBrokerMetaCopy(t *testing.T) {
	orig := BrokerMeta{
		StorageFree:                 100.5,
		MetricsIncomplete:           false,
		ListenerSecurityProtocolMap: map[string]string{"fake": "fake"},
		Endpoints:                   []string{"localhost:9092"},
		Rack:                        "a",
		JMXPort:                     9999,
		Host:                        "localhost",
		Timestamp:                   "123456",
		Port:                        9092,
		Version:                     1,
	}

	cp := orig.Copy()

	var equal bool
	switch {
	// Simple fields.
	case
		orig.StorageFree != cp.StorageFree,
		orig.MetricsIncomplete != cp.MetricsIncomplete,
		orig.Rack != cp.Rack,
		orig.JMXPort != cp.JMXPort,
		orig.Host != cp.Host,
		orig.Timestamp != cp.Timestamp,
		orig.Port != cp.Port,
		orig.Version != cp.Version:
		equal = false
	default:
		equal = true
	}

	// Check the complex types. We could be exhaustive here and also check len
	// first, but a test failure should be understandable here.
	for k, v := range orig.ListenerSecurityProtocolMap {
		if cp.ListenerSecurityProtocolMap[k] != v {
			equal = false
		}
	}

	for i, e := range orig.Endpoints {
		if cp.Endpoints[i] != e {
			equal = false
		}
	}

	if !equal {
		t.Log("BrokerMeta invalid copy")
		t.Logf("Have:\n%+v\n", cp)
		t.Logf("Want:\n%+v\n", orig)
		t.Fail()
	}

	// Test that the complex types are actually copies and not pointing to the
	// same memory.

	orig.ListenerSecurityProtocolMap["fake"] = "fake2"
	orig.Endpoints[0] = "127.0.0.1"

	switch {
	case
		cp.ListenerSecurityProtocolMap["fake"] != "fake",
		cp.Endpoints[0] != "localhost:9092":
		t.Errorf("The copy shares memory with the original")
	}
}
*/
