package mapper

import (
	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
)

// BrokerMetaMap is a map of broker IDs to BrokerMeta.
type BrokerMetaMap map[int]*BrokerMeta

// BrokerMeta holds metadata that describes a broker.
type BrokerMeta struct {
	StorageFree       float64 // In bytes.
	MetricsIncomplete bool
	// Metadata from ZooKeeper.
	ListenerSecurityProtocolMap map[string]string `json:"listener_security_protocol_map"`
	Endpoints                   []string          `json:"endpoints"`
	Rack                        string            `json:"rack"`
	JMXPort                     int               `json:"jmx_port"`
	Host                        string            `json:"host"`
	Timestamp                   string            `json:"timestamp"`
	Port                        int               `json:"port"`
	Version                     int               `json:"version"`
}

// Copy returns a copy of a BrokerMetaMap.
func (bmm BrokerMetaMap) Copy() BrokerMetaMap {
	var c = BrokerMetaMap{}

	for id, b := range bmm {
		cp := b.Copy()
		c[id] = &cp
	}

	return c
}

// Copy returns a copy of a BrokerMeta.
func (bm BrokerMeta) Copy() BrokerMeta {
	cp := BrokerMeta{
		StorageFree:                 bm.StorageFree,
		MetricsIncomplete:           bm.MetricsIncomplete,
		ListenerSecurityProtocolMap: map[string]string{},
		Rack:                        bm.Rack,
		JMXPort:                     bm.JMXPort,
		Host:                        bm.Host,
		Timestamp:                   bm.Timestamp,
		Port:                        bm.Port,
		Version:                     bm.Version,
	}

	for k, v := range bm.ListenerSecurityProtocolMap {
		cp.ListenerSecurityProtocolMap[k] = v
	}

	cp.Endpoints = append(cp.Endpoints, bm.Endpoints...)

	return cp
}

// BrokerMetaMapFromStates takes a kafkaadmin.BrokerStates and translates it
// to a BrokerMetaMap.
func BrokerMetaMapFromStates(states kafkaadmin.BrokerStates) (BrokerMetaMap, error) {
	var bmm = BrokerMetaMap{}

	for id, state := range states {
		bmm[id] = &BrokerMeta{
			// XXX(jamie): dropped the fields that don't exist in
			// kafkaadmin.BrokerStates; we don't even use these.
			ListenerSecurityProtocolMap: map[string]string{},
			Endpoints:                   []string{},
			Rack:                        state.Rack,
			// JMXPort: ,
			Host: state.Host,
			// Timestamp: ,
			Port: state.Port,
			// Version: ,
		}
	}

	return bmm, nil
}
