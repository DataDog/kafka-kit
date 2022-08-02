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
	// Metadata from the Kafka cluster state.
	Host                       string
	Port                       int
	Rack                       string
	LogMessageFormat           string
	InterBrokerProtocolVersion string
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
		StorageFree:                bm.StorageFree,
		MetricsIncomplete:          bm.MetricsIncomplete,
		Host:                       bm.Host,
		Port:                       bm.Port,
		Rack:                       bm.Rack,
		LogMessageFormat:           bm.LogMessageFormat,
		InterBrokerProtocolVersion: bm.InterBrokerProtocolVersion,
	}

	return cp
}

// BrokerMetaMapFromStates takes a kafkaadmin.BrokerStates and translates it
// to a BrokerMetaMap.
func BrokerMetaMapFromStates(states kafkaadmin.BrokerStates) (BrokerMetaMap, error) {
	var bmm = BrokerMetaMap{}

	for id, state := range states {
		bmm[id] = &BrokerMeta{
			Host:                       state.Host,
			Port:                       state.Port,
			Rack:                       state.Rack,
			LogMessageFormat:           state.LogMessageFormat,
			InterBrokerProtocolVersion: state.InterBrokerProtocolVersion,
		}
	}

	return bmm, nil
}
