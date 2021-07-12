package kafkazk

// BrokerMetaMap is a map of broker IDs to BrokerMeta
// metadata fetched from ZooKeeper. Currently, just
// the rack field is retrieved.
type BrokerMetaMap map[int]*BrokerMeta

// BrokerMeta holds metadata that describes a broker,
// used in satisfying constraints.
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
