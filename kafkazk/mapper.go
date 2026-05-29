package kafkazk

import (
	"fmt"

	"github.com/DataDog/kafka-kit/v4/mapper"
)

// LoadMetrics takes a Handler and fetches stored broker metrics, populating the
// BrokerMetaMap.
func LoadMetrics(zk Handler, bm mapper.BrokerMetaMap) []error {
	metrics, err := zk.GetBrokerMetrics()
	if err != nil {
		return []error{err}
	}

	// Populate each broker with metric data.
	var errs []error
	for id := range bm {
		m, exists := metrics[id]
		if !exists {
			errs = append(errs, fmt.Errorf("Metrics not found for broker %d", id))
			bm[id].MetricsIncomplete = true
		} else {
			bm[id].StorageFree = m.StorageFree
		}
	}

	return errs
}
