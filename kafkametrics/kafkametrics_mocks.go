package kafkametrics

import (
	"fmt"
)

// Mock mocks tshe
// Handler interface.
type Mock struct{}

// GetMetrics mocks the GetMetrics function.
func (k *Mock) GetMetrics() (BrokerMetrics, []error) {
	bm := BrokerMetrics{}
	for i := 0; i < 10; i++ {
		bm[1000+i] = &Broker{
			ID:           1000 + i,
			Host:         fmt.Sprintf("host%d", i),
			InstanceType: "mock",
			NetTX:        100.00 + float64(i),
		}
	}

	return bm, nil
}

// PostEvent mocks the PostEvent function.
func (k *Mock) PostEvent(e *Event) error {
	_ = e
	return nil
}
