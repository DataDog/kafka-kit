package kafkametrics

import (
	"fmt"
)

// Stub stubs tshe
// Handler interface.
type Stub struct{}

// GetMetrics stubs the GetMetrics function.
func (k *Stub) GetMetrics() (BrokerMetrics, []error) {
	bm := BrokerMetrics{}
	for i := 0; i < 10; i++ {
		bm[1000+i] = &Broker{
			ID:           1000 + i,
			Host:         fmt.Sprintf("host%d", i),
			InstanceType: "stub",
			NetTX:        100.00 + float64(i),
		}
	}

	return bm, nil
}

// PostEvent stubs the PostEvent function.
func (k *Stub) PostEvent(e *Event) error {
	_ = e
	return nil
}
