package kafkametrics

import (
	"fmt"
)

type KafkaMetricsMock struct{}

func (k *KafkaMetricsMock) GetMetrics() (BrokerMetrics, error) {
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

func (k *KafkaMetricsMock) PostEvent(e *Event) error {
	_ = e
	return nil
}
