package main

import (
	"errors"
	"math"

	"github.com/DataDog/topicmappr/kafkametrics"
)

var (
	// Hardcoded for now.
	BWLimits = Limits{
		// Min. config.
		"mininum": 10.00,
		// d2 class.
		"d2.xlarge":  100.00,
		"d2.2xlarge": 120.00,
		"d2.4xlarge": 240.00,
		// i3 class.
		"i3.xlarge":  130.00,
		"i3.2xlarge": 250.00,
		"i3.4xlarge": 500.00,
	}
)

// Limits is a map of instance-type
// to network bandwidth limits.
type Limits map[string]float64

// headroom takes an instance type and utilization
// and returns the headroom / free capacity. A minimum
// value of 10MB/s is returned.
func (l Limits) headroom(b *kafkametrics.Broker) (float64, error) {
	if b == nil {
		return l["mininum"], errors.New("Nil broker provided")
	}

	if k, exists := l[b.InstanceType]; exists {
		return math.Max(k-b.NetTX, l["mininum"]), nil
	}

	return l["mininum"], errors.New("Unknown instance type")
}
