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

// headroom takes a *kafkametrics.Broker and last set
// throttle rate and returns the headroom based on utilization
// vs capacity. A minimum value of 10MB/s is returned.
func (l Limits) headroom(b *kafkametrics.Broker, t float64) (float64, error) {
	if b == nil {
		return l["mininum"], errors.New("Nil broker provided")
	}

	if k, exists := l[b.InstanceType]; exists {
		nonThrottleUtil := math.Max(b.NetTX-t, 0.00)
		return math.Max((k-nonThrottleUtil)*0.90, l["mininum"]), nil
	}

	return l["mininum"], errors.New("Unknown instance type")
}
