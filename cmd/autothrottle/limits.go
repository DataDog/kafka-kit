package main

import (
	"errors"
	"math"

	"github.com/DataDog/topicmappr/kafkametrics"
)

// Limits is a map of instance-type
// to network bandwidth limits.
type Limits map[string]float64

// NewLimits takes a minimum float64 and a map
// of instance-type to float64 network capacity values (in MB/s).
func NewLimits(m float64, l map[string]float64) Limits {
	lim := Limits{
		// Min. config.
		"mininum": m,
	}

	// Update with provided
	// capacity map.
	for k, v := range l {
		lim[k] = v
	}

	return lim
}

// headroom takes a *kafkametrics.Broker and last set
// throttle rate and returns the headroom based on utilization
// vs capacity. Headroom is determined by subtracting the current
// throttle rate from the current outbound network utilization.
// This yields a crude approximation of how much non-replication
// throughput is currently being demanded. The non-replication
// throughput is then subtracted from the total network capacity available.
// This value suggests what headroom is available for replication.
// The greater of this value*0.9 and 10MB/s is returned.
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
