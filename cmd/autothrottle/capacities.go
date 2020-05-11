package main

import (
	"fmt"

	"github.com/DataDog/kafka-kit/kafkametrics"
)

// replicationCapacityByBroker is a mapping of broker ID to capacity.
type replicationCapacityByBroker map[int]throttleByRole

// throttleByRole represents a source and destination throttle rate in respective
// order to index; position 0 is a source rate, position 1 is a dest. rate.
// A nil value means that no throttle was needed according to the broker's role
// in the replication, as opposed to 0.00 which explicitly describes the
// broker as having no spare capacity available for replication.
type throttleByRole [2]*float64

func (r replicationCapacityByBroker) storeLeaderCapacity(id int, c float64) {
	if _, exist := r[id]; !exist {
		r[id] = [2]*float64{}
	}

	a := r[id]
	a[0] = &c
	r[id] = a
}

func (r replicationCapacityByBroker) storeFollowerCapacity(id int, c float64) {
	if _, exist := r[id]; !exist {
		r[id] = [2]*float64{}
	}

	a := r[id]
	a[1] = &c
	r[id] = a
}

func (r replicationCapacityByBroker) storeLeaderAndFollerCapacity(id int, c float64) {
	r.storeLeaderCapacity(id, c)
	r.storeFollowerCapacity(id, c)
}

func (r replicationCapacityByBroker) setAllRatesWithDefault(ids []int, rate float64) {
	for _, id := range ids {
		r.storeLeaderCapacity(id, rate)
		r.storeFollowerCapacity(id, rate)
	}
}

// brokerReplicationCapacities traverses the list of all brokers participating
// in the reassignment. For each broker, it determines whether the broker is
// a leader (source) or a follower (destination), and calculates an throttle
// accordingly, returning a replicationCapacityByBroker and error.
func brokerReplicationCapacities(rtc *ReplicationThrottleConfigs, reassigning reassigningBrokers, bm kafkametrics.BrokerMetrics) (replicationCapacityByBroker, error) {
	capacities := replicationCapacityByBroker{}

	// For each broker, check whether the it's a source and/or destination,
	// calculating and storing the throttle for each.
	for ID := range reassigning.all {
		capacities[ID] = throttleByRole{}
		// Get the kafkametrics.Broker from the ID, check that
		// it exists in the kafkametrics.BrokerMetrics.
		broker, exists := bm[ID]
		if !exists {
			return capacities, fmt.Errorf("Broker %d not found in broker metrics", ID)
		}

		// We're traversing brokers from 'all', but a broker's role is either
		// a leader, a follower, or both. If it's exclusively one, we can
		// skip throttle computation for that role type for the broker.
		for i, role := range []replicaType{"leader", "follower"} {
			var isInRole bool
			switch role {
			case "leader":
				_, isInRole = reassigning.src[ID]
			case "follower":
				_, isInRole = reassigning.dst[ID]
			}

			if !isInRole {
				continue
			}

			var currThrottle float64
			// Check if a throttle rate was previously set.
			throttles, exists := rtc.previouslySetThrottles[ID]
			if exists && throttles[i] != nil {
				currThrottle = *throttles[i]
			} else {
				// If not, we assume that none of the current bandwidth is being
				// consumed from reassignment bandwidth.
				currThrottle = 0.00
			}

			// Calc. and store the rate.
			rate, err := rtc.limits.replicationHeadroom(broker, role, currThrottle)
			if err != nil {
				return capacities, err
			}

			switch role {
			case "leader":
				capacities.storeLeaderCapacity(ID, rate)
			case "follower":
				capacities.storeFollowerCapacity(ID, rate)
			}
		}
	}

	return capacities, nil
}
