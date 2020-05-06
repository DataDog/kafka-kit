package main

import (
	"bytes"
	"fmt"
	"log"

	"github.com/DataDog/kafka-kit/kafkametrics"
	"github.com/DataDog/kafka-kit/kafkazk"
)

// ReplicationThrottleConfigs holds all the data needed to call
// updateReplicationThrottle.
type ReplicationThrottleConfigs struct {
	topics                 []string // TODO(jamie): probably don't even need this anymore.
	reassignments          kafkazk.Reassignments
	zk                     kafkazk.Handler
	km                     kafkametrics.Handler
	overrideRate           int
	brokerOverrides        BrokerOverrides
	events                 *DDEventWriter
	previouslySetThrottles replicationCapacityByBroker
	limits                 Limits
	failureThreshold       int
	failures               int
	skipTopicUpdates       bool
}

// ThrottleOverrideConfig holds throttle override configurations.
type ThrottleOverrideConfig struct {
	// Rate in MB.
	Rate int `json:"rate"`
	// Whether the override rate should be
	// removed when the current reassignments finish.
	AutoRemove bool `json:"autoremove"`
}

// BrokerOverrides is a map of broker ID to BrokerThrottleOverride.
type BrokerOverrides map[int]BrokerThrottleOverride

// BrokerThrottleOverride holds broker-specific overrides.
type BrokerThrottleOverride struct {
	// Broker ID.
	ID int
	// Whether this override has been applied.
	Applied bool
	// The ThrottleOverrideConfig.
	Config ThrottleOverrideConfig
}

// IDs returns a []int of broker IDs held by the BrokerOverrides.
func (b BrokerOverrides) IDs() []int {
	var ids []int
	for id := range b {
		ids = append(ids, id)
	}

	return ids
}

// Failure increments the failures count and returns true if the
// count exceeds the failures threshold.
func (r *ReplicationThrottleConfigs) Failure() bool {
	r.failures++

	if r.failures > r.failureThreshold {
		return true
	}

	return false
}

// ResetFailures resets the failures count.
func (r *ReplicationThrottleConfigs) ResetFailures() {
	r.failures = 0
}

// DisableTopicUpdates prevents topic throttled replica lists from being
// updated in ZooKeeper.
func (r *ReplicationThrottleConfigs) DisableTopicUpdates() {
	r.skipTopicUpdates = true
}

// DisableTopicUpdates allow topic throttled replica lists from being
// updated in ZooKeeper.
func (r *ReplicationThrottleConfigs) EnableTopicUpdates() {
	r.skipTopicUpdates = false
}

// ThrottledBrokers is a list of brokers with a throttle applied
// for an ongoing reassignment.
type ThrottledBrokers struct {
	Src []*kafkametrics.Broker
	Dst []*kafkametrics.Broker
}

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

// updateReplicationThrottle takes a ReplicationThrottleConfigs that holds
// topics being replicated, any ZooKeeper/other clients, throttle override
// params, and other required metadata. Metrics for brokers participating in
// any ongoing replication are fetched to determine replication headroom.
// The replication throttle is then adjusted accordingly. If a non-empty
// override is provided, that static value is used instead of a dynamically
// determined value.
func updateReplicationThrottle(params *ReplicationThrottleConfigs) error {
	// Get the maps of brokers handling reassignments.
	reassigning, err := getReassigningBrokers(params.reassignments, params.zk)
	if err != nil {
		return err
	}

	// Creates lists from maps.
	srcBrokers, dstBrokers, allBrokers := reassigning.lists()

	log.Printf("Source brokers participating in replication: %v\n", srcBrokers)
	log.Printf("Destination brokers participating in replication: %v\n", dstBrokers)

	// Determine throttle rates.

	// Use the throttle override if set. Otherwise, make a calculation
	// using broker metrics and configured capacity values.
	var capacities = make(replicationCapacityByBroker)
	var brokerMetrics kafkametrics.BrokerMetrics
	var rateOverride bool
	var inFailureMode bool
	var metricErrs []error

	if params.overrideRate != 0 {
		log.Printf("A global throttle override is set: %dMB/s\n", params.overrideRate)
		rateOverride = true

		capacities.setAllRatesWithDefault(allBrokers, float64(params.overrideRate))
	}

	if !rateOverride {
		// Get broker metrics.
		brokerMetrics, metricErrs = params.km.GetMetrics()
		// Even if errors are returned, we can still proceed as long as we have
		// complete metrics data for all target brokers. If we have broker
		// metrics for all target brokers, we can ignore any errors.
		if metricErrs != nil {
			if brokerMetrics == nil || incompleteBrokerMetrics(allBrokers, brokerMetrics) {
				inFailureMode = true
			}
		}
	}

	// If we cannot proceed normally due to missing/partial metrics data,
	// check what failure iteration we're in. If we're above the threshold,
	// revert to the minimum rate, otherwise retain the previous rate.
	if inFailureMode {
		log.Printf("Errors fetching metrics: %s\n", metricErrs)

		// Increment and check our failure count against the configured threshold.
		over := params.Failure()

		// If we're not over the threshold, return and just retain previous throttles.
		if !over {
			log.Printf("Metrics fetch failure count %d doesn't exeed threshold %d, retaining previous throttle\n",
				params.failures, params.failureThreshold)
			return nil
		}

		// We're over the threshold; failback to the configured minimum.
		log.Printf("Metrics fetch failure count %d exceeds threshold %d, reverting to min-rate %.2fMB/s\n",
			params.failures, params.failureThreshold, params.limits["minimum"])

		// Set the failback rate.
		capacities.setAllRatesWithDefault(allBrokers, params.limits["minimum"])
	}

	// Reset the failure counter. We may have incremented in past iterations,
	// but if we're here now, we can reset the count.
	if !inFailureMode {
		params.ResetFailures()
	}

	// If there's no override set and we're not in a failure mode, apply
	// the calculated throttles.
	if !rateOverride && !inFailureMode {
		capacities, err = brokerReplicationCapacities(params, reassigning, brokerMetrics)
		if err != nil {
			return err
		}
	}

	// Merge in broker-specific overrides.
	overrideIDs := params.brokerOverrides.IDs()
	capacities.setAllRatesWithDefault(overrideIDs, 0)

	//Set broker throttle configs.
	events, errs := applyBrokerThrottles(reassigning.all, capacities, params.previouslySetThrottles, params.limits, params.zk)
	for _, e := range errs {
		log.Println(e)
	}

	//Set topic throttle configs.
	if !params.skipTopicUpdates {
		_, errs = applyTopicThrottles(reassigning.throttledReplicas, params.zk)
		for _, e := range errs {
			log.Println(e)
		}
	}

	// Append broker throttle info to event.
	var b bytes.Buffer
	if len(events) > 0 {
		b.WriteString("Replication throttles changes for brokers [ID, role, rate]: ")

		for e := range events {
			b.WriteString(fmt.Sprintf("[%d, %s, %.2f], ", e.id, e.role, e.rate))
		}

		b.WriteString("\n")
	}

	// Append topic stats to event.
	b.WriteString(fmt.Sprintf("Topics currently undergoing replication: %v", params.topics))

	// Ship it.
	params.events.Write("Broker replication throttle set", b.String())

	return nil
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
