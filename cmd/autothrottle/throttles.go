package main

import (
	"github.com/DataDog/kafka-kit/v3/cmd/autothrottle/internal/throttlestore"
	"github.com/DataDog/kafka-kit/v3/kafkametrics"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

// ThrottleManager manages Kafka throttle rates.
type ThrottleManager struct {
	reassignments   kafkazk.Reassignments
	zk              kafkazk.Handler
	km              kafkametrics.Handler
	overrideRate    int
	kafkaNativeMode bool
	// The following three fields are for brokers with static overrides set
	// and a topicThrottledReplicas for topics where those brokers are assigned.
	brokerOverrides          throttlestore.BrokerOverrides
	overrideThrottleLists    topicThrottledReplicas
	skipOverrideTopicUpdates bool
	reassigningBrokers       reassigningBrokers
	events                   *DDEventWriter
	previouslySetThrottles   replicationCapacityByBroker
	limits                   Limits
	failureThreshold         int
	failures                 int
	skipTopicUpdates         bool
}

func hasActiveOverride(bto throttlestore.BrokerThrottleOverride) bool {
	return bto.Config.Rate != 0
}

func notReassignmentParticipant(bto throttlestore.BrokerThrottleOverride) bool {
	return !bto.ReassignmentParticipant && bto.Config.Rate != 0
}

// Failure increments the failures count and returns true if the
// count exceeds the failures threshold.
func (r *ThrottleManager) Failure() bool {
	r.failures++

	if r.failures > r.failureThreshold {
		return true
	}

	return false
}

// ResetFailures resets the failures count.
func (r *ThrottleManager) ResetFailures() {
	r.failures = 0
}

// DisableTopicUpdates prevents topic throttled replica lists from being
// updated in ZooKeeper.
func (r *ThrottleManager) DisableTopicUpdates() {
	r.skipTopicUpdates = true
}

// DisableTopicUpdates allows topic throttled replica lists updates in ZooKeeper.
func (r *ThrottleManager) EnableTopicUpdates() {
	r.skipTopicUpdates = false
}

// DisableOverrideTopicUpdates prevents topic throttled replica lists for
// topics assigned to override brokers from being updated in ZooKeeper.
func (r *ThrottleManager) DisableOverrideTopicUpdates() {
	r.skipOverrideTopicUpdates = true
}

// EnableOverrideTopicUpdates allows topic throttled replica lists for
// topics assigned to override brokers to be updated in ZooKeeper.
func (r *ThrottleManager) EnableOverrideTopicUpdates() {
	r.skipOverrideTopicUpdates = false
}

// ThrottledBrokers is a list of brokers with a throttle applied
// for an ongoing reassignment.
type ThrottledBrokers struct {
	Src []*kafkametrics.Broker
	Dst []*kafkametrics.Broker
}
