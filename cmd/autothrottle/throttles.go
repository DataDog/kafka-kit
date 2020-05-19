package main

import (
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
	reassigningBrokers     reassigningBrokers
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
	// Whether this override is for a broker that's part of a reassignment.
	ReassignmentParticipant bool
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
