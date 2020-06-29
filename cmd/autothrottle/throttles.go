package main

import (
	"github.com/DataDog/kafka-kit/v3/kafkametrics"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

// ReplicationThrottleConfigs holds all the data needed to call
// updateReplicationThrottle.
type ReplicationThrottleConfigs struct {
	reassignments kafkazk.Reassignments
	zk            kafkazk.Handler
	km            kafkametrics.Handler
	overrideRate  int
	// The following two fields are for brokers with static overrides set
	// and a topicThrottledReplicas for topics where those brokers are assigned.
	brokerOverrides        BrokerOverrides
	overrideThrottleLists  topicThrottledReplicas
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

// Copy returns a copy of a BrokerThrottleOverride.
func (b BrokerThrottleOverride) Copy() BrokerThrottleOverride {
	return BrokerThrottleOverride{
		ID:                      b.ID,
		ReassignmentParticipant: b.ReassignmentParticipant,
		Config: ThrottleOverrideConfig{
			Rate:       b.Config.Rate,
			AutoRemove: b.Config.AutoRemove,
		},
	}
}

// IDs returns a []int of broker IDs held by the BrokerOverrides.
func (b BrokerOverrides) IDs() []int {
	var ids []int
	for id := range b {
		ids = append(ids, id)
	}

	return ids
}

// BrokerOverridesFilterFn specifies a filter function.
type BrokerOverridesFilterFn func(BrokerThrottleOverride) bool

// Filter funcs.

func hasActiveOverride(bto BrokerThrottleOverride) bool {
	return bto.Config.Rate != 0
}

// Filter takes a BrokerOverridesFilterFn and returns a BrokerOverrides where
// all elements return true as an input to the filter func.
func (b BrokerOverrides) Filter(fn BrokerOverridesFilterFn) BrokerOverrides {
	var bo = make(BrokerOverrides)
	for _, bto := range b {
		if fn(bto) {
			bo[bto.ID] = bto.Copy()
		}
	}

	return bo
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
