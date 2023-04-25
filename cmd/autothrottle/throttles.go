package main

import (
	"context"
	"time"

	"github.com/DataDog/kafka-kit/v4/internal/autothrottle/throttlestore"
	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
	"github.com/DataDog/kafka-kit/v4/kafkametrics"
	"github.com/DataDog/kafka-kit/v4/kafkazk"
)

// ThrottleManager manages Kafka throttle rates.
type ThrottleManager struct {
	reassignments          kafkazk.Reassignments
	zk                     kafkazk.Handler
	km                     kafkametrics.Handler
	ka                     kafkaadmin.KafkaAdmin
	overrideRate           int
	kafkaNativeMode        bool
	kafkaAPIRequestTimeout int
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

// InitKafkaAdmin takes a csv Kafka broker list and initializes a kafkaadmin
// client.
func (tm *ThrottleManager) InitKafkaAdmin(brokers string) error {
	cfg := kafkaadmin.Config{BootstrapServers: brokers}
	ka, err := kafkaadmin.NewClient(cfg)
	if err != nil {
		return err
	}

	tm.ka = ka
	return nil
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
func (tm *ThrottleManager) ResetFailures() {
	tm.failures = 0
}

// DisableTopicUpdates prevents topic throttled replica lists from being
// updated in ZooKeeper.
func (tm *ThrottleManager) DisableTopicUpdates() {
	tm.skipTopicUpdates = true
}

// DisableTopicUpdates allows topic throttled replica lists updates in ZooKeeper.
func (tm *ThrottleManager) EnableTopicUpdates() {
	tm.skipTopicUpdates = false
}

// DisableOverrideTopicUpdates prevents topic throttled replica lists for
// topics assigned to override brokers from being updated in ZooKeeper.
func (tm *ThrottleManager) DisableOverrideTopicUpdates() {
	tm.skipOverrideTopicUpdates = true
}

// EnableOverrideTopicUpdates allows topic throttled replica lists for
// topics assigned to override brokers to be updated in ZooKeeper.
func (tm *ThrottleManager) EnableOverrideTopicUpdates() {
	tm.skipOverrideTopicUpdates = false
}

// kafkaRequestContext returns a context and cancel func with the default
// ThrottleManager Kafka API request timeout.
func (tm *ThrottleManager) kafkaRequestContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(
		context.Background(),
		time.Duration(tm.kafkaAPIRequestTimeout)*time.Second,
	)
}

// ThrottledBrokers is a list of brokers with a throttle applied
// for an ongoing reassignment.
type ThrottledBrokers struct {
	Src []*kafkametrics.Broker
	Dst []*kafkametrics.Broker
}
