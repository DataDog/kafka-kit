package replication

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
	changeThreshold        float64
	// The following three fields are for brokers with static overrides set
	// and a TopicThrottledReplicas for topics where those brokers are assigned.
	brokerOverrides          throttlestore.BrokerOverrides
	overrideThrottleLists    TopicThrottledReplicas
	skipOverrideTopicUpdates bool
	reassigningBrokers       reassigningBrokers
	events                   EventWriter
	previouslySetThrottles   ReplicationCapacityByBroker
	limits                   Limits
	failureThreshold         int
	failures                 int
	skipTopicUpdates         bool
}

// ThrottleManagerConfig configures a ThrottleManager.
type ThrottleManagerConfig struct {
	Limits                 Limits
	FailureThreshold       int
	ChangeThreshold        float64
	KafkaZK                kafkazk.Handler
	KafkaMetrics           kafkametrics.Handler
	KafkaNativeMode        bool
	KafkaAPIRequestTimeout int
	Events                 EventWriter
}

// EventWriter for writing event key values.
type EventWriter interface {
	Write(string, string)
}

// NewThrottleManager takes a ThrottleManagerConfig and returns a
// *ThrottleManager.
func NewThrottleManager(cfg ThrottleManagerConfig) (*ThrottleManager, error) {
	return &ThrottleManager{
		limits:                 cfg.Limits,
		failureThreshold:       cfg.FailureThreshold,
		changeThreshold:        cfg.ChangeThreshold,
		zk:                     cfg.KafkaZK,
		km:                     cfg.KafkaMetrics,
		kafkaNativeMode:        cfg.KafkaNativeMode,
		kafkaAPIRequestTimeout: cfg.KafkaAPIRequestTimeout,
		events:                 cfg.Events,
		previouslySetThrottles: make(ReplicationCapacityByBroker),
	}, nil
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

// Failure increments the failures count and returns true if the
// count exceeds the failures threshold.
func (r *ThrottleManager) Failure() bool {
	r.failures++

	if r.failures > r.failureThreshold {
		return true
	}

	return false
}

// SetBrokerOverrides sets the ThrottleManager brokerOverrides.
func (tm *ThrottleManager) SetBrokerOverrides(bo throttlestore.BrokerOverrides) {
	tm.brokerOverrides = bo
}

// SetReassigningBrokers sets the ThrottleManager reassigningBrokers.
func (tm *ThrottleManager) SetReassigningBrokers(rb reassigningBrokers) {
	tm.reassigningBrokers = rb
}

// SetOverrideRate sets the ThrottleManager overrideRate.
func (tm *ThrottleManager) SetOverrideRate(r int) {
	tm.overrideRate = r
}

// SetReassignments sets the ThrottleManager reassignments.
func (tm *ThrottleManager) SetReassignments(r kafkazk.Reassignments) {
	tm.reassignments = r
}

// GetBrokerOverrides returns the ThrottleManager brokerOverrides.
func (tm *ThrottleManager) GetBrokerOverrides() throttlestore.BrokerOverrides {
	return tm.brokerOverrides
}

// GetOverrideThrottleLists returns the ThrottleManager overrideThrottleLists.
func (tm *ThrottleManager) GetOverrideThrottleLists() TopicThrottledReplicas {
	return tm.overrideThrottleLists
}

// SetOverrideThrottleLists sets the ThrottleManager overrideThrottleLists.
func (tm *ThrottleManager) SetOverrideThrottleLists(t TopicThrottledReplicas) {
	tm.overrideThrottleLists = t
}

// ResetPreviousThrottles resets and previously set throttles.
func (tm *ThrottleManager) ResetPreviousThrottles() {
	tm.previouslySetThrottles.reset()
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

// HasActiveOverride filter func.
func HasActiveOverride(bto throttlestore.BrokerThrottleOverride) bool {
	return bto.Config.Rate != 0
}

// NotReassignmentParticipant filter func.
func NotReassignmentParticipant(bto throttlestore.BrokerThrottleOverride) bool {
	return !bto.ReassignmentParticipant && bto.Config.Rate != 0
}

// AlsoReassignmentParticipant filter func.
func AlsoReassignmentParticipant(bto throttlestore.BrokerThrottleOverride) bool {
	return bto.Config.Rate != 0
}

// ThrottledBrokers is a list of brokers with a throttle applied
// for an ongoing reassignment.
type ThrottledBrokers struct {
	Src []*kafkametrics.Broker
	Dst []*kafkametrics.Broker
}
