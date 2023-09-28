package replication

import (
	"strconv"

	"github.com/DataDog/kafka-kit/v4/mapper"
)

// TopicStates is a map of topic names to kafakzk.TopicState.
type TopicStates map[string]mapper.TopicState

// legacyGetTopicsWithThrottledBrokers returns a TopicThrottledReplicas that
// includes any topics that have partitions assigned to brokers with a static
// throttle rate set.
func (tm *ThrottleManager) legacyGetTopicsWithThrottledBrokers() (TopicThrottledReplicas, error) {
	// Fetch all topic states.
	states, err := tm.legacyGetAllTopicStates()
	if err != nil {
		return nil, err
	}

	//Lookup brokers with overrides set (previously we only filtered for brokers
	// that are not a reassignment participant).
	throttledBrokers := tm.brokerOverrides.Filter(AlsoReassignmentParticipant)

	// Construct a TopicThrottledReplicas that includes any topics with replicas
	// assigned to brokers with overrides. The throttled list only includes brokers
	// with throttles set rather than all configured replicas.
	var throttleLists = make(TopicThrottledReplicas)

	// For each topic...
	for topicName, state := range states {
		// TODO(jamie): make this configurable.
		if topicName == "__consumer_offsets" {
			continue
		}
		// For each partition...
		for partn, replicas := range state.Partitions {
			// For each replica assignment...
			for _, assignedID := range replicas {
				// If the replica is a throttled broker, append that broker to the
				// throttled list for this {topic, partition}.
				if _, exists := throttledBrokers[assignedID]; exists {
					throttleLists.addReplica(
						Topic(topicName),
						partn,
						ReplicaType("followers"),
						strconv.Itoa(assignedID))
				}
			}
		}
	}

	return throttleLists, nil
}

// legacyGetAllTopicStates returns a TopicStates for all topics in Kafka.
func (tm *ThrottleManager) legacyGetAllTopicStates() (TopicStates, error) {
	var states = make(TopicStates)

	// Get all topics.
	topics, err := tm.zk.GetTopics(topicsRegex)
	if err != nil {
		return nil, err
	}

	// Fetch state for each topic.
	for _, topic := range topics {
		state, err := tm.zk.GetTopicState(topic)
		if err != nil {
			return nil, err
		}
		states[topic] = *state
	}

	return states, nil
}

/*

	This code isn't currently accessed, but holding it as a comment until we
	remove all the deprecated bits.

// TopicStatesFilterFn specifies a filter function.
type TopicStatesFilterFn func(mapper.TopicState) bool

// Filter takes a TopicStatesFilterFn and returns a TopicStates where
// all elements return true as an input to the filter func.
func (t TopicStates) Filter(fn TopicStatesFilterFn) TopicStates {
	var ts = make(TopicStates)
	for name, state := range t {
		if fn(state) {
			ts[name] = state
		}
	}

	return ts
}
*/
