package main

import (
	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

// topicThrottledReplicas is a map of topic names to throttled types.
// This is ultimately populated as follows:
//   map[topic]map[leaders]["0:1001", "1:1002"]
//   map[topic]map[followers]["2:1003", "3:1004"]
type topicThrottledReplicas map[topic]throttled

// throttled is a replica type (leader, follower) to replica list.
type throttled map[replicaType]brokerIDs

// topic name.
type topic string

// leader, follower.
type replicaType string

// Replica broker IDs as a []string.
type brokerIDs []string

// TopicStates is a map of topic names to kafakzk.TopicState.
type TopicStates map[string]kafkazk.TopicState

// TopicStatesFilterFn specifies a filter function.
type TopicStatesFilterFn func(kafkazk.TopicState) bool

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

// getTopicsWithThrottledBrokers returns a TopicStates that includes any topics that
// have partitions assigned to brokers with a static throttle rate set.
func getTopicsWithThrottledBrokers(params *ReplicationThrottleConfigs) (TopicStates, error) {
	// Fetch all topic states.
	states, err := getAllTopicStates(params.zk)
	if err != nil {
		return nil, err
	}

	// Lookup brokers with overrides set that are not a reassignment participant.
	overridesFilterFn := func(bto BrokerThrottleOverride) bool {
		return !bto.ReassignmentParticipant
	}

	overrides := params.brokerOverrides.Filter(overridesFilterFn)

	// Filter out topic states where the target brokers are assigned to at least
	// one partition.
	statesFilterFn := func(ts kafkazk.TopicState) bool {
		for _, assignment := range ts.Partitions {
			for _, id := range assignment {
				if _, exists := overrides[id]; exists {
					return true
				}
			}
		}
		return false
	}

	return states.Filter(statesFilterFn), nil
}

// getAllTopicStates returns a TopicStates for all topics in Kafka.
func getAllTopicStates(zk kafkazk.Handler) (TopicStates, error) {
	var states = make(TopicStates)

	// Get all topics.
	topics, err := zk.GetTopics(topicsRegex)
	if err != nil {
		return nil, err
	}

	// Fetch state for each topic.
	for _, topic := range topics {
		state, err := zk.GetTopicState(topic)
		if err != nil {
			return nil, err
		}
		states[topic] = *state
	}

	return states, nil
}
