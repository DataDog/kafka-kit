package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

var (
	errInvalidReplicaType error = errors.New("invalid replica type")
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

// Replica broker IDs as a []string where string == partition_number:broker_id.
type brokerIDs []string

var acceptedReplicaTypes = map[replicaType]struct{}{
	"leaders":   struct{}{},
	"followers": struct{}{},
}

// addReplica takes a topic, partition number, role (leader, follower), and
// broker ID and adds the configuration to the topicThrottledReplicas.
func (ttr topicThrottledReplicas) addReplica(topic topic, partn string, role replicaType, id string) error {
	if _, exist := acceptedReplicaTypes[role]; !exist {
		return errInvalidReplicaType
	}

	// Check / create the topic entry.
	if _, exist := ttr[topic]; !exist {
		ttr[topic] = make(throttled)
	}

	// Check / create the leader/follower list.
	if ttr[topic][role] == nil {
		ttr[topic][role] = []string{}
	}

	// Form the throttled replica string.
	str := fmt.Sprintf("%s:%s", partn, id)

	// If the entry is already in the list, return early.
	for _, entry := range ttr[topic][role] {
		if entry == str {
			return nil
		}
	}

	// Otherwise add the entry.
	l := ttr[topic][role]
	l = append(l, str)
	ttr[topic][role] = l

	return nil
}

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

// getTopicsWithThrottledBrokers returns a topicThrottledReplicas that includes
// any topics that have partitions assigned to brokers with a static throttle
// rate set.
func getTopicsWithThrottledBrokers(params *ReplicationThrottleConfigs) (topicThrottledReplicas, error) {
	// Fetch all topic states.
	states, err := getAllTopicStates(params.zk)
	if err != nil {
		return nil, err
	}

	// Lookup brokers with overrides set that are not a reassignment participant.
	notReassignmentParticipant := func(bto BrokerThrottleOverride) bool {
		return !bto.ReassignmentParticipant && bto.Config.Rate != 0
	}

	throttledBrokers := params.brokerOverrides.Filter(notReassignmentParticipant)

	// Construct a topicThrottledReplicas that includes any topics with replicas
	// assigned to brokers with overrides. The throttled list only includes brokers
	// with throttles set rather than all configured replicas.
	var throttleLists = make(topicThrottledReplicas)

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
						topic(topicName),
						partn,
						replicaType("followers"),
						strconv.Itoa(assignedID))
				}
			}
		}
	}

	return throttleLists, nil
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
