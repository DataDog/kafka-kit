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

// topicStates is a map of topic names to kafakzk.TopicState.
type topicStates map[string]kafkazk.TopicState

// getRecoveringTopics
func getRecoveringTopics(params *ReplicationThrottleConfigs) (topicStates, error) {
	return getAllTopicStates(params.zk)
}

func getAllTopicStates(zk kafkazk.Handler) (topicStates, error) {
	var states = make(topicStates)

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
