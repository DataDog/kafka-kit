package kafkaadmin

// Note: tests are located in the stub subdir so that mock data can be used
// without resulting in import cycle.

// TopicStates is a map of topic names to TopicState.
type TopicStates map[string]TopicState

// TopicState describes the current state of a topic.
type TopicState struct {
	Name              string
	Partitions        int32
	ReplicationFactor int32
	PartitionStates   map[int]PartitionState
}

// PartitionState describes the state of a partition.
type PartitionState struct {
	ID       int32
	Leader   int32
	Replicas []int32
	ISR      []int32
}

// NewTopicStates initializes a TopicStates.
func NewTopicStates() TopicStates {
	return make(TopicStates)
}

// NewTopicState initializes a TopicState.
func NewTopicState(name string) TopicState {
	return TopicState{
		Name:            name,
		PartitionStates: make(map[int]PartitionState),
	}
}

// UnderReplicated returns a TopicStates and only includes under-replicated topics.
func (ts TopicStates) UnderReplicated() TopicStates {
	filtered := TopicStates{}

	// Loop through all topics.
	for topic, state := range ts {
		// As of writing, the underlying confluent-kafka-go library returns the
		// following PartitionMetadata for an under-replicated topic:
		// {ID:0 Error:Success Leader:1001 Replicas:[1001 1002 1003] Isrs:[1001 1003]}
		// Since the Error field is in a non-error state, the best inference we have
		// as to whether a partition (and therefore its parent topic) is under-replicated
		// is looking for those where len(ISR) < len(Replicas). This also means that
		// under-replicated topics are indistinguishable from reassigning topics.
		for _, partnState := range state.PartitionStates {
			if len(partnState.ISR) < len(partnState.Replicas) {
				filtered[topic] = state
				continue
			}
		}
	}

	return filtered
}
