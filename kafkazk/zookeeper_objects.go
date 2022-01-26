package kafkazk

// TopicStateISR is a map of partition numbers to PartitionState.
type TopicStateISR map[string]PartitionState

// PartitionState is used for unmarshalling json data from a partition state:
// e.g. /brokers/topics/some-topic/partitions/0/state
type PartitionState struct {
	Version         int   `json:"version"`
	ControllerEpoch int   `json:"controller_epoch"`
	Leader          int   `json:"leader"`
	LeaderEpoch     int   `json:"leader_epoch"`
	ISR             []int `json:"isr"`
}

// Reassignments is a map of topic:partition:brokers.
type Reassignments map[string]map[int][]int

// reassignPartitions is used for unmarshalling /admin/reassign_partitions data.
type reassignPartitions struct {
	Partitions []reassignConfig `json:"partitions"`
}

type reassignConfig struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

// TopicConfig is used for unmarshalling  /config/topics/<topic> from ZooKeeper.
type TopicConfig struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

// TopicMetadata holds the topic data found in the /brokers/topics/<topic> znode.
// This is designed for the version 3 fields present in Kafka version ~2.4+.
type TopicMetadata struct {
	Version          int
	Name             string
	TopicID          string `json:"topic_id"`
	Partitions       map[int][]int
	AddingReplicas   map[int][]int `json:"adding_replicas"`
	RemovingReplicas map[int][]int `json:"removing_replicas"`
}

// KafkaConfig is used to issue configuration updates to either
// topics or brokers in ZooKeeper.
type KafkaConfig struct {
	Type    string          // Topic or broker.
	Name    string          // Entity name.
	Configs []KafkaConfigKV // Config KVs.
}

// KafkaConfigKV is a [2]string{key, value} representing a Kafka configuration.
type KafkaConfigKV [2]string

// KafkaConfigData is used for unmarshalling /config/<type>/<name> data
// from ZooKeeper.
type KafkaConfigData struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

// NewKafkaConfigData creates a KafkaConfigData.
func NewKafkaConfigData() KafkaConfigData {
	return KafkaConfigData{
		Config: make(map[string]string),
	}
}

// Reassignments returns a Reassignments from a given topics TopicMetadata.
func (tm TopicMetadata) Reassignments() Reassignments {
	var reassignments = make(Reassignments)

	// Create a reassignment entry for this topic.
	reassignments[tm.Name] = make(map[int][]int)

	/*
			Build a replica set for each partition that excludes brokers being removed.
			This represents the desired, final partition assignment.
			Example:
		  Given the following TopicMetadata:
			  TopicMetadata {
					Version: 3,
					Name:	"mytopic",
					TopicID: "some_id",
					Partitions: {"0":[1001,1003,1002]},
					AddingReplicas: {"0":[1001]},
					RemovingReplicas: {0":[1002]}
			  }

			We want the reassignment result: {"0":[1001,1003]}.
	*/
	for partition, currentReplicas := range tm.Partitions {
		// We only have to modify the final set if there's a RemovingReplicas entry.
		if removing, hasRemoving := tm.RemovingReplicas[partition]; hasRemoving {
			var targetReplicas []int
			for _, replica := range currentReplicas {
				if !inIntSlice(replica, removing) {
					targetReplicas = append(targetReplicas, replica)
				}
			}
			// Add the result to the reassignments.
			reassignments[tm.Name][partition] = targetReplicas
			continue
		}

		// We then only need to add partitions that currently have an AddingReplicas
		// entry and weren't already added due to having a RemovingReplicas entry.
		// These are automatically skipped with the 'continue' above.
		if _, hasAdding := tm.AddingReplicas[partition]; hasAdding {
			targetReplicas := make([]int, len(tm.Partitions[partition]))
			copy(targetReplicas, tm.Partitions[partition])
			reassignments[tm.Name][partition] = targetReplicas
		}
	}

	return reassignments
}

func inIntSlice(i int, s []int) bool {
	for _, e := range s {
		if i == e {
			return true
		}
	}

	return false
}
