package kafkaadmin

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPopulateTopicConfigs(t *testing.T) {
	inputTopics := []string{"topic1", "topic2"}

	tests := []struct {
		input       ResourceConfigs
		expected    ResourceConfigs
		expectedErr error
	}{
		// Case: the input ResourceConfigs is empty. We should get back an entry for
		// each of the inputTopics.
		{
			input: ResourceConfigs{},
			expected: ResourceConfigs{
				"topic1": map[string]string{
					"leader.replication.throttled.replicas":   "*",
					"follower.replication.throttled.replicas": "*",
				},
				"topic2": map[string]string{
					"leader.replication.throttled.replicas":   "*",
					"follower.replication.throttled.replicas": "*",
				},
			},
			expectedErr: nil,
		},
		// Case: the input ResourceConfigs has a topic with one existing but unrelated
		// dynamic config, one topic with a dynamic config that will be updated, and
		// one topic that is not in the inputTopics.
		{
			input: ResourceConfigs{
				"topic1": map[string]string{
					"message.retention.ms": "10000",
				},
				"topic2": map[string]string{
					"leader.replication.throttled.replicas": "0:1001",
				},
				"topic3": map[string]string{
					"message.retention.ms": "10000",
				},
			},
			expected: ResourceConfigs{
				"topic1": map[string]string{
					"message.retention.ms":                    "10000",
					"leader.replication.throttled.replicas":   "*",
					"follower.replication.throttled.replicas": "*",
				},
				"topic2": map[string]string{
					"leader.replication.throttled.replicas":   "*",
					"follower.replication.throttled.replicas": "*",
				},
			},
			expectedErr: nil,
		},
		// Case: one topic exists but doesn't have any configs set. This shouldn't
		// happen but is essentially a fuzz.
		{
			input: ResourceConfigs{
				"topic1": map[string]string{},
			},
			expected: ResourceConfigs{
				"topic1": map[string]string{
					"leader.replication.throttled.replicas":   "*",
					"follower.replication.throttled.replicas": "*",
				},
				"topic2": map[string]string{
					"leader.replication.throttled.replicas":   "*",
					"follower.replication.throttled.replicas": "*",
				},
			},
			expectedErr: nil,
		},
	}

	for i, testCase := range tests {
		err := populateTopicConfigs(inputTopics, testCase.input)
		// Check the error.
		assert.Equalf(t, testCase.expectedErr, err, fmt.Sprintf("case %d", i))
		// Check the output.
		assert.Equalf(t, testCase.expected, testCase.input, fmt.Sprintf("case %d", i))
	}
}

// func TestClearTopicThrottleConfigs(t *testing.T) {}
// func TestPopulateBrokerConfigs(t *testing.T) {}
// func TestClearBrokerThrottleConfigs(t *testing.T) {}
