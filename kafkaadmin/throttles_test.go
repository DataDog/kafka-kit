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

func TestClearTopicThrottleConfigs(t *testing.T) {
	tests := []struct {
		input       ResourceConfigs
		expected    ResourceConfigs
		expectedErr error
	}{
		// Case: Empty.
		{
			input:       ResourceConfigs{},
			expected:    ResourceConfigs{},
			expectedErr: nil,
		},
		// Case: One topic only has configs unrelated to throttles and is excluded.
		// Another topic has a throttle that needs to be unset.
		{
			input: ResourceConfigs{
				"topic1": map[string]string{
					"message.retention.ms": "10000",
				},
				"topic2": map[string]string{
					"leader.replication.throttled.replicas": "0:1001",
				},
			},
			expected: ResourceConfigs{
				"topic2": map[string]string{},
			},
			expectedErr: nil,
		},
		// Case: Two topics with throttles that need to be cleared. One topic has
		// unrelated configs that need to be retained.
		{
			input: ResourceConfigs{
				"topic1": map[string]string{
					"leader.replication.throttled.replicas": "*",
				},
				"topic2": map[string]string{
					"follower.replication.throttled.replicas": "*",
					"message.retention.ms":                    "10000",
				},
			},
			expected: ResourceConfigs{
				"topic1": map[string]string{},
				"topic2": map[string]string{
					"message.retention.ms": "10000",
				},
			},
			expectedErr: nil,
		},
	}

	for i, testCase := range tests {
		err := clearTopicThrottleConfigs(testCase.input)
		// Check the error.
		assert.Equalf(t, testCase.expectedErr, err, fmt.Sprintf("case %d", i))
		// Check the output.
		assert.Equalf(t, testCase.expected, testCase.input, fmt.Sprintf("case %d", i))
	}
}

func TestPopulateBrokerConfigs(t *testing.T) {
	inputBrokers := map[int]BrokerThrottleConfig{
		1001: {
			InboundLimitBytes:  4000,
			OutboundLimitBytes: 2000,
		},
		1002: {
			InboundLimitBytes:  3000,
			OutboundLimitBytes: 4000,
		},
	}

	tests := []struct {
		input       ResourceConfigs
		expected    ResourceConfigs
		expectedErr error
	}{
		// Case: Adding throttles for two brokers. One is not in the ResourceConfigs
		// yet, the other has a pre-existing config we need to retain.
		{
			input: ResourceConfigs{
				"1001": map[string]string{
					"log.cleaner.threads": "8",
				},
			},
			expected: ResourceConfigs{
				"1001": map[string]string{
					"log.cleaner.threads":                 "8",
					"leader.replication.throttled.rate":   "2000",
					"follower.replication.throttled.rate": "4000",
				},
				"1002": map[string]string{
					"leader.replication.throttled.rate":   "4000",
					"follower.replication.throttled.rate": "3000",
				},
			},
			expectedErr: nil,
		},
		// Case: Updating configs for a broker that has previously had throttles set.
		// Another broker exists but has no configs.
		{
			input: ResourceConfigs{
				"1001": map[string]string{
					"leader.replication.throttled.rate":   "9000",
					"follower.replication.throttled.rate": "8000",
				},
				"1002": map[string]string{},
			},
			expected: ResourceConfigs{
				"1001": map[string]string{
					"leader.replication.throttled.rate":   "2000",
					"follower.replication.throttled.rate": "4000",
				},
				"1002": map[string]string{
					"leader.replication.throttled.rate":   "4000",
					"follower.replication.throttled.rate": "3000",
				},
			},
			expectedErr: nil,
		},
	}

	for i, testCase := range tests {
		err := populateBrokerConfigs(inputBrokers, testCase.input)
		// Check the error.
		assert.Equalf(t, testCase.expectedErr, err, fmt.Sprintf("case %d", i))
		// Check the output.
		assert.Equalf(t, testCase.expected, testCase.input, fmt.Sprintf("case %d", i))
	}
}

// func TestClearBrokerThrottleConfigs(t *testing.T) {}
