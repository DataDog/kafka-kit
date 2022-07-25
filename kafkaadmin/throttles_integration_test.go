//go:build integration

package kafkaadmin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetThrottle(t *testing.T) {
	ctx, ka := testKafkaAdminClient(t)

	// Populate a config.
	config := SetThrottleConfig{
		Topics: []string{"test1", "test2"},
		Brokers: map[int]BrokerThrottleConfig{
			1001: {
				InboundLimitBytes:  1000,
				OutboundLimitBytes: 2000,
			},
			1002: {
				InboundLimitBytes:  1000,
				OutboundLimitBytes: 2000,
			},
		},
	}

	err := ka.SetThrottle(ctx, config)
	assert.Nil(t, err)

	// Get the throttle configs for the topic and brokers.
	tConfigs, err := ka.GetDynamicConfigs(ctx, "topic", []string{"test1", "test2"})
	assert.Nil(t, err)

	bConfigs, err := ka.GetDynamicConfigs(ctx, "broker", []string{"1001", "1002"})
	assert.Nil(t, err)

	// Check output.

	expectedTConfigs := ResourceConfigs{
		"test1": {
			"follower.replication.throttled.replicas": "*",
			"leader.replication.throttled.replicas":   "*",
		},
		"test2": {
			"follower.replication.throttled.replicas": "*",
			"leader.replication.throttled.replicas":   "*",
		},
	}

	expectedBConfigs := ResourceConfigs{
		"1001": {
			"follower.replication.throttled.rate": "1000",
			"leader.replication.throttled.rate":   "2000",
		},
		"1002": {
			"follower.replication.throttled.rate": "1000",
			"leader.replication.throttled.rate":   "2000",
		},
	}

	assert.Equal(t, expectedTConfigs, tConfigs)
	assert.Equal(t, expectedBConfigs, bConfigs)
}
