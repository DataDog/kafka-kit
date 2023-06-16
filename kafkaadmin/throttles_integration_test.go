//go:build integration

package kafkaadmin

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSetRemoveThrottle(t *testing.T) {
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

	// Minimizes flakiness; Kafka is ocassionally slower to react than these tests
	// run.
	time.Sleep(1 * time.Second)

	// Get the throttle configs for the topic and brokers.
	tConfigs, err := ka.GetDynamicConfigs(ctx, "topic", []string{"test1", "test2"})
	assert.Nil(t, err)

	bConfigs, err := ka.GetDynamicConfigs(ctx, "broker", []string{"1001", "1002"})
	assert.Nil(t, err)

	// Check output; ensure that it was added.

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

	// Partially delete the throttles. We want to verify that previously set
	// throttles that were not specified to be removed in this call remain at
	// the expected values.

	removeConfig := RemoveThrottleConfig{
		Topics:  []string{"test2"},
		Brokers: []int{1001},
	}

	err = ka.RemoveThrottle(ctx, removeConfig)
	assert.Nil(t, err)

	time.Sleep(1 * time.Second)

	// Get the throttle configs for the topic and brokers.
	tConfigs, err = ka.GetDynamicConfigs(ctx, "topic", []string{"test1", "test2"})
	assert.Nil(t, err)

	bConfigs, err = ka.GetDynamicConfigs(ctx, "broker", []string{"1001", "1002"})
	assert.Nil(t, err)

	// We now expect the resources that had throttles removed to return no
	// dynamic configs.

	expectedTConfigs = ResourceConfigs{
		"test1": {
			"follower.replication.throttled.replicas": "*",
			"leader.replication.throttled.replicas":   "*",
		},
	}

	expectedBConfigs = ResourceConfigs{
		"1002": {
			"follower.replication.throttled.rate": "1000",
			"leader.replication.throttled.rate":   "2000",
		},
	}

	assert.Equal(t, expectedTConfigs, tConfigs)
	assert.Equal(t, expectedBConfigs, bConfigs)
}
