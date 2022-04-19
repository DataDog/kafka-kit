// These are legacy methods that perform updates directly through ZooKeeper.
package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

// legacyApplyTopicThrottles performs config updates directly through ZooKeeper.
func (tm *ThrottleManager) legacyApplyTopicThrottles(throttled topicThrottledReplicas) []error {
	var errs []error

	for t := range throttled {
		// Generate config.
		config := kafkazk.KafkaConfig{
			Type:    "topic",
			Name:    string(t),
			Configs: []kafkazk.KafkaConfigKV{},
		}

		// The sort is important; it avoids unecessary config updates due to the same
		// data but in different orders.
		sort.Strings(throttled[t]["leaders"])
		sort.Strings(throttled[t]["followers"])

		leaderList := strings.Join(throttled[t]["leaders"], ",")
		if leaderList != "" {
			c := kafkazk.KafkaConfigKV{"leader.replication.throttled.replicas", leaderList}
			config.Configs = append(config.Configs, c)
		}

		followerList := strings.Join(throttled[t]["followers"], ",")
		if followerList != "" {
			c := kafkazk.KafkaConfigKV{"follower.replication.throttled.replicas", followerList}
			config.Configs = append(config.Configs, c)
		}

		// Write the config.
		_, err := tm.zk.UpdateKafkaConfig(config)
		if err != nil {
			errs = append(errs, fmt.Errorf("Error setting throttle list on topic %s: %s\n", t, err))
		}
	}

	return nil
}
