// These are legacy methods that perform updates directly through ZooKeeper.
package main

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"time"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

func (tm *ThrottleManager) legacyApplyBrokerThrottles(configs map[int]kafkazk.KafkaConfig, capacities replicationCapacityByBroker) (chan brokerChangeEvent, []error) {
	events := make(chan brokerChangeEvent, len(configs)*2)
	var errs []error

	for ID, config := range configs {
		changes, err := tm.zk.UpdateKafkaConfig(config)
		if err != nil {
			errs = append(errs, fmt.Errorf("Error setting throttle on broker %d: %s", ID, err))
		}

		for i, changed := range changes {
			if changed {
				// This will be either "leader.replication.throttled.rate" or
				// "follower.replication.throttled.rate".
				throttleConfigString := config.Configs[i][0]
				// Split on ".", get "leader" or "follower" string.
				role := strings.Split(throttleConfigString, ".")[0]

				log.Printf("Updated throttle on broker %d [%s]\n", ID, role)

				var rate *float64

				// Store the configured rate.
				switch role {
				case "leader":
					rate = capacities[ID][0]
					tm.previouslySetThrottles.storeLeaderCapacity(ID, *rate)
				case "follower":
					rate = capacities[ID][1]
					tm.previouslySetThrottles.storeFollowerCapacity(ID, *rate)
				}

				events <- brokerChangeEvent{
					id:   ID,
					role: role,
					rate: *rate,
				}
			}
		}

		// Hard coded sleep to reduce
		// ZK load.
		time.Sleep(250 * time.Millisecond)
	}

	close(events)

	return events, errs
}

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
