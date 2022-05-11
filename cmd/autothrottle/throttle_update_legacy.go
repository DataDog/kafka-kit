// These are legacy methods that perform updates directly through ZooKeeper.
package main

import (
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/kafka-kit/v4/kafkazk"
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

func (tm *ThrottleManager) legacyRemoveTopicThrottles() error {
	topics, err := tm.zk.GetTopics(topicsRegex)
	if err != nil {
		return err
	}

	var errTopics []string

	for _, topic := range topics {
		config := kafkazk.KafkaConfig{
			Type: "topic",
			Name: topic,
			Configs: []kafkazk.KafkaConfigKV{
				{"leader.replication.throttled.replicas", ""},
				{"follower.replication.throttled.replicas", ""},
			},
		}

		// Update the config.
		_, err := tm.zk.UpdateKafkaConfig(config)
		if err != nil {
			errTopics = append(errTopics, topic)
		}

		time.Sleep(250 * time.Millisecond)
	}

	if errTopics != nil {
		names := strings.Join(errTopics, ", ")
		return fmt.Errorf("Error removing throttle config on topics: %s\n", names)
	}

	return nil
}

func (tm *ThrottleManager) legacyRemoveBrokerThrottlesByID(ids map[int]struct{}) error {
	var unthrottledBrokers []int
	var errorEncountered bool

	// Unset throttles.
	for b := range ids {
		config := kafkazk.KafkaConfig{
			Type: "broker",
			Name: strconv.Itoa(b),
			Configs: []kafkazk.KafkaConfigKV{
				{"leader.replication.throttled.rate", ""},
				{"follower.replication.throttled.rate", ""},
			},
		}

		changed, err := tm.zk.UpdateKafkaConfig(config)
		switch err.(type) {
		case nil:
		case kafkazk.ErrNoNode:
			// We'd get an ErrNoNode here only if the parent path for dynamic broker
			// configs (/config/brokers) if it doesn't exist, which can happen in
			// new clusters that have never had dynamic configs applied. Rather than
			// creating that znode, we'll just ignore errors here; if the znodes
			// don't exist, there's not even config to remove.
		default:
			errorEncountered = true
			log.Printf("Error removing throttle on broker %d: %s\n", b, err)
		}

		if changed[0] || changed[1] {
			unthrottledBrokers = append(unthrottledBrokers, b)
			log.Printf("Throttle removed on broker %d\n", b)

			// Unset the previously stored throttle rate.
			tm.previouslySetThrottles[b] = [2]*float64{}
		}

		// Hardcoded sleep to reduce ZK load.
		time.Sleep(250 * time.Millisecond)
	}

	// Write event.
	if len(unthrottledBrokers) > 0 {
		m := fmt.Sprintf("Replication throttle removed on the following brokers: %v",
			unthrottledBrokers)
		tm.events.Write("Broker replication throttle removed", m)
	}

	// Lazily check if any errors were encountered, return a generic error.
	if errorEncountered {
		return errors.New("one or more throttles were not cleared")
	}

	return nil
}
