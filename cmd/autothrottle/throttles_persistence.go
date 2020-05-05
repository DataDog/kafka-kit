package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/kafka-kit/kafkazk"
)

var (
	errNoOverideSet = errors.New("no override set at path")
)

// brokerChangeEvent is the message type returned in the events channel
// from the applyBrokerThrottles func.
type brokerChangeEvent struct {
	id   int
	role string
	rate float64
}

// applyBrokerThrottles takes a set of brokers, a replication throttle rate
// string, rate, map for tracking applied throttles, and zk kafkazk.Handler
// zookeeper client. For each broker, the throttle rate is applied and if
// successful, the rate is stored in the throttles map for future reference.
// A channel of events and []string of errors is returned.
func applyBrokerThrottles(bs map[int]struct{}, capacities, prevThrottles replicationCapacityByBroker, l Limits, zk kafkazk.Handler) (chan brokerChangeEvent, []string) {
	events := make(chan brokerChangeEvent, len(bs)*2)
	var errs []string

	// Set the throttle config for all reassigning brokers.
	for ID := range bs {
		brokerConfig := kafkazk.KafkaConfig{
			Type:    "broker",
			Name:    strconv.Itoa(ID),
			Configs: []kafkazk.KafkaConfigKV{},
		}

		// Check if a rate was determined for each role (leader, follower) type.
		for i, rate := range capacities[ID] {
			if rate == nil {
				continue
			}

			role := roleFromIndex(i)

			prevRate := prevThrottles[ID][i]
			if prevRate == nil {
				v := 0.00
				prevRate = &v
			}

			var max float64
			switch role {
			case "leader":
				max = l["srcMax"]
			case "follower":
				max = l["dstMax"]
			}

			log.Printf("Replication throttle rate for broker %d [%s] (based on a %.0f%% max free capacity utilization): %0.2fMB/s\n",
				ID, role, max, *rate)

			// Check if the delta between the newly calculated throttle and the
			// previous throttle exceeds the ChangeThreshold param.
			d := math.Abs((*prevRate - *rate) / *prevRate * 100)
			if d < Config.ChangeThreshold {
				log.Printf("Proposed throttle is within %.2f%% of the previous throttle "+
					"(below %.2f%% threshold), skipping throttle update for broker %d\n",
					d, Config.ChangeThreshold, ID)
				continue
			}

			rateBytesString := fmt.Sprintf("%.0f", *rate*1000000.00)

			// Append config.
			c := kafkazk.KafkaConfigKV{fmt.Sprintf("%s.replication.throttled.rate", role), rateBytesString}
			brokerConfig.Configs = append(brokerConfig.Configs, c)
		}

		// Write the throttle config.
		changes, err := zk.UpdateKafkaConfig(brokerConfig)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Error setting throttle on broker %d: %s\n", ID, err))
		}

		for i, changed := range changes {
			if changed {
				// This will be either "leader.replication.throttled.rate" or
				// "follower.replication.throttled.rate".
				throttleConfigString := brokerConfig.Configs[i][0]
				// Split on ".", get "leader" or "follower" string.
				role := strings.Split(throttleConfigString, ".")[0]

				log.Printf("Updated throttle on broker %d [%s]\n", ID, role)

				var rate *float64

				// Store the configured rate.
				switch role {
				case "leader":
					rate = capacities[ID][0]
					prevThrottles.storeLeaderCapacity(ID, *rate)
				case "follower":
					rate = capacities[ID][1]
					prevThrottles.storeFollowerCapacity(ID, *rate)
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

// applyTopicThrottles updates a throttledReplicas for all topics
// undergoing replication, returning a channel of events and []string
// of errors.
// TODO(jamie) review whether the throttled replicas list changes as
// replication finishes; each time the list changes here, we probably
// update the config then propagate a watch to all the brokers in the cluster.
func applyTopicThrottles(throttled topicThrottledReplicas, zk kafkazk.Handler) (chan string, []string) {
	events := make(chan string, len(throttled))
	var errs []string

	for t := range throttled {
		// Generate config.
		config := kafkazk.KafkaConfig{
			Type:    "topic",
			Name:    string(t),
			Configs: []kafkazk.KafkaConfigKV{},
		}

		leaderList := sliceToString(throttled[t]["leaders"])
		if leaderList != "" {
			c := kafkazk.KafkaConfigKV{"leader.replication.throttled.replicas", leaderList}
			config.Configs = append(config.Configs, c)
		}

		followerList := sliceToString(throttled[t]["followers"])
		if followerList != "" {
			c := kafkazk.KafkaConfigKV{"follower.replication.throttled.replicas", followerList}
			config.Configs = append(config.Configs, c)
		}

		// Write the config.
		changes, err := zk.UpdateKafkaConfig(config)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Error setting throttle list on topic %s: %s\n", t, err))
		}

		var anyChanges bool
		for _, changed := range changes {
			if changed {
				anyChanges = true
			}
		}

		if anyChanges {
			// TODO(jamie): we don't use these events yet, but this probably isn't
			// actually the format we want anyway.
			events <- fmt.Sprintf("updated throttled brokers list for %s", string(t))
		}
	}

	close(events)

	return events, errs
}

// removeAllThrottles removes all topic and broker throttle configs.
func removeAllThrottles(zk kafkazk.Handler, params *ReplicationThrottleConfigs) error {
	/****************************
	Clear topic throttle configs.
	****************************/

	// Get all topics.
	topics, err := zk.GetTopics(topicsRegex)
	if err != nil {
		return err
	}

	for _, topic := range topics {
		config := kafkazk.KafkaConfig{
			Type: "topic",
			Name: topic,
			Configs: []kafkazk.KafkaConfigKV{
				kafkazk.KafkaConfigKV{"leader.replication.throttled.replicas", ""},
				kafkazk.KafkaConfigKV{"follower.replication.throttled.replicas", ""},
			},
		}

		// Update the config.
		_, err := zk.UpdateKafkaConfig(config)
		if err != nil {
			log.Printf("Error removing throttle config on topic %s: %s\n", topic, err)
		}

		// Hardcoded sleep to reduce
		// ZK load.
		time.Sleep(250 * time.Millisecond)
	}

	/**********************
	Clear broker throttles.
	**********************/

	// Fetch brokers.
	brokers, errs := zk.GetAllBrokerMeta(false)
	if errs != nil {
		return errs[0]
	}

	var unthrottledBrokers []int

	// Unset throttles.
	for b := range brokers {
		config := kafkazk.KafkaConfig{
			Type: "broker",
			Name: strconv.Itoa(b),
			Configs: []kafkazk.KafkaConfigKV{
				kafkazk.KafkaConfigKV{"leader.replication.throttled.rate", ""},
				kafkazk.KafkaConfigKV{"follower.replication.throttled.rate", ""},
			},
		}

		changed, err := zk.UpdateKafkaConfig(config)
		switch err.(type) {
		case nil:
		case kafkazk.ErrNoNode:
			// We'd get an ErrNoNode here only if the parent path for dynamic broker
			// configs (/config/brokers) if it doesn't exist, which can happen in
			// new clusters that have never had dynamic configs applied. Rather than
			// creating that znode, we'll just ignore errors here; if the znodes
			// don't exist, there's not even config to remove.
		default:
			log.Printf("Error removing throttle on broker %d: %s\n", b, err)
		}

		if changed[0] || changed[1] {
			unthrottledBrokers = append(unthrottledBrokers, b)
			log.Printf("Throttle removed on broker %d\n", b)
		}

		// Hardcoded sleep to reduce ZK load.
		time.Sleep(250 * time.Millisecond)
	}

	// Write event.
	if len(unthrottledBrokers) > 0 {
		m := fmt.Sprintf("Replication throttle removed on the following brokers: %v",
			unthrottledBrokers)
		params.events.Write("Broker replication throttle removed", m)
	}

	// Lazily check if any errors were encountered, return a generic error.
	if err != nil {
		return errors.New("one or more throttles were not cleared")
	}

	// Unset all stored throttle rates.
	for ID := range params.previouslySetThrottles {
		params.previouslySetThrottles[ID] = [2]*float64{}
	}

	return nil
}

// getThrottleOverride gets a throttle override from path p.
func getThrottleOverride(zk kafkazk.Handler, p string) (*ThrottleOverrideConfig, error) {
	c := &ThrottleOverrideConfig{}

	override, err := zk.Get(p)
	if err != nil {
		switch err.(type) {
		case kafkazk.ErrNoNode:
			return c, errNoOverideSet
		default:
			return c, fmt.Errorf("error getting throttle override: %s", err)
		}
	}

	if len(override) == 0 {
		return c, nil
	}

	if err := json.Unmarshal(override, c); err != nil {
		return c, fmt.Errorf("error unmarshalling override config: %s", err)
	}

	return c, nil
}

// setThrottleOverride sets a throttle override to path p.
func setThrottleOverride(zk kafkazk.Handler, p string, c ThrottleOverrideConfig) error {
	d, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("error marshalling override config: %s", err)
	}

	// Check if the path exists.
	exists, _ := zk.Exists(p)
	err = nil

	if exists {
		// Update.
		err = zk.Set(p, string(d))
	} else {
		// Create.
		err = zk.Create(p, string(d))
	}

	if err != nil {
		return fmt.Errorf("error setting throttle override: %s", err)
	}

	return nil
}

// removeThrottleOverride deletes an override at path p.
func removeThrottleOverride(zk kafkazk.Handler, p string) error {
	exists, err := zk.Exists(p)
	if !exists && err == nil {
		return nil
	}

	err = zk.Delete(p)
	if err != nil {
		return fmt.Errorf("error removing throttle override: %s", err)
	}

	return nil
}

// getBrokerOverrides returns a BrokerOverrides populated with all brokers
// with overrides set.
func getBrokerOverrides(zk kafkazk.Handler, p string) (BrokerOverrides, error) {
	overrides := BrokerOverrides{}

	// Get brokers with overrides configured.
	brokers, err := zk.Children(p)
	if err != nil {
		return nil, err
	}

	// Fetch override config for each.
	for _, b := range brokers {
		id, _ := strconv.Atoi(b)
		c := &ThrottleOverrideConfig{}
		brokerZnode := fmt.Sprintf("%s/%d", p, id)

		override, err := zk.Get(brokerZnode)
		if err != nil {
			return overrides, fmt.Errorf("error getting throttle override: %s", err)
		}

		err = json.Unmarshal(override, c)
		if err != nil {
			return overrides, fmt.Errorf("error unmarshalling override config: %s", err)
		}

		overrides[id] = BrokerThrottleOverride{
			ID:      id,
			Applied: false,
			Config:  *c,
		}
	}

	return overrides, nil
}
