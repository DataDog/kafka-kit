package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/kafka-kit/kafkametrics"
	"github.com/DataDog/kafka-kit/kafkazk"
)

// ReplicationThrottleConfigs holds all the data/types needed to
// call updateReplicationThrottle.
type ReplicationThrottleConfigs struct {
	topics                 []string // TODO(jamie): probably don't even need this anymore.
	reassignments          kafkazk.Reassignments
	zk                     kafkazk.Handler
	km                     kafkametrics.Handler
	overrideRate           int
	events                 *DDEventWriter
	previouslySetThrottles replicationCapacityByBroker
	limits                 Limits
	failureThreshold       int
	failures               int
}

// ThrottleOverrideConfig holds throttle
// override configurations.
type ThrottleOverrideConfig struct {
	// Rate in MB.
	Rate int `json:"rate"`
	// Whether the override rate should be
	// removed when the current reassignments finish.
	AutoRemove bool `json:"autoremove"`
}

// Failure increments the failures count and returns true if the
// count exceeds the failures threshold.
func (r *ReplicationThrottleConfigs) Failure() bool {
	r.failures++

	if r.failures > r.failureThreshold {
		return true
	}

	return false
}

// ResetFailures resets the failures count.
func (r *ReplicationThrottleConfigs) ResetFailures() {
	r.failures = 0
}

// ThrottledBrokers is a list of brokers with a throttle applied
// for an ongoing reassignment.
type ThrottledBrokers struct {
	Src []*kafkametrics.Broker
	Dst []*kafkametrics.Broker
}

// replicationCapacityByBroker is a mapping of broker ID to capacity.
type replicationCapacityByBroker map[int]throttleByRole

// throttleByRole represents a source and destination throttle rate in respective
// order to index; position 0 is a source rate, position 1 is a dest. rate.
// A nil value means that no throttle was needed according to the broker's role
// in the replication, as opposed to 0.00 which explicitly describes the
// broker as having no spare capacity available for replication.
type throttleByRole [2]*float64

func (r replicationCapacityByBroker) storeLeaderCapacity(id int, c float64) {
	if _, exist := r[id]; !exist {
		r[id] = [2]*float64{}
	}

	a := r[id]
	a[0] = &c
	r[id] = a
}

func (r replicationCapacityByBroker) storeFollowerCapacity(id int, c float64) {
	if _, exist := r[id]; !exist {
		r[id] = [2]*float64{}
	}

	a := r[id]
	a[1] = &c
	r[id] = a
}

func (r replicationCapacityByBroker) setAllRatesWithDefault(ids []int, rate float64) {
	for _, id := range ids {
		r.storeLeaderCapacity(id, rate)
		r.storeFollowerCapacity(id, rate)
	}
}

// brokerChangeEvent is the message type returned in the events channel
// from the applyBrokerThrottles func.
type brokerChangeEvent struct {
	id   int
	role string
	rate float64
}

// updateReplicationThrottle takes a ReplicationThrottleConfigs that holds
// topics being replicated, any ZooKeeper/other clients, throttle override
// params, and other required metadata. Metrics for brokers participating in
// any ongoing replication are fetched to determine replication headroom.
// The replication throttle is then adjusted accordingly. If a non-empty
// override is provided, that static value is used instead of a dynamically
// determined value.
func updateReplicationThrottle(params *ReplicationThrottleConfigs) error {
	// Get the maps of brokers handling reassignments.
	reassigning, err := getReassigningBrokers(params.reassignments, params.zk)
	if err != nil {
		return err
	}

	// Creates lists from maps.
	srcBrokers, dstBrokers, allBrokers := reassigning.lists()

	log.Printf("Source brokers participating in replication: %v\n", srcBrokers)
	log.Printf("Destination brokers participating in replication: %v\n", dstBrokers)

	// Determine throttle rates.

	// Use the throttle override if set. Otherwise, make a calculation
	// using broker metrics and configured capacity values.
	var capacities = make(replicationCapacityByBroker)
	var brokerMetrics kafkametrics.BrokerMetrics
	var rateOverride bool
	var inFailureMode bool
	var metricErrs []error

	if params.overrideRate != 0 {
		log.Printf("A throttle override is set: %dMB/s\n", params.overrideRate)
		rateOverride = true

		capacities.setAllRatesWithDefault(allBrokers, float64(params.overrideRate))
	}

	if !rateOverride {
		// Get broker metrics.
		brokerMetrics, metricErrs = params.km.GetMetrics()
		// Even if errors are returned, we can still proceed as long as we have
		// complete metrics data for all target brokers. If we have broker
		// metrics for all target brokers, we can ignore any errors.
		if metricErrs != nil {
			if brokerMetrics == nil || incompleteBrokerMetrics(allBrokers, brokerMetrics) {
				inFailureMode = true
			}
		}
	}

	// If we cannot proceed normally due to missing/partial metrics data,
	// check what failure iteration we're in. If we're above the threshold,
	// revert to the minimum rate, otherwise retain the previous rate.
	if inFailureMode {
		log.Printf("Errors fetching metrics: %s\n", metricErrs)

		// Increment and check our failure count against the configured threshold.
		over := params.Failure()

		// If we're not over the threshold, return and just retain previous throttles.
		if !over {
			log.Printf("Metrics fetch failure count %d doesn't exeed threshold %d, retaining previous throttle\n",
				params.failures, params.failureThreshold)
			return nil
		}

		// We're over the threshold; failback to the configured minimum.
		log.Printf("Metrics fetch failure count %d exceeds threshold %d, reverting to min-rate %.2fMB/s\n",
			params.failures, params.failureThreshold, params.limits["minimum"])

		// Set the failback rate.
		capacities.setAllRatesWithDefault(allBrokers, params.limits["minimum"])
	}

	// Reset the failure counter. We may have incremented in past iterations,
	// but if we're here now, we can reset the count.
	if !inFailureMode {
		params.ResetFailures()
	}

	// If there's no override set and we're not in a failure mode, apply
	// the calculated throttles.
	if !rateOverride && !inFailureMode {
		capacities, err = brokerReplicationCapacities(params, reassigning, brokerMetrics)
		if err != nil {
			return err
		}
	}

	//Set broker throttle configs.
	events, errs := applyBrokerThrottles(reassigning.all, capacities, params.previouslySetThrottles, params.limits, params.zk)
	for _, e := range errs {
		log.Println(e)
	}

	//Set topic throttle configs.
	_, errs = applyTopicThrottles(reassigning.throttledReplicas, params.zk)
	for _, e := range errs {
		log.Println(e)
	}

	// Append broker throttle info to event.
	var b bytes.Buffer
	if len(events) > 0 {
		b.WriteString("Replication throttles changes for brokers [ID, role, rate]: ")

		for e := range events {
			b.WriteString(fmt.Sprintf("[%d, %s, %.2f], ", e.id, e.role, e.rate))
		}

		b.WriteString("\n")
	}

	// Append topic stats to event.
	b.WriteString(fmt.Sprintf("Topics currently undergoing replication: %v", params.topics))

	// Ship it.
	params.events.Write("Broker replication throttle set", b.String())

	return nil
}

// getReassigningBrokers takes a kafakzk.Reassignments and returns a reassigningBrokers,
// which includes a broker list for source, destination, and all brokers
// handling any ongoing reassignments. Additionally, a map of throttled
// replicas by topic is included.
func getReassigningBrokers(r kafkazk.Reassignments, zk kafkazk.Handler) (reassigningBrokers, error) {
	lb := reassigningBrokers{
		// Maps of src and dst brokers used as sets.
		src: map[int]struct{}{},
		dst: map[int]struct{}{},
		all: map[int]struct{}{},
		// A map for each topic with a list throttled leaders and followers.
		// This is used to write the topic config throttled brokers lists.
		throttledReplicas: topicThrottledReplicas{},
	}

	// Get topic data for each topic undergoing a reassignment.
	for t := range r {
		topic := topic(t)
		lb.throttledReplicas[topic] = make(throttled)
		lb.throttledReplicas[topic]["leaders"] = []string{}
		lb.throttledReplicas[topic]["followers"] = []string{}
		tstate, err := zk.GetTopicStateISR(t)
		if err != nil {
			return lb, fmt.Errorf("Error fetching topic data: %s", err.Error())
		}

		// For each partition, compare the current ISR leader to the brokers being
		// assigned in the reassignments. The current leaders will be sources,
		// new brokers in the assignment list will be destinations.
		for p := range tstate {
			partn, _ := strconv.Atoi(p)
			if reassigning, exists := r[t][partn]; exists {
				// Source brokers.
				leader := tstate[p].Leader
				// In offline partitions, the leader value is set to -1. Skip.
				if leader != -1 {
					lb.src[leader] = struct{}{}
					// Append to the throttle list.
					leaders := lb.throttledReplicas[topic]["leaders"]
					lb.throttledReplicas[topic]["leaders"] = append(leaders, fmt.Sprintf("%d:%d", partn, leader))
				}

				// Dest brokers.
				for _, b := range reassigning {
					if b != leader && b != -1 {
						lb.dst[b] = struct{}{}
						followers := lb.throttledReplicas[topic]["followers"]
						lb.throttledReplicas[topic]["followers"] = append(followers, fmt.Sprintf("%d:%d", partn, b))
					}
				}
			}
		}
	}

	lb.all = mergeMaps(lb.src, lb.dst)

	return lb, nil
}

// brokerReplicationCapacities traverses the list of all brokers participating
// in the reassignment. For each broker, it determines whether the broker is
// a leader (source) or a follower (destination), and calculates an throttle
// accordingly, returning a replicationCapacityByBroker and error.
func brokerReplicationCapacities(rtc *ReplicationThrottleConfigs, reassigning reassigningBrokers, bm kafkametrics.BrokerMetrics) (replicationCapacityByBroker, error) {
	capacities := replicationCapacityByBroker{}

	// For each broker, check whether the it's a source and/or destination,
	// calculating and storing the throttle for each.
	for ID := range reassigning.all {
		capacities[ID] = throttleByRole{}
		// Get the kafkametrics.Broker from the ID, check that
		// it exists in the kafkametrics.BrokerMetrics.
		broker, exists := bm[ID]
		if !exists {
			return capacities, fmt.Errorf("Broker %d not found in broker metrics", ID)
		}

		// We're traversing brokers from 'all', but a broker's role is either
		// a leader, a follower, or both. If it's exclusively one, we can
		// skip throttle computation for that role type for the broker.
		for i, role := range []replicaType{"leader", "follower"} {
			var isInRole bool
			switch role {
			case "leader":
				_, isInRole = reassigning.src[ID]
			case "follower":
				_, isInRole = reassigning.dst[ID]
			}

			if !isInRole {
				continue
			}

			var currThrottle float64
			// Check if a throttle rate was previously set.
			throttles, exists := rtc.previouslySetThrottles[ID]
			if exists && throttles[i] != nil {
				currThrottle = *throttles[i]
			} else {
				// If not, we assume that none of the current bandwidth is being
				// consumed from reassignment bandwidth.
				currThrottle = 0.00
			}

			// Calc. and store the rate.
			rate, err := rtc.limits.replicationHeadroom(broker, role, currThrottle)
			if err != nil {
				return capacities, err
			}

			switch role {
			case "leader":
				capacities.storeLeaderCapacity(ID, rate)
			case "follower":
				capacities.storeFollowerCapacity(ID, rate)
			}
		}
	}

	return capacities, nil
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

func getThrottleOverride(zk kafkazk.Handler, p string) (*ThrottleOverrideConfig, error) {
	c := &ThrottleOverrideConfig{}

	override, err := zk.Get(p)
	if err != nil {
		return c, fmt.Errorf("Error getting throttle override: %s", err)
	}

	if len(override) == 0 {
		return c, nil
	}

	if err := json.Unmarshal(override, c); err != nil {
		return c, fmt.Errorf("Error unmarshalling override config: %s", err)
	}

	return c, nil
}

func setThrottleOverride(zk kafkazk.Handler, p string, c ThrottleOverrideConfig) error {
	d, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("Error marshalling override config: %s", err)
	}

	err = zk.Set(p, string(d))
	if err != nil {
		return fmt.Errorf("Error setting throttle override: %s", err)
	}

	return nil
}

// mergeMaps takes two maps and merges them.
func mergeMaps(a map[int]struct{}, b map[int]struct{}) map[int]struct{} {
	m := map[int]struct{}{}

	// Merge from each.
	for k := range a {
		m[k] = struct{}{}
	}

	for k := range b {
		m[k] = struct{}{}
	}

	return m
}

// sliceToString takes []string and returns a comma delimited string.
func sliceToString(l []string) string {
	var b bytes.Buffer
	for n, i := range l {
		b.WriteString(i)
		if n < len(l)-1 {
			b.WriteString(",")
		}
	}

	return b.String()
}

func roleFromIndex(i int) string {
	if i == 0 {
		return "leader"
	}

	return "follower"
}
