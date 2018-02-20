package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/DataDog/topicmappr/kafkametrics"
	"github.com/DataDog/topicmappr/kafkazk"
)

// ReplicationThrottleMeta holds all types
// needed to call the updateReplicationThrottle func.
type ReplicationThrottleMeta struct {
	topics        []string
	reassignments kafkazk.Reassignments
	zk            *kafkazk.ZK
	km            *kafkametrics.KafkaMetrics
	override      string
	events        *EventGenerator
	// Map of broker ID to last set throttle rate.
	throttles map[int]float64
	limits    Limits
}

// ReassigningBrokers is a list of brokers
// with a throttle applied for an ongoing
// reassignment.
type ReassigningBrokers struct {
	Src []*kafkametrics.Broker
	Dst []*kafkametrics.Broker
}

// highestSrcNetTX takes a ReassigningBrokers and returns
// the leader with the highest outbound network throughput.
func (t ReassigningBrokers) highestSrcNetTX() *kafkametrics.Broker {
	hwm := 0.00
	var broker *kafkametrics.Broker

	for _, b := range t.Src {
		if b.NetTX > hwm {
			hwm = b.NetTX
			broker = b
		}
	}

	return broker
}

// updateReplicationThrottle takes a ReplicationThrottleMeta
// that holds topics being replicated, any clients, throttle override params,
// and other required metadata.
// Metrics for brokers participating in any ongoing replication
// are fetched to determine replication headroom.
// The replication throttle is then adjusted accordingly.
// If a non-empty override is provided, that static value is used instead
// of a dynamically determined value.
func updateReplicationThrottle(params *ReplicationThrottleMeta) error {
	// Maps of src and dst brokers
	// used as sets.
	srcBrokers := map[int]interface{}{}
	dstBrokers := map[int]interface{}{}
	allBrokers := map[int]interface{}{}

	// A map for each topic with a list throttled
	// leaders and followers. This is used to write
	// the topic config throttled brokers lists. E.g.:
	// map[topic]map[leaders]["0:1001", "1:1002"]
	// map[topic]map[followers]["2:1003", "3:1004"]
	throttled := map[string]map[string][]string{}

	// Get topic data for each topic
	// undergoing a reassignment.
	for t := range params.reassignments {
		throttled[t] = make(map[string][]string)
		throttled[t]["leaders"] = []string{}
		throttled[t]["followers"] = []string{}
		tstate, err := params.zk.GetTopicState(t)
		if err != nil {
			errS := fmt.Sprintf("Error fetching topic data: %s\n", err.Error())
			return errors.New(errS)
		}

		// For each partition in the current topic
		// state, check if this partition exists
		// in the reassignments data. If so, the
		// brokers from the current state are src
		// and those in the reassignments are dst.
		for p := range tstate.Partitions {
			part, _ := strconv.Atoi(p)
			if reassigning, exists := params.reassignments[t][part]; exists {
				// Src.
				for _, b := range tstate.Partitions[p] {
					// Add to the maps.
					srcBrokers[b] = nil
					allBrokers[b] = nil
					// Append to the throttle list.
					throttled[t]["leaders"] = append(throttled[t]["leaders"], fmt.Sprintf("%d:%d", part, b))
				}
				// Dst.
				for _, b := range reassigning {
					dstBrokers[b] = nil
					allBrokers[b] = nil
					throttled[t]["followers"] = append(throttled[t]["followers"], fmt.Sprintf("%d:%d", part, b))
				}
			}
		}
	}

	// Creates lists from maps.
	srcBrokersList := []int{}
	dstBrokersList := []int{}
	for n, m := range []map[int]interface{}{srcBrokers, dstBrokers} {
		for b := range m {
			if n == 0 {
				srcBrokersList = append(srcBrokersList, b)
			} else {
				dstBrokersList = append(dstBrokersList, b)
			}
		}
	}

	allBrokersList := []int{}
	for b := range allBrokers {
		allBrokersList = append(allBrokersList, b)
	}

	// Sort.
	sort.Ints(srcBrokersList)
	sort.Ints(dstBrokersList)
	sort.Ints(allBrokersList)

	log.Printf("Source brokers participating in replication: %v\n", srcBrokersList)
	log.Printf("Destination brokers participating in replication: %v\n", dstBrokersList)

	/************************
	Determine throttle rates.
	************************/

	// Use the throttle override if set.
	// Otherwise, use metrics.
	var tvalue float64
	var replicationHeadRoom float64
	var useMetrics bool
	var brokerMetrics kafkametrics.BrokerMetrics
	var err error

	if params.override != "" {
		log.Printf("A throttle override is set: %sMB/s\n", params.override)
		o, _ := strconv.Atoi(params.override)
		tvalue = float64(o) * 1000000.00
		// For log output.
		replicationHeadRoom = float64(o)
	} else {
		useMetrics = true
		// Get broker metrics.
		brokerMetrics, err = params.km.GetMetrics()
		if err != nil {
			// If there was a error fetching metrics,
			// revert to the minimum replication rate
			// configured.
			log.Println(err)
			log.Printf("Reverting to minimum throttle of %.2fMB/s\n", params.limits["minimum"])
			tvalue = params.limits["minimum"] * 1000000.00
		}
	}

	// If we're using metrics and successfully
	// fetched them, determine a tvalue based on
	// the most-utilized path.
	if useMetrics && err == nil {
		// Map src/dst broker IDs to a *ReassigningBrokers.
		participatingBrokers := &ReassigningBrokers{}

		// Source brokers.
		for b := range srcBrokers {
			if broker, exists := brokerMetrics[b]; exists {
				participatingBrokers.Src = append(participatingBrokers.Src, broker)
			} else {
				return fmt.Errorf("Broker %d not found in broker metrics", b)
			}
		}

		// Destination brokers.
		for b := range dstBrokers {
			if broker, exists := brokerMetrics[b]; exists {
				participatingBrokers.Dst = append(participatingBrokers.Dst, broker)
			} else {
				return fmt.Errorf("Broker %d not found in broker metrics", b)
			}
		}

		// Get the most constrained src broker and
		// its current throttle, if applied.
		constrainingSrc := participatingBrokers.highestSrcNetTX()
		currThrottle, exists := params.throttles[constrainingSrc.ID]
		if !exists {
			currThrottle = 0.0
		}

		replicationHeadRoom, err = params.limits.headroom(constrainingSrc, currThrottle)
		if err != nil {
			return err
		}

		tvalue = replicationHeadRoom * 1000000.00

		log.Printf("Most utilized source broker: [%d] outbound net tx of %.2fMB/s (over %ds) with an existing throttle rate of %2.fMB/s\n",
			constrainingSrc.ID, constrainingSrc.NetTX, Config.MetricsWindow, replicationHeadRoom)
		log.Printf("Replication headroom (based on a %.0f%% max free capacity utilization): %.2fMB/s\n", params.limits["maximum"], replicationHeadRoom)
	}

	// Get a rate string based on the
	// final tvalue.
	rateString := fmt.Sprintf("%.0f", tvalue)

	/**************************
	Set topic throttle configs.
	**************************/

	// Update the throttled brokers list for
	// all topics undergoing replication.
	// TODO we need to avoid continously resetting
	// this to reduce writes to ZK and subsequent
	// config propagations to all brokers. We can either:
	// - Ensure throttle lists are sorted so that
	// if we provide the same list each iteration
	// that it results in a no-op in the backend.
	// - Keep track of topics that have already had
	// a throttle list written and assume that it's
	// not going to change (a throttle list is applied)
	// when a topic is initially set for reassignment and
	// cleared by autothrottle as soon as the reassignment
	// is done).
	for t := range throttled {
		// Generate config.
		config := kafkazk.KafkaConfig{
			Type:    "topic",
			Name:    t,
			Configs: [][2]string{},
		}

		leaderList := sliceToString(throttled[t]["leaders"])
		if leaderList != "" {
			c := [2]string{"leader.replication.throttled.replicas", leaderList}
			config.Configs = append(config.Configs, c)
		}

		followerList := sliceToString(throttled[t]["followers"])
		if followerList != "" {
			c := [2]string{"follower.replication.throttled.replicas", followerList}
			config.Configs = append(config.Configs, c)
		}

		// Write the config.
		_, err := params.zk.UpdateKafkaConfig(config)
		if err != nil {
			log.Printf("Error setting throttle list on topic %s: %s\n", t, err)
		}
	}

	/***************************
	Set broker throttle configs.
	***************************/

	// Generate a broker throttle config.
	for b := range allBrokers {
		config := kafkazk.KafkaConfig{
			Type: "broker",
			Name: strconv.Itoa(b),
			Configs: [][2]string{
				[2]string{"leader.replication.throttled.rate", rateString},
				[2]string{"follower.replication.throttled.rate", rateString},
			},
		}

		// Write the throttle config.
		changed, err := params.zk.UpdateKafkaConfig(config)
		if err != nil {
			log.Printf("Error setting throttle on broker %d: %s\n", b, err)
		}

		if changed {
			// Store the configured rate.
			// We used the replicationHeadRoom, which
			// is the tvalue in MB.
			params.throttles[b] = replicationHeadRoom
			log.Printf("Updated throttle to %.2fMB/s on broker %d\n", replicationHeadRoom, b)
		}

		// Hard coded sleep to reduce
		// ZK load.
		time.Sleep(500 * time.Millisecond)
	}

	// Write event.
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("Replication throttle of %.2fMB/s set on the following brokers: %v\n",
		replicationHeadRoom, allBrokersList))
	b.WriteString(fmt.Sprintf("Topics currently undergoing replication: %v", params.topics))
	params.events.Write("Broker replication throttle set", b.String())

	return nil
}

func removeAllThrottles(zk *kafkazk.ZK, params *ReplicationThrottleMeta) error {
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
			Configs: [][2]string{
				[2]string{"leader.replication.throttled.replicas", ""},
				[2]string{"follower.replication.throttled.replicas", ""},
			},
		}

		// Update the config.
		_, err := zk.UpdateKafkaConfig(config)
		if err != nil {
			log.Printf("Error removing throttle config on topic %s: %s\n", topic, err)
		}

		// Hardcoded sleep to reduce
		// ZK load.
		time.Sleep(500 * time.Millisecond)
	}

	/**********************
	Clear broker throttles.
	**********************/

	// Fetch brokers.
	brokers, err := zk.GetAllBrokerMeta()
	if err != nil {
		return err
	}

	var unthrottledBrokers []int

	// Unset throttles.
	for b := range brokers {
		config := kafkazk.KafkaConfig{
			Type: "broker",
			Name: strconv.Itoa(b),
			Configs: [][2]string{
				[2]string{"leader.replication.throttled.rate", ""},
				[2]string{"follower.replication.throttled.rate", ""},
			},
		}

		changed, err := zk.UpdateKafkaConfig(config)
		if err != nil {
			log.Printf("Error removing throttle on broker %d: %s\n", b, err)
		}

		if changed {
			unthrottledBrokers = append(unthrottledBrokers, b)
			log.Printf("Throttle removed on broker %d\n", b)
		}

		// Hardcoded sleep to reduce
		// ZK load.
		time.Sleep(500 * time.Millisecond)
	}

	// Write event.
	if len(unthrottledBrokers) > 0 {
		m := fmt.Sprintf("Replication throttle removed on the following brokers: %v", unthrottledBrokers)
		params.events.Write("Broker replication throttle removed", m)
	}

	// Lazily check if any
	// errors were encountered,
	// return a generic error.
	if err != nil {
		return errors.New("one or more throttles were not cleared")
	}

	// Unset all stored throttle rates.
	for b := range params.throttles {
		params.throttles[b] = 0.0
	}

	return nil
}

// sliceToString takes []string and
// returns a comma delimited string.
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
