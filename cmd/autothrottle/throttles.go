package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
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
	km            kafkametrics.KafkaMetrics
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

// bmapBundle holds several maps
// used as sets. Reduces return params
// for mapsFromReassigments.
type bmapBundle struct {
	src       map[int]interface{}
	dst       map[int]interface{}
	all       map[int]interface{}
	throttled map[string]map[string][]string
}

// lists returns a []int of broker IDs for the
// src, dst and all bmapBundle maps.
func (bm bmapBundle) lists() ([]int, []int, []int) {
	srcBrokers := []int{}
	dstBrokers := []int{}
	for n, m := range []map[int]interface{}{bm.src, bm.dst} {
		for b := range m {
			if n == 0 {
				srcBrokers = append(srcBrokers, b)
			} else {
				dstBrokers = append(dstBrokers, b)
			}
		}
	}

	allBrokers := []int{}
	for b := range bm.all {
		allBrokers = append(allBrokers, b)
	}

	// Sort.
	sort.Ints(srcBrokers)
	sort.Ints(dstBrokers)
	sort.Ints(allBrokers)

	return srcBrokers, dstBrokers, allBrokers
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
	// Get the maps of brokers handling
	// reassignments.
	bmaps, err := mapsFromReassigments(params.reassignments, params.zk)
	if err != nil {
		return err
	}

	// Creates lists from maps.
	srcBrokers, dstBrokers, allBrokers := bmaps.lists()

	log.Printf("Source brokers participating in replication: %v\n", srcBrokers)
	log.Printf("Destination brokers participating in replication: %v\n", dstBrokers)

	/************************
	Determine throttle rates.
	************************/

	// Use the throttle override if set.
	// Otherwise, make a calculation
	// using broker metrics and known
	// capacity values.
	var replicationCapacity float64
	var currThrottle float64
	var useMetrics bool
	var brokerMetrics kafkametrics.BrokerMetrics

	if params.override != "" {
		log.Printf("A throttle override is set: %sMB/s\n", params.override)
		o, _ := strconv.Atoi(params.override)
		replicationCapacity = float64(o)
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
			replicationCapacity = params.limits["minimum"]
		}
	}

	// If we're using metrics and successfully
	// fetched them, determine a tvalue based on
	// the most-utilized path.
	if useMetrics && err == nil {
		replicationCapacity, currThrottle, err = repCapacityByMetrics(params, bmaps, brokerMetrics)
		if err != nil {
			return err
		}

		// Check if the delta between the newly calculated
		// throttle and the previous throttle exceeds the
		// ChangeThreshold param.
		d := math.Abs((currThrottle - replicationCapacity) / currThrottle * 100)
		if d < Config.ChangeThreshold {
			log.Printf("Proposed throttle is within %.2f%% of the previous throttle "+
				"(below %.2f%% threshold), skipping throttle update\n",
				d, Config.ChangeThreshold)
			return nil
		}
	}

	// Get a rate string based on the final tvalue.
	rateString := fmt.Sprintf("%.0f", replicationCapacity*1000000.00)

	/**************************
	Set topic throttle configs.
	**************************/

	errs := applyTopicThrottles(bmaps.throttled, params.zk)
	for _, e := range errs {
		log.Println(e)
	}

	/***************************
	Set broker throttle configs.
	***************************/
	errs = applyBrokerThrottles(bmaps.all,
		rateString,
		replicationCapacity,
		params.throttles,
		params.zk)
	for _, e := range errs {
		log.Println(e)
	}

	/***********
	Log success.
	***********/

	// Write event.
	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("Replication throttle of %0.2fMB/s set on the following brokers: %v\n",
		replicationCapacity, allBrokers))
	b.WriteString(fmt.Sprintf("Topics currently undergoing replication: %v", params.topics))
	params.events.Write("Broker replication throttle set", b.String())

	return nil
}

// mapsFromReassigments takes a kafakzk.Reassignments and returns
// a bmapBundle, which includes a broker list for source, destination,
// and all brokers handling any ongoing reassignments. Additionally, a map
// of throttled replicas by topic is included.
func mapsFromReassigments(r kafkazk.Reassignments, zk *kafkazk.ZK) (bmapBundle, error) {
	lb := bmapBundle{
		// Maps of src and dst brokers
		// used as sets.
		src: map[int]interface{}{},
		dst: map[int]interface{}{},
		all: map[int]interface{}{},
		// A map for each topic with a list throttled
		// leaders and followers. This is used to write
		// the topic config throttled brokers lists. E.g.:
		// map[topic]map[leaders]["0:1001", "1:1002"]
		// map[topic]map[followers]["2:1003", "3:1004"]
		throttled: map[string]map[string][]string{},
	}

	// Get topic data for each topic
	// undergoing a reassignment.
	for t := range r {
		lb.throttled[t] = make(map[string][]string)
		lb.throttled[t]["leaders"] = []string{}
		lb.throttled[t]["followers"] = []string{}
		tstate, err := zk.GetTopicState(t)
		if err != nil {
			errS := fmt.Sprintf("Error fetching topic data: %s\n", err.Error())
			return lb, errors.New(errS)
		}

		// For each partition in the current topic
		// state, check if this partition exists
		// in the reassignments data. If so, the
		// brokers from the current state are src
		// and those in the reassignments are dst.
		for p := range tstate.Partitions {
			part, _ := strconv.Atoi(p)
			if reassigning, exists := r[t][part]; exists {
				// Src.
				for _, b := range tstate.Partitions[p] {
					// Add to the maps.
					lb.src[b] = nil
					// Append to the throttle list.
					lb.throttled[t]["leaders"] = append(lb.throttled[t]["leaders"], fmt.Sprintf("%d:%d", part, b))
				}
				// Dst.
				for _, b := range reassigning {
					lb.dst[b] = nil
					lb.throttled[t]["followers"] = append(lb.throttled[t]["followers"], fmt.Sprintf("%d:%d", part, b))
				}
			}
		}
	}

	lb.all = mergeMaps(lb.src, lb.dst)

	return lb, nil
}

// repCapacityByMetrics finds the most constrained src broker and returns
// a calculated replication capacity, the currently applied throttle and
// and any errors if encountered.
func repCapacityByMetrics(rtm *ReplicationThrottleMeta, bmb bmapBundle, bm kafkametrics.BrokerMetrics) (float64, float64, error) {
	// Map src/dst broker IDs to a *ReassigningBrokers.
	participatingBrokers := &ReassigningBrokers{}

	// Source brokers.
	for b := range bmb.src {
		if broker, exists := bm[b]; exists {
			participatingBrokers.Src = append(participatingBrokers.Src, broker)
		} else {
			return 0.00, 0.00, fmt.Errorf("Broker %d not found in broker metrics", b)
		}
	}

	// Destination brokers.
	for b := range bmb.dst {
		if broker, exists := bm[b]; exists {
			participatingBrokers.Dst = append(participatingBrokers.Dst, broker)
		} else {
			return 0.00, 0.00, fmt.Errorf("Broker %d not found in broker metrics", b)
		}
	}

	// Get the most constrained src broker and
	// its current throttle, if applied.
	constrainingSrc := participatingBrokers.highestSrcNetTX()
	currThrottle, exists := rtm.throttles[constrainingSrc.ID]
	if !exists {
		currThrottle = 0.00
	}

	replicationCapacity, err := rtm.limits.headroom(constrainingSrc, currThrottle)
	if err != nil {
		return 0.00, 0.00, err
	}

	log.Printf("Most utilized source broker: "+
		"[%d] net tx of %.2fMB/s (over %ds) with an existing throttle rate of %.2fMB/s\n",
		constrainingSrc.ID, constrainingSrc.NetTX, Config.MetricsWindow, currThrottle)

	log.Printf("Replication capacity (based on a %.0f%% max free capacity utilization): %0.2fMB/s\n",
		rtm.limits["maximum"], replicationCapacity)

	return replicationCapacity, currThrottle, nil
}

// applyTopicThrottles updates the throttled brokers list for
// all topics undergoing replication.
// TODO we need to avoid continously resetting this to reduce writes
// to ZK and subsequent config propagations to all brokers.
// We can either:
// - Ensure throttle lists are sorted so that if we provide the
// same list each iteration that it results in a no-op in the backend.
// - Keep track of topics that have already had a throttle list
// written and assume that it's not going to change
// (a throttle list is applied) when a topic is initially set
// for reassignment and cleared by autothrottle as soon as
// the reassignment is done).
func applyTopicThrottles(throttled map[string]map[string][]string, zk *kafkazk.ZK) []string {
	var errs []string

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
		_, err := zk.UpdateKafkaConfig(config)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Error setting throttle list on topic %s: %s\n", t, err))
		}
	}

	return errs
}

// applyBrokerThrottles take a list of brokers, a replication throttle rate string,
// rate, map of applied throttles, and zk *kafkazk.ZK zookeeper client.
// For each broker, the throttle rate is applied and if successful, the rate
// is stored in the throttles map for future reference.
func applyBrokerThrottles(bs map[int]interface{}, ratestr string, r float64, ts map[int]float64, zk *kafkazk.ZK) []string {
	var errs []string

	// Generate a broker throttle config.
	for b := range bs {
		config := kafkazk.KafkaConfig{
			Type: "broker",
			Name: strconv.Itoa(b),
			Configs: [][2]string{
				[2]string{"leader.replication.throttled.rate", ratestr},
				[2]string{"follower.replication.throttled.rate", ratestr},
			},
		}

		// Write the throttle config.
		changed, err := zk.UpdateKafkaConfig(config)
		if err != nil {
			errs = append(errs, fmt.Sprintf("Error setting throttle on broker %d: %s\n", b, err))
		}

		if changed {
			// Store the configured rate.
			ts[b] = r
			log.Printf("Updated throttle to %0.2fMB/s on broker %d\n", r, b)
		}

		// Hard coded sleep to reduce
		// ZK load.
		time.Sleep(500 * time.Millisecond)
	}

	return errs
}

// removeAllThrottles removes all topic and
// broker throttle configs.
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
		m := fmt.Sprintf("Replication throttle removed on the following brokers: %v",
			unthrottledBrokers)
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

// mergeMaps takes two maps and merges them.
func mergeMaps(a map[int]interface{}, b map[int]interface{}) map[int]interface{} {
	m := map[int]interface{}{}

	// Merge from each.
	for k := range a {
		m[k] = nil
	}

	for k := range b {
		m[k] = nil
	}

	return m
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
