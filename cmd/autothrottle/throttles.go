package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/DataDog/kafka-kit/kafkametrics"
	"github.com/DataDog/kafka-kit/kafkazk"
)

// ReplicationThrottleMeta holds all types
// needed to call the updateReplicationThrottle func.
type ReplicationThrottleMeta struct {
	topics        []string
	reassignments kafkazk.Reassignments
	zk            kafkazk.Handler
	km            kafkametrics.Handler
	overrideRate  int
	events        *EventGenerator
	// Map of broker ID to last set throttle rate.
	throttles        map[int]float64
	limits           Limits
	failureThreshold int
	failures         int
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

// Failure increments the failures count
// and returns true if the count exceeds
// the failures threshold.
func (r *ReplicationThrottleMeta) Failure() bool {
	r.failures++

	if r.failures > r.failureThreshold {
		return true
	}

	return false
}

// ResetFailures resets the failures count.
func (r *ReplicationThrottleMeta) ResetFailures() {
	r.failures = 0
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
// that holds topics being replicated, any ZooKeeper/other clients, throttle override
// params, and other required metadata.
// Metrics for brokers participating in any ongoing replication are fetched to
// determine replication headroom. The replication throttle is then adjusted
// accordingly. If a non-empty override is provided, that static value is
// used instead of a dynamically determined value.
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

	// Use the throttle override if set. Otherwise,
	// make a calculation using broker metrics and known
	// capacity values.
	var replicationCapacity float64
	var currThrottle float64
	var useMetrics bool
	var brokerMetrics kafkametrics.BrokerMetrics
	var metricErrs []error
	var inFailureMode bool

	if params.overrideRate != 0 {
		log.Printf("A throttle override is set: %dMB/s\n", params.overrideRate)
		replicationCapacity = float64(params.overrideRate)
	} else {
		useMetrics = true

		// Get broker metrics.
		brokerMetrics, metricErrs = params.km.GetMetrics()
		// Even if errors are returned, we can still
		// proceed as long as we have complete metrics
		// data for all target brokers. If we have broker
		// metrics for all target brokers, we can ignore
		// any errors.
		if metricErrs != nil {
			if brokerMetrics == nil || incompleteBrokerMetrics(allBrokers, brokerMetrics) {
				inFailureMode = true
			}
		}

		// If we cannot proceed normally due to missing/partial
		// metrics data, check what failure iteration we're in.
		// If we're above the threshold, revert to the minimum
		// rate, otherwise retain the previous rate.
		if inFailureMode {
			log.Printf("Errors fetching metrics: %s\n", metricErrs)
			// Check our failures against the
			// configured threshold.
			over := params.Failure()
			// Over threshold. Set replicationCapacity which will be
			// applied in the apply throttles stage.
			if over {
				log.Printf("Metrics fetch failure count %d exceeds threshold %d, reverting to min-rate %.2fMB/s\n",
					params.failures, params.failureThreshold, params.limits["minimum"])
				replicationCapacity = params.limits["minimum"]
				// Not over threshold. Return and retain previous throttle.
			} else {
				log.Printf("Metrics fetch failure count %d doesn't exceed threshold %d, retaining previous throttle\n",
					params.failures, params.failureThreshold)
				return nil
			}
		} else {
			// Reset the failure counter
			// in case it was incremented
			// in previous iterations.
			params.ResetFailures()
		}
	}

	// If we're using metrics and successfully
	// fetched them, determine a tvalue based on
	// the most-utilized path.
	if useMetrics && !inFailureMode {
		var e string
		replicationCapacity, currThrottle, e, err = repCapacityByMetrics(params, bmaps, brokerMetrics)
		if err != nil {
			return err
		}

		log.Println(e)
		log.Printf("Replication capacity (based on a %.0f%% max free capacity utilization): %0.2fMB/s\n",
			params.limits["maximum"], replicationCapacity)

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
func mapsFromReassigments(r kafkazk.Reassignments, zk kafkazk.Handler) (bmapBundle, error) {
	lb := bmapBundle{
		// Maps of src and dst brokers
		// used as sets.
		src: map[int]struct{}{},
		dst: map[int]struct{}{},
		all: map[int]struct{}{},
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
		tstate, err := zk.GetTopicStateISR(t)
		if err != nil {
			return lb, fmt.Errorf("Error fetching topic data: %s", err.Error())
		}

		// For each partition, compare the current
		// ISR leader to the brokers being assigned
		// in the reassignments. The current leaders
		// will be sources, new brokers in the assignment
		// list will be destinations.
		for p := range tstate {
			part, _ := strconv.Atoi(p)
			if reassigning, exists := r[t][part]; exists {
				// Source brokers.
				leader := tstate[p].Leader
				// In offline partitions, the leader value is set to -1. Skip.
				if leader != -1 {
					lb.src[leader] = struct{}{}
					// Append to the throttle list.
					lb.throttled[t]["leaders"] = append(lb.throttled[t]["leaders"], fmt.Sprintf("%d:%d", part, leader))
				}

				// Dest brokers.
				for _, b := range reassigning {
					if b != leader && b != -1 {
						lb.dst[b] = struct{}{}
						lb.throttled[t]["followers"] = append(lb.throttled[t]["followers"], fmt.Sprintf("%d:%d", part, b))
					}
				}
			}
		}
	}

	lb.all = mergeMaps(lb.src, lb.dst)

	return lb, nil
}

// repCapacityByMetrics finds the most constrained src broker and returns
// a calculated replication capacity, the currently applied throttle, a slice
// of event strings and any errors if encountered.
func repCapacityByMetrics(rtm *ReplicationThrottleMeta, bmb bmapBundle, bm kafkametrics.BrokerMetrics) (float64, float64, string, error) {
	// Map src/dst broker IDs to a *ReassigningBrokers.
	participatingBrokers := &ReassigningBrokers{}

	var event string

	// Source brokers.
	for b := range bmb.src {
		if broker, exists := bm[b]; exists {
			participatingBrokers.Src = append(participatingBrokers.Src, broker)
		} else {
			return 0.00, 0.00, event, fmt.Errorf("Broker %d not found in broker metrics", b)
		}
	}

	// Destination brokers.
	for b := range bmb.dst {
		if broker, exists := bm[b]; exists {
			participatingBrokers.Dst = append(participatingBrokers.Dst, broker)
		} else {
			return 0.00, 0.00, event, fmt.Errorf("Broker %d not found in broker metrics", b)
		}
	}

	// Error if either source or destination broker list is empty.
	if len(participatingBrokers.Src) == 0 || len(participatingBrokers.Dst) == 0 {
		return 0.00, 0.00, event, fmt.Errorf("Source and destination broker list cannot be empty")
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
		return 0.00, 0.00, event, err
	}

	event = fmt.Sprintf("Most utilized source broker: "+
		"[%d] net tx of %.2fMB/s (over %ds) with an existing throttle rate of %.2fMB/s",
		constrainingSrc.ID, constrainingSrc.NetTX, Config.MetricsWindow, currThrottle)

	return replicationCapacity, currThrottle, event, nil
}

// applyTopicThrottles updates the throttled brokers list for
// all topics undergoing replication.
// XXX we need to avoid continously resetting this to reduce writes
// to ZK and subsequent config propagations to all brokers.
// We can either:
// - Ensure throttle lists are sorted so that if we provide the
// same list each iteration that it results in a no-op in the backend.
// - Keep track of topics that have already had a throttle list
// written and assume that it's not going to change
// (a throttle list is applied) when a topic is initially set
// for reassignment and cleared by autothrottle as soon as
// the reassignment is done).
func applyTopicThrottles(throttled map[string]map[string][]string, zk kafkazk.Handler) []string {
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
// rate, map of applied throttles, and zk kafkazk.Handler zookeeper client.
// For each broker, the throttle rate is applied and if successful, the rate
// is stored in the throttles map for future reference.
func applyBrokerThrottles(bs map[int]struct{}, ratestr string, r float64, ts map[int]float64, zk kafkazk.Handler) []string {
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
		time.Sleep(250 * time.Millisecond)
	}

	return errs
}

// removeAllThrottles removes all topic and
// broker throttle configs.
func removeAllThrottles(zk kafkazk.Handler, params *ReplicationThrottleMeta) error {
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
			Configs: [][2]string{
				[2]string{"leader.replication.throttled.rate", ""},
				[2]string{"follower.replication.throttled.rate", ""},
			},
		}

		changed, err := zk.UpdateKafkaConfig(config)
		switch err.(type) {
		case nil:
		case kafkazk.ErrNoNode:
			// We'd get an ErrNoNode here only if
			// the parent path for dynamic broker
			// configs (/config/brokers) if it doesn't
			// exist, which can happen in new clusters
			// that have never had dynamic configs applied.
			// Rather than creating that znode, we'll just
			// ignore errors here; if the znodes don't exist,
			// there's not even config to remove.
		default:
			log.Printf("Error removing throttle on broker %d: %s\n", b, err)
		}

		if changed {
			unthrottledBrokers = append(unthrottledBrokers, b)
			log.Printf("Throttle removed on broker %d\n", b)
		}

		// Hardcoded sleep to reduce
		// ZK load.
		time.Sleep(250 * time.Millisecond)
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
