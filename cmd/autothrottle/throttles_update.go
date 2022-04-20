package main

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/DataDog/kafka-kit/v3/cmd/autothrottle/internal/api"
	"github.com/DataDog/kafka-kit/v3/cmd/autothrottle/internal/throttlestore"
	"github.com/DataDog/kafka-kit/v3/kafkaadmin"
	"github.com/DataDog/kafka-kit/v3/kafkametrics"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

// brokerChangeEvent is the message type returned in the events channel from the
// applyBrokerThrottles func.
type brokerChangeEvent struct {
	id   int
	role string
	rate float64
}

// updateReplicationThrottle takes a ThrottleManager that holds topics
// being replicated, any ZooKeeper/other clients, throttle override params, and
// other required metadata. Metrics for brokers participating in any ongoing
// replication are fetched to determine replication headroom. The replication
// throttle is then adjusted accordingly. If a non-empty override is provided,
// that static value is used instead of a dynamically determined value.
// Additionally, broker-specific overrides may be specified, which take precedence
// over the global override.
// TODO(jamie): This function is a masssively messy artifact from autothrottle
// quickly growing in complexity from an originally flat, simple program. A
// considerable amount of shared data needs to be better encapsulated so we can
// deconstruct these functions that hold too much of the general autothrottle logic.
// WIP on doing so.
func (tm *ThrottleManager) updateReplicationThrottle() error {
	// Creates lists from maps.
	srcBrokers, dstBrokers, allBrokers := tm.reassigningBrokers.lists()

	log.Printf("Source brokers participating in replication: %v\n", srcBrokers)
	log.Printf("Destination brokers participating in replication: %v\n", dstBrokers)

	// Determine throttle rates.

	// Use the throttle override if set. Otherwise, make a calculation using broker
	// metrics and configured capacity values.
	var capacities = make(replicationCapacityByBroker)
	var brokerMetrics kafkametrics.BrokerMetrics
	var rateOverride bool
	var inFailureMode bool
	var metricErrs []error

	if tm.overrideRate != 0 {
		log.Printf("A global throttle override is set: %dMB/s\n", tm.overrideRate)
		rateOverride = true

		capacities.setAllRatesWithDefault(allBrokers, float64(tm.overrideRate))
	}

	if !rateOverride {
		// Get broker metrics.
		brokerMetrics, metricErrs = tm.km.GetMetrics()
		// Even if errors are returned, we can still proceed as long as we have complete
		// metrics data for all target brokers. If we have broker metrics for all target
		// brokers, we can ignore any errors.
		if metricErrs != nil {
			if brokerMetrics == nil || incompleteBrokerMetrics(allBrokers, brokerMetrics) {
				inFailureMode = true
			}
		}
	}

	// If we cannot proceed normally due to missing/partial metrics data, check what
	// failure iteration we're in. If we're above the threshold, revert to the minimum
	// rate, otherwise retain the previous rate.
	if inFailureMode {
		log.Printf("Errors fetching metrics: %s\n", metricErrs)

		// Increment and check our failure count against the configured threshold.
		over := tm.Failure()

		// If we're not over the threshold, return and just retain previous throttles.
		if !over {
			log.Printf("Metrics fetch failure count %d doesn't exeed threshold %d, retaining previous throttle\n",
				tm.failures, tm.failureThreshold)
			return nil
		}

		// We're over the threshold; failback to the configured minimum.
		log.Printf("Metrics fetch failure count %d exceeds threshold %d, reverting to min-rate %.2fMB/s\n",
			tm.failures, tm.failureThreshold, tm.limits["minimum"])

		// Set the failback rate.
		capacities.setAllRatesWithDefault(allBrokers, tm.limits["minimum"])
	}

	// Reset the failure counter. We may have incremented in past iterations, but if
	// we're here now, we can reset the count.
	if !inFailureMode {
		tm.ResetFailures()
	}

	// If there's no override set and we're not in a failure mode, apply the
	// calculated throttles.
	if !rateOverride && !inFailureMode {
		var err error
		capacities, err = brokerReplicationCapacities(tm, tm.reassigningBrokers, brokerMetrics)
		if err != nil {
			return err
		}
	}

	// Merge in broker-specific overrides if they're part of the reassignment.
	for id := range tm.reassigningBrokers.all {
		if override, exists := tm.brokerOverrides[id]; exists {
			// Any brokers with throttle overrides that are being issued as part of a
			// reassignemnt should be marked as such.
			override.ReassignmentParticipant = true
			tm.brokerOverrides[id] = override

			rate := override.Config.Rate
			// A rate of 0 means we intend to remove this throttle override. Skip.
			if rate == 0 {
				continue
			}

			log.Printf("A broker throttle override is set for %d: %dMB/s\n", id, rate)
			// Store the rate for both inbound and outbound traffic.
			capacities.storeLeaderAndFollerCapacity(id, float64(rate))
		}
	}

	// Set broker throttle configs.
	events, errs := tm.applyBrokerThrottles(tm.reassigningBrokers.all, capacities)

	for _, e := range errs {
		// TODO(jamie): revisit whether we should actually be returning rather than
		// just logging errors here.
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

	// Set topic throttle configs.
	if !tm.skipTopicUpdates {
		errs := tm.applyTopicThrottles(tm.reassigningBrokers.throttledReplicas)
		for _, e := range errs {
			log.Println(e)
		}
		if errs == nil {
			topics := tm.reassigningBrokers.throttledReplicas.topics()
			log.Printf("updated the throttle replicas configs for topics: %v\n", topics)
		}
	}

	// Append topic stats to event.
	var topics []string
	for t := range tm.reassignments {
		topics = append(topics, t)
	}
	b.WriteString(fmt.Sprintf("Topics currently undergoing replication: %v", topics))

	// Ship it.
	tm.events.Write("Broker replication throttle set", b.String())

	return nil
}

// updateOverrideThrottles takes a *ThrottleManager and applies
// replication throttles for any brokers with overrides set.
func (tm *ThrottleManager) updateOverrideThrottles() error {
	// The rate spec we'll be applying, which is the override rates.
	var capacities = make(replicationCapacityByBroker)
	// Broker IDs that will have throttles set.
	var toAssign = make(map[int]struct{})
	// Broker IDs that should have previously set throttles removed.
	var toRemove = make(map[int]struct{})

	for _, override := range tm.brokerOverrides {
		// ReassignmentParticipant have already had their override rate used as part
		// of an ongoing reassignment.
		if !override.ReassignmentParticipant {
			rate := float64(override.Config.Rate)
			// Rate == 0 means the rate was removed via the API.
			if rate == 0 {
				toRemove[override.ID] = struct{}{}
			} else {
				toAssign[override.ID] = struct{}{}
				capacities.storeLeaderAndFollerCapacity(override.ID, rate)
			}
		}
	}

	if len(toAssign) > 0 || len(toRemove) > 0 {
		log.Println("Setting broker level throttle overrides")
	} else {
		return nil
	}

	// Set broker throttle configs.
	events, errs := tm.applyBrokerThrottles(toAssign, capacities)

	for _, e := range errs {
		log.Println(e)
	}

	// Set topic throttle configs.
	if !tm.skipOverrideTopicUpdates {
		errs := tm.applyTopicThrottles(tm.overrideThrottleLists)
		for _, e := range errs {
			log.Println(e)
		}
		if errs == nil {
			topics := tm.overrideThrottleLists.topics()
			log.Printf("updated the throttle replicas configs for topics: %v\n", topics)
		}
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

	// Ship it.
	tm.events.Write("Broker level throttle override(s) configured", b.String())

	// Unset the broker throttles marked for removal.
	return tm.removeBrokerThrottlesByID(toRemove)
}

// purgeOverrideThrottles takes a *ThrottleManager and removes
// broker overrides from ZK that have been set to a value of 0.
func (tm *ThrottleManager) purgeOverrideThrottles() []error {
	// Broker IDs that should have previously set throttles removed.
	var toRemove = make(map[int]struct{})

	for _, override := range tm.brokerOverrides {
		rate := float64(override.Config.Rate)
		// Rate == 0 means the rate was removed via the API.
		if rate == 0 {
			toRemove[override.ID] = struct{}{}
		}
	}

	var errs []error

	for id := range toRemove {
		path := fmt.Sprintf("%s/%d", api.OverrideRateZnodePath, id)
		if err := throttlestore.RemoveThrottleOverride(tm.zk, path); err != nil {
			errs = append(errs, err)
		}
	}

	return errs
}

// applyBrokerThrottles applies broker throttle configs.
func (tm *ThrottleManager) applyBrokerThrottles(bs map[int]struct{}, capacities replicationCapacityByBroker) (chan brokerChangeEvent, []error) {
	var configs = kafkaadmin.SetThrottleConfig{Brokers: map[int]kafkaadmin.BrokerThrottleConfig{}}
	var legacyConfigs map[int]kafkazk.KafkaConfig

	// Set the throttle config for all reassigning brokers. We currently populate
	// both the Kafka native and legacy configs, conditionally applying whichever
	// is configured after all rates are calculated.
	for ID := range bs {

		legacyBrokerConfig := kafkazk.KafkaConfig{
			Type:    "broker",
			Name:    strconv.Itoa(ID),
			Configs: []kafkazk.KafkaConfigKV{},
		}

		brokerConfig := kafkaadmin.BrokerThrottleConfig{}

		// Check if a rate was determined for each role (leader, follower) type.
		for i, rate := range capacities[ID] {
			if rate == nil {
				continue
			}
			role := roleFromIndex(i)

			// Get the previously set throttle rate.
			prevRate := tm.previouslySetThrottles[ID][i]
			if prevRate == nil {
				v := 0.00
				prevRate = &v
			}

			// Get the maximum utilization value for logging purposes.
			var max float64
			switch role {
			case "leader":
				max = tm.limits["srcMax"]
			case "follower":
				max = tm.limits["dstMax"]
			}

			log.Printf("Replication throttle rate for broker %d [%s] (based on a %.0f%% max free capacity utilization): %0.2fMB/s\n",
				ID, role, max, *rate)

			// Check if the delta between the newly calculated throttle and the previous
			// throttle exceeds the ChangeThreshold param.
			d := math.Abs((*prevRate - *rate) / *prevRate * 100)
			if d < Config.ChangeThreshold {
				log.Printf("Proposed throttle is within %.2f%% of the previous throttle "+
					"(below %.2f%% threshold), skipping throttle update for broker %d\n",
					d, Config.ChangeThreshold, ID)
				continue
			}

			rateBytes := *rate * 1000000.00
			rateBytesString := fmt.Sprintf("%.0f", rateBytes)

			// Add config.
			switch role {
			case "leader":
				brokerConfig.OutboundLimitBytes = int(math.Round(rateBytes))
			case "follower":
				brokerConfig.InboundLimitBytes = int(math.Round(rateBytes))
			}

			// Add legacy config.
			c := kafkazk.KafkaConfigKV{fmt.Sprintf("%s.replication.throttled.rate", role), rateBytesString}
			legacyBrokerConfig.Configs = append(legacyBrokerConfig.Configs, c)
		}

		// Populate each configuration collection.
		configs.Brokers[ID] = brokerConfig
		legacyConfigs[ID] = legacyBrokerConfig
	}

	// Write the throttle configs.

	if !tm.kafkaNativeMode {
		// Use the direct ZooKeeper config update method.
		return tm.legacyApplyBrokerThrottles(legacyConfigs, capacities)
	}

	return tm.applyBrokerThrottlesSequential(configs, capacities)
}

// KafkaAdmin applies these sequentially under the hood, but from an API perspective
// it's a single batch job: if one fails, a single error is returned. We break
// these into sequential KafkaAdmin SetThrottle calls so that we can individually
// report errors/successes.
func (tm *ThrottleManager) applyBrokerThrottlesSequential(configs kafkaadmin.SetThrottleConfig, capacities replicationCapacityByBroker) (chan brokerChangeEvent, []error) {
	events := make(chan brokerChangeEvent, len(configs.Brokers)*2)
	var errs []error

	// For each broker, create a new config that only holds that broker and apply it.
	for id, config := range configs.Brokers {
		cfg := kafkaadmin.SetThrottleConfig{
			Brokers: map[int]kafkaadmin.BrokerThrottleConfig{
				id: config,
			}}

		ctx, cancelFn := tm.kafkaRequestContext()
		defer cancelFn()

		// Apply.
		err := tm.ka.SetThrottle(ctx, cfg)
		if err != nil {
			errs = append(errs, fmt.Errorf("Error setting throttle on broker %d: %s", id, err))
			// Continue to the next broker if we encounter an error.
			continue
		}

		// Store the configured rates in the previously set throttles map.

		// Store and log leader configs, if any.
		if cfg.Brokers[id].OutboundLimitBytes != 0 {
			rate := capacities[id][0]
			tm.previouslySetThrottles.storeLeaderCapacity(id, *rate)

			log.Printf("Updated throttle on broker %d [leader]\n", id)
			events <- brokerChangeEvent{
				id:   id,
				role: "leader",
				rate: *rate,
			}
		}

		// Store and log follower configs, if any.
		if cfg.Brokers[id].InboundLimitBytes != 0 {
			rate := capacities[id][1]
			tm.previouslySetThrottles.storeFollowerCapacity(id, *rate)

			log.Printf("Updated throttle on broker %d [follower]\n", id)
			events <- brokerChangeEvent{
				id:   id,
				role: "follower",
				rate: *rate,
			}
		}
	}

	close(events)

	return events, errs
}

// applyTopicThrottles updates a throttledReplicas for all topics undergoing
// replication.
// TODO(jamie) review whether the throttled replicas list changes as replication
// finishes; each time the list changes here, we probably update the config then
// propagate a watch to all the brokers in the cluster.
func (tm *ThrottleManager) applyTopicThrottles(throttledTopics topicThrottledReplicas) []error {
	if !tm.kafkaNativeMode {
		// Use the direct ZooKeeper config update method.
		return tm.legacyApplyTopicThrottles(throttledTopics)
	}

	// Populate the config with all topics named in the topicThrottledReplicas.
	ctx, cancel := tm.kafkaRequestContext()
	defer cancel()

	var throttleCfg = kafkaadmin.SetThrottleConfig{Topics: throttledTopics.topics()}

	// Apply the config.
	if err := tm.ka.SetThrottle(ctx, throttleCfg); err != nil {
		return []error{err}
	}

	return nil
}

// removeAllThrottles calls removeTopicThrottles and removeBrokerThrottles in sequence.
func (tm *ThrottleManager) removeAllThrottles() error {
	for _, fn := range []func() error{
		tm.removeTopicThrottles,
		tm.removeBrokerThrottles,
	} {
		if err := fn(); err != nil {
			return err
		}
	}

	return nil
}

// removeTopicThrottles removes all topic throttle configs.
func (tm *ThrottleManager) removeTopicThrottles() error {
	// ZooKeeper method.
	if !tm.kafkaNativeMode {
		return tm.legacyRemoveTopicThrottles()
	}

	// Get all topic states.
	ctx, cancel := tm.kafkaRequestContext()
	defer cancel()

	tstates, err := tm.ka.DescribeTopics(ctx, []string{".*"})
	if err != nil {
		return err
	}

	// States to []string of names.
	var topics []string
	for name := range tstates {
		topics = append(topics, name)
	}

	ctx, cancel = tm.kafkaRequestContext()
	defer cancel()

	cfg := kafkaadmin.RemoveThrottleConfig{
		Topics: topics,
	}

	// Issue the remove.
	if err := tm.ka.RemoveThrottle(ctx, cfg); err != nil {
		return err
	}

	return nil
}

// removeBrokerThrottlesByID removes broker throttle configs for the specified IDs.
func (tm *ThrottleManager) removeBrokerThrottlesByID(ids map[int]struct{}) error {
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

	// Unset all stored throttle rates.
	for ID := range tm.previouslySetThrottles {
		tm.previouslySetThrottles[ID] = [2]*float64{}
	}

	return nil
}

// removeBrokerThrottles removes all broker throttle configs.
func (tm *ThrottleManager) removeBrokerThrottles() error {
	// Fetch brokers.
	brokers, errs := tm.zk.GetAllBrokerMeta(false)
	if errs != nil {
		return errs[0]
	}

	var ids = make(map[int]struct{})
	for id := range brokers {
		// Skip brokers with an override where AutoRemove is false.
		if override, exists := tm.brokerOverrides[id]; exists {
			if !override.Config.AutoRemove {
				continue
			}
		}

		ids[id] = struct{}{}
	}

	return tm.removeBrokerThrottlesByID(ids)
}
