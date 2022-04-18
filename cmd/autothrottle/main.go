package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/kafka-kit/v3/kafkametrics"
	"github.com/DataDog/kafka-kit/v3/kafkametrics/datadog"
	"github.com/DataDog/kafka-kit/v3/kafkazk"

	"github.com/jamiealquiza/envy"
)

var (
	// This can be set with -ldflags "-X main.version=x.x.x"
	version = "0.0.0"

	// Config holds configuration
	// parameters.
	Config struct {
		KafkaNativeMode    bool
		APIKey             string
		AppKey             string
		NetworkTXQuery     string
		NetworkRXQuery     string
		BrokerIDTag        string
		InstanceTypeTag    string
		MetricsWindow      int
		BootstrapServers   []string
		ZKAddr             string
		ZKPrefix           string
		Interval           int
		APIListen          string
		ConfigZKPrefix     string
		DDEventTags        string
		MinRate            float64
		SourceMaxRate      float64
		DestinationMaxRate float64
		ChangeThreshold    float64
		FailureThreshold   int
		CapMap             map[string]float64
		CleanupAfter       int64
	}

	// Misc.
	topicsRegex = []*regexp.Regexp{regexp.MustCompile(".*")}
)

func main() {
	v := flag.Bool("version", false, "version")
	flag.BoolVar(&Config.KafkaNativeMode, "kafka-native-mode", false, "Favor native Kafka RPCs over ZooKeeper metadata access")
	flag.StringVar(&Config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&Config.AppKey, "app-key", "", "Datadog app key")
	flag.StringVar(&Config.NetworkTXQuery, "net-tx-query", "avg:system.net.bytes_sent{service:kafka} by {host}", "Datadog query for broker outbound bandwidth by host")
	flag.StringVar(&Config.NetworkRXQuery, "net-rx-query", "avg:system.net.bytes_rcvd{service:kafka} by {host}", "Datadog query for broker inbound bandwidth by host")
	flag.StringVar(&Config.BrokerIDTag, "broker-id-tag", "broker_id", "Datadog host tag for broker ID")
	flag.StringVar(&Config.InstanceTypeTag, "instance-type-tag", "instance-type", "Datadog tag for instance type")
	flag.IntVar(&Config.MetricsWindow, "metrics-window", 120, "Time span of metrics required (seconds)")
	bss := flag.String("bootstrap-servers", "localhost:9092", "Kafka bootstrap servers")
	flag.StringVar(&Config.ZKAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups)")
	flag.StringVar(&Config.ZKPrefix, "zk-prefix", "", "ZooKeeper namespace prefix")
	flag.IntVar(&Config.Interval, "interval", 180, "Autothrottle check interval (seconds)")
	flag.StringVar(&Config.APIListen, "api-listen", "localhost:8080", "Admin API listen address:port")
	flag.StringVar(&Config.ConfigZKPrefix, "zk-config-prefix", "autothrottle", "ZooKeeper prefix to store autothrottle configuration")
	flag.StringVar(&Config.DDEventTags, "dd-event-tags", "", "Comma-delimited list of Datadog event tags")
	flag.Float64Var(&Config.MinRate, "min-rate", 10, "Minimum replication throttle rate (MB/s)")
	flag.Float64Var(&Config.SourceMaxRate, "max-tx-rate", 90, "Maximum outbound replication throttle rate (as a percentage of available capacity)")
	flag.Float64Var(&Config.DestinationMaxRate, "max-rx-rate", 90, "Maximum inbound replication throttle rate (as a percentage of available capacity)")
	flag.Float64Var(&Config.ChangeThreshold, "change-threshold", 10, "Required change in replication throttle to trigger an update (percent)")
	flag.IntVar(&Config.FailureThreshold, "failure-threshold", 1, "Number of iterations that throttle determinations can fail before reverting to the min-rate")
	m := flag.String("cap-map", "", "JSON map of instance types to network capacity in MB/s")
	flag.Int64Var(&Config.CleanupAfter, "cleanup-after", 60, "Number of intervals after which to issue a global throttle unset if no replication is running")

	envy.Parse("AUTOTHROTTLE")
	flag.Parse()

	if *v {
		fmt.Println(version)
		os.Exit(0)
	}

	Config.BootstrapServers = strings.Split(*bss, ",")

	// Deserialize instance-type capacity map.
	Config.CapMap = map[string]float64{}
	if len(*m) > 0 {
		err := json.Unmarshal([]byte(*m), &Config.CapMap)
		if err != nil {
			fmt.Printf("Error parsing cap-map flag: %s\n", err)
			os.Exit(1)
		}
	}

	log.Println("Autothrottle Running")
	// Lazily prevent a tight restart
	// loop from thrashing ZK.
	time.Sleep(1 * time.Second)

	// Init ZK.
	zk, err := kafkazk.NewHandler(&kafkazk.Config{
		Connect: Config.ZKAddr,
		Prefix:  Config.ZKPrefix,
	})
	if err != nil {
		log.Fatal(err)
	}

	defer zk.Close()

	// Init the admin API.
	apiConfig := &APIConfig{
		Listen:   Config.APIListen,
		ZKPrefix: Config.ConfigZKPrefix,
	}

	initAPI(apiConfig, zk)
	log.Printf("Admin API: %s\n", Config.APIListen)

	// Init a Kafka metrics fetcher.
	km, err := datadog.NewHandler(&datadog.Config{
		APIKey:          Config.APIKey,
		AppKey:          Config.AppKey,
		NetworkTXQuery:  Config.NetworkTXQuery,
		NetworkRXQuery:  Config.NetworkRXQuery,
		BrokerIDTag:     Config.BrokerIDTag,
		InstanceTypeTag: Config.InstanceTypeTag,
		MetricsWindow:   Config.MetricsWindow,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Get optional Datadog event tags.
	t := strings.Split(Config.DDEventTags, ",")
	tags := []string{"name:kafka-autothrottle"}
	for _, tag := range t {
		tags = append(tags, tag)
	}

	// Init the Datadog event writer.
	echan := make(chan *kafkametrics.Event, 100)
	go eventWriter(km, echan)

	// Init an DDEventWriter.
	events := &DDEventWriter{
		c:           echan,
		titlePrefix: eventTitlePrefix,
		tags:        tags,
	}

	// Default to true on startup in case throttles were set in an autothrottle
	// process other than the current one.
	knownThrottles := true

	var reassignments kafkazk.Reassignments

	// Track topic replication states across intervals.
	var topicsReplicatingNow = newSet()
	var topicsReplicatingPreviously = newSet()

	// Track override broker states.
	var brokersThrottledPreviously = newSet()

	// Params for the updateReplicationThrottle request.

	newLimitsConfig := NewLimitsConfig{
		Minimum:            Config.MinRate,
		SourceMaximum:      Config.SourceMaxRate,
		DestinationMaximum: Config.DestinationMaxRate,
		CapacityMap:        Config.CapMap,
	}

	lim, err := NewLimits(newLimitsConfig)
	if err != nil {
		log.Fatal(err)
	}

	throttleMeta := &ThrottleManager{
		zk:                     zk,
		km:                     km,
		events:                 events,
		previouslySetThrottles: make(replicationCapacityByBroker),
		limits:                 lim,
		failureThreshold:       Config.FailureThreshold,
	}

	// Run.
	var interval int64
	var ticker = time.NewTicker(time.Duration(Config.Interval) * time.Second)

	// TODO(jamie): refactor this loop.
	for ; ; <-ticker.C {
		interval++

		// Get topics undergoing reassignment.
		if !Config.KafkaNativeMode {
			reassignments = zk.GetReassignments()
		} else {
			// KIP-455 compatible reassignments lookup.
			reassignments, err = zk.ListReassignments()
		}

		if err != nil {
			fmt.Printf("error fetching reassignments: %s\n", err)
			continue
		}

		topicsReplicatingNow = newSet()
		for t := range reassignments {
			topicsReplicatingNow.add(t)
		}

		// Check for topics that were previously seen replicating, but are no
		// longer in this interval.
		topicsDoneReplicating := topicsReplicatingPreviously.diff(topicsReplicatingNow)

		// Log and write event.
		if len(topicsDoneReplicating) > 0 {
			m := fmt.Sprintf("Topics done reassigning: %s", topicsDoneReplicating.keys())
			log.Println(m)
			events.Write("Topics done reassigning", m)
		}

		// If all of the currently replicating topics are a subset
		// of the previously replicating topics, we can stop updating
		// the Kafka topic throttled replicas list. This minimizes
		// state that must be propagated through the cluster.
		if topicsReplicatingNow.isSubSet(topicsReplicatingPreviously) {
			throttleMeta.DisableTopicUpdates()
		} else {
			throttleMeta.EnableTopicUpdates()
			// Unset any previously stored throttle rates. This is done to avoid a
			// scenario that results in autothrottle being unaware of externally
			// specified throttles and failing to override them. The condition can be
			// triggered when two subsequent reassignments involving the same broker
			// set are handled by autothrottle. The error condition is as follows:
			//
			// - Autothrottle sees reassignment 1 involving brokers 1001, 1002
			//   and determines a throttle rate of 100MB/s.
			// - Reassignment 1 completes, reassignment 2 is started in-between
			//   autothrottle intervals and a manual rate of 25MB/s is specified from
			//   the reassignment tool.
			// - Autothrottle sees reassignment 2, revisits throughput and determines
			//   the rate for brokers 1001 and 1002 should be 105MB/s, below the
			//   ChangeThreshold of 10% when compared to the last known rates set;
			//   throttle updates are skipped.
			// - The reassignment is now stuck at 25MB/s.
			//
			// There's two solutions considered to reconcile the stale state:
			// - Reset all previously stored rates when the current reassigning
			//   topic list is not a subset of the previous reassigning topic list.
			// - Force throttle updates every so many intervals, regardless of the
			//   required ChangeThreshold.
			//
			// Ensure we're doing option 1 right here:
			throttleMeta.previouslySetThrottles.reset()
		}

		// Rebuild topicsReplicatingPreviously with the current replications
		// for the next check iteration.
		topicsReplicatingPreviously = topicsReplicatingNow.copy()

		// Check if a global throttle override was configured.
		overrideCfg, err := fetchThrottleOverride(zk, overrideRateZnodePath)
		if err != nil {
			log.Println(err)
		}

		// Fetch all broker-specific overrides.
		throttleMeta.brokerOverrides, err = fetchBrokerOverrides(zk, overrideRateZnodePath)
		if err != nil {
			log.Println(err)
		}

		// Get the maps of brokers handling reassignments.
		throttleMeta.reassigningBrokers, err = getReassigningBrokers(reassignments, zk)
		if err != nil {
			log.Println(err)
		}

		// If topics are being reassigned, update the replication throttle.
		if len(topicsReplicatingNow) > 0 {
			log.Printf("Topics with ongoing reassignments: %s\n", topicsReplicatingNow.keys())

			// Update the throttleMeta.
			throttleMeta.overrideRate = overrideCfg.Rate
			throttleMeta.reassignments = reassignments

			err = updateReplicationThrottle(throttleMeta)
			if err != nil {
				log.Println(err)
			} else {
				// Set knownThrottles.
				knownThrottles = true
			}
		}

		// Get brokers with active overrides, ie where the override rate is non-0,
		// that are also not part of a reassignment.
		activeOverrideBrokers := throttleMeta.brokerOverrides.Filter(notReassignmentParticipant)

		// Apply any additional broker-specific throttles that were not applied as
		// part of a reassignment.
		if len(throttleMeta.brokerOverrides) > 0 {
			// Find all topics that include brokers with static overrides
			// configured that aren't being reassigned. In order for broker-specific
			// throttles to be applied, topics being replicated by those brokers
			// must include them in the follower.replication.throttled.replicas
			// dynamic configuration parameter. It's clumsy, but this is the way
			// Kafka was designed.
			// TODO(jamie): is there a scenario where we should exclude topics
			// have also have a reassignment? We're discovering topics here by
			// reverse lookup of brokers that are not reassignment participants.
			var err error
			throttleMeta.overrideThrottleLists, err = getTopicsWithThrottledBrokers(throttleMeta)
			if err != nil {
				log.Printf("Error fetching topic states: %s\n", err)
			}

			// Determine whether we need to propagate topic throttle replica
			// list configs. If the brokers with overrides remains the same,
			// we don't need to need to update those configs.
			var brokersThrottledNow = newSet()
			for broker := range activeOverrideBrokers {
				brokersThrottledNow.add(strconv.Itoa(broker))
			}

			if brokersThrottledNow.equal(brokersThrottledPreviously) {
				throttleMeta.DisableOverrideTopicUpdates()
			} else {
				throttleMeta.EnableOverrideTopicUpdates()
			}

			brokersThrottledPreviously = brokersThrottledNow.copy()

			// Update throttles.
			if err := updateOverrideThrottles(throttleMeta); err != nil {
				log.Println(err)
			}

			// If we're updating throttles and the active count (those not marked for
			// removal) is > 0, we should set the knownThrottles to true.
			if len(activeOverrideBrokers) > 0 {
				knownThrottles = true
			}
		}

		// Remove and delete any broker-specific overrides set to 0.
		if errs := purgeOverrideThrottles(throttleMeta); errs != nil {
			log.Println("Error removing persisted broker throttle overrides")
			for i := range errs {
				log.Println(errs[i])
			}
		}

		// If there's no topics being reassigned, clear any throttles marked
		// for automatic removal. Also, check if there's any broker throttles set.
		// There's a somewhat complicated state problem here; if we previously
		// set a broker throttle override but there's no reassignment, we'll
		// immediately clear it here. There's two options:
		//
		// 1) Simply hold up clearing throttles if there's a broker throttle
		//   override set.
		// 2) Fetch all topics where any brokers with overrides are assigned
		//   replicas, fetch all topic ISR states, diff the ISR states and the
		//   replica assignments to track under-replicated topics, then adding
		//   an under-replicated == 0 condition here.
		//
		// We're going with option 1 for now.

		// Capture all the current conditions:

		// Are there throttles eligible to be cleared?
		var throttlesToClear = knownThrottles || interval == Config.CleanupAfter

		// Are any topics being reassigned?
		var topicsReassigning bool
		if len(topicsReplicatingNow) > 0 {
			topicsReassigning = true
		}

		// Do any brokers have throttle overrides set?
		var brokerOverridesSet bool
		if len(activeOverrideBrokers) > 0 {
			brokerOverridesSet = true
		}

		// Next steps according to the various conditions:

		if !topicsReassigning {
			log.Println("No topics undergoing reassignment")
		}

		if !topicsReassigning && throttlesToClear && brokerOverridesSet {
			log.Println("One or more brokers level override are set; automatic throttle removal will be skipped")
		}

		// If there's previously set throttles but no topics reassigning nor
		// broker overrides set, we can issue a global throttle removal.
		if throttlesToClear && !topicsReassigning && !brokerOverridesSet {
			// Reset the interval count.
			interval = 0

			// Remove all the broker + topic throttle configs.
			err := removeAllThrottles(throttleMeta)
			if err != nil {
				log.Printf("Error removing throttles: %s\n", err.Error())
			} else {
				// Only set knownThrottles to false if we've removed all
				// without error.
				knownThrottles = false
			}

			// Ensure topic throttle updates are re-enabled.
			throttleMeta.EnableTopicUpdates()
			throttleMeta.EnableOverrideTopicUpdates()

			// Remove any configured throttle overrides if AutoRemove is true.
			if overrideCfg.AutoRemove {
				err := storeThrottleOverride(zk, overrideRateZnodePath, ThrottleOverrideConfig{})
				if err != nil {
					log.Println(err)
				} else {
					log.Println("Global throttle override removed")
				}
			}
		}

	}

}
