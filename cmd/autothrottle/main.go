package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
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
		APIKey             string
		AppKey             string
		NetworkTXQuery     string
		NetworkRXQuery     string
		BrokerIDTag        string
		MetricsWindow      int
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

type topicSet map[string]struct{}
type brokerSet map[int]struct{}

func (t1 topicSet) isSubSetOf(t2 topicSet) bool {
	for k := range t1 {
		if _, exist := t2[k]; !exist {
			return false
		}
	}

	return true
}

func main() {
	v := flag.Bool("version", false, "version")
	flag.StringVar(&Config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&Config.AppKey, "app-key", "", "Datadog app key")
	flag.StringVar(&Config.NetworkTXQuery, "net-tx-query", "avg:system.net.bytes_sent{service:kafka} by {host}", "Datadog query for broker outbound bandwidth by host")
	flag.StringVar(&Config.NetworkRXQuery, "net-rx-query", "avg:system.net.bytes_rcvd{service:kafka} by {host}", "Datadog query for broker inbound bandwidth by host")
	flag.StringVar(&Config.BrokerIDTag, "broker-id-tag", "broker_id", "Datadog host tag for broker ID")
	flag.IntVar(&Config.MetricsWindow, "metrics-window", 120, "Time span of metrics required (seconds)")
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

	// Init the admin API.
	apiConfig := &APIConfig{
		Listen:   Config.APIListen,
		ZKPrefix: Config.ConfigZKPrefix,
	}

	initAPI(apiConfig, zk)
	log.Printf("Admin API: %s\n", Config.APIListen)
	if err != nil {
		log.Fatal(err)
	}
	defer zk.Close()

	// Init a Kafka metrics fetcher.
	km, err := datadog.NewHandler(&datadog.Config{
		APIKey:         Config.APIKey,
		AppKey:         Config.AppKey,
		NetworkTXQuery: Config.NetworkTXQuery,
		NetworkRXQuery: Config.NetworkRXQuery,
		BrokerIDTag:    Config.BrokerIDTag,
		MetricsWindow:  Config.MetricsWindow,
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

	// Default to true on startup in case throttles were set in  an autothrottle
	// process other than the current one.
	knownThrottles := true

	var reassignments kafkazk.Reassignments

	// Replication state tracking across intervals:

	// Topics.
	var topicsReplicatingPreviously = topicSet{}
	var topicsReplicatingNow = topicSet{}
	var topicsDoneReplicating []string
	// Brokers.
	var brokersReplicatingPreviously = brokerSet{}
	var brokersReplicatingNow = brokerSet{}
	var brokersDoneReplicating = brokerSet{}

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

	throttleMeta := &ReplicationThrottleConfigs{
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
	for {
		interval++

		// Get topics undergoing reassignment.
		reassignments = zk.GetReassignments() // XXX This needs to return an error.
		topicsReplicatingNow = make(topicSet)
		for t := range reassignments {
			topicsReplicatingNow[t] = struct{}{}
		}

		// Check for topics that were previously seen replicating, but are no
		// longer in this interval.
		topicsDoneReplicating = topicsDoneReplicating[:0]
		for t := range topicsReplicatingPreviously {
			if _, replicating := topicsReplicatingNow[t]; !replicating {
				topicsDoneReplicating = append(topicsDoneReplicating, t)
			}
		}

		// Log and write event.
		if len(topicsDoneReplicating) > 0 {
			m := fmt.Sprintf("Topics done reassigning: %s", topicsDoneReplicating)
			log.Println(m)
			events.Write("Topics done reassigning", m)
		}

		// Rebuild topicsReplicatingPreviously with the current replications
		// for the next check iteration.
		topicsReplicatingPreviously = make(map[string]struct{})
		for t := range topicsReplicatingNow {
			topicsReplicatingPreviously[t] = struct{}{}
		}

		// If all of the currently replicating topics are a subset
		// of the previously replicating topics, we can stop updating
		// the Kafka topic throttled replicas list. This minimizes
		// state that must be propagated through the cluster.
		if topicsReplicatingNow.isSubSetOf(topicsReplicatingPreviously) {
			throttleMeta.DisableTopicUpdates()
		} else {
			throttleMeta.EnableTopicUpdates()
		}

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

		// Get a count of brokers with active overrides, ie where the override
		// rate is non-0.
		activeBrokerOverrides := len(throttleMeta.brokerOverrides.Filter(hasActiveOverride))

		// Get the maps of brokers handling reassignments.
		throttleMeta.reassigningBrokers, err = getReassigningBrokers(reassignments, zk)
		if err != nil {
			log.Println(err)
		}

		// Track broker replication states across intervals.
		for b := range throttleMeta.reassigningBrokers.all {
			brokersReplicatingNow[b] = struct{}{}
		}

		// Check for brokers that were previously seen replicating, but are no
		// longer in this interval.
		brokersDoneReplicating = make(brokerSet)
		for b := range brokersReplicatingPreviously {
			if _, replicating := brokersReplicatingNow[b]; !replicating {
				brokersDoneReplicating[b] = struct{}{}
			}
		}

		// Rebuild topicsReplicatingPreviously with the current replications
		// for the next check iteration.
		brokersReplicatingPreviously = make(brokerSet)
		for t := range brokersReplicatingNow {
			brokersReplicatingPreviously[t] = struct{}{}
		}

		// If topics are being reassigned, update the replication throttle.
		if len(topicsReplicatingNow) > 0 {
			var topics []string
			for t := range topicsReplicatingNow {
				topics = append(topics, t)
			}
			log.Printf("Topics with ongoing reassignments: %s\n", topics)

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

			if err := updateOverrideThrottles(throttleMeta); err != nil {
				log.Println(err)
			}

			// If we're updating (which includes removing) throttles and the
			// active count (those not marked for removal) is > 0, we should
			// set the knownThrottles to true.
			if activeBrokerOverrides > 0 {
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
		if activeBrokerOverrides > 0 {
			brokerOverridesSet = true
		}

		// Next steps according to the varous conditions:

		if !topicsReassigning {
			log.Println("No topics undergoing reassignment")
		}

		if !topicsReassigning && throttlesToClear && brokerOverridesSet {
			log.Println("One or more broker level override are set; automatic throttle removal will be skipped")
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

		<-ticker.C
	}

}
