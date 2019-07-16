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

	"github.com/DataDog/kafka-kit/kafkametrics"
	"github.com/DataDog/kafka-kit/kafkametrics/datadog"
	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/jamiealquiza/envy"
)

var (
	// Config holds configuration
	// parameters.
	Config struct {
		APIKey           string
		AppKey           string
		NetworkTXQuery   string
		BrokerIDTag      string
		MetricsWindow    int
		ZKAddr           string
		ZKPrefix         string
		Interval         int
		APIListen        string
		ConfigZKPrefix   string
		DDEventTags      string
		MinRate          float64
		MaxRate          float64
		ChangeThreshold  float64
		FailureThreshold int
		CapMap           map[string]float64
		CleanupAfter     int64
	}

	// Misc.
	topicsRegex = []*regexp.Regexp{regexp.MustCompile(".*")}
)

func init() {
	// log.SetOutput(ioutil.Discard)

	flag.StringVar(&Config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&Config.AppKey, "app-key", "", "Datadog app key")
	flag.StringVar(&Config.NetworkTXQuery, "net-tx-query", "avg:system.net.bytes_sent{service:kafka} by {host}", "Datadog query for broker outbound bandwidth by host")
	flag.StringVar(&Config.BrokerIDTag, "broker-id-tag", "broker_id", "Datadog host tag for broker ID")
	flag.IntVar(&Config.MetricsWindow, "metrics-window", 120, "Time span of metrics required (seconds)")
	flag.StringVar(&Config.ZKAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups)")
	flag.StringVar(&Config.ZKPrefix, "zk-prefix", "", "ZooKeeper namespace prefix")
	flag.IntVar(&Config.Interval, "interval", 180, "Autothrottle check interval (seconds)")
	flag.StringVar(&Config.APIListen, "api-listen", "localhost:8080", "Admin API listen address:port")
	flag.StringVar(&Config.ConfigZKPrefix, "zk-config-prefix", "autothrottle", "ZooKeeper prefix to store autothrottle configuration")
	flag.StringVar(&Config.DDEventTags, "dd-event-tags", "", "Comma-delimited list of Datadog event tags")
	flag.Float64Var(&Config.MinRate, "min-rate", 10, "Minimum replication throttle rate (MB/s)")
	flag.Float64Var(&Config.MaxRate, "max-rate", 90, "Maximum replication throttle rate (as a percentage of available capacity)")
	flag.Float64Var(&Config.ChangeThreshold, "change-threshold", 10, "Required change in replication throttle to trigger an update (percent)")
	flag.IntVar(&Config.FailureThreshold, "failure-threshold", 1, "Number of iterations that throttle determinations can fail before reverting to the min-rate")
	m := flag.String("cap-map", "", "JSON map of instance types to network capacity in MB/s")
	flag.Int64Var(&Config.CleanupAfter, "cleanup-after", 60, "Number of intervals after which to issue a global throttle unset if no replication is running")

	envy.Parse("AUTOTHROTTLE")
	flag.Parse()

	// Deserialize instance-type capacity map.
	Config.CapMap = map[string]float64{}
	if len(*m) > 0 {
		err := json.Unmarshal([]byte(*m), &Config.CapMap)
		if err != nil {
			fmt.Printf("Error parsing cap-map flag: %s\n", err)
			os.Exit(1)
		}
	}
}

func main() {
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

	// Init an EventGenerator.
	events := &EventGenerator{
		c:           echan,
		titlePrefix: eventTitlePrefix,
		tags:        tags,
	}

	// Default to true on startup in case
	// throttles were set in an autothrottle
	// session other than the current one.
	knownThrottles := true

	var reassignments kafkazk.Reassignments
	var replicatingPreviously map[string]struct{}
	var replicatingNow map[string]struct{}
	var done []string

	// Params for the updateReplicationThrottle
	// request.

	newLimitsConfig := NewLimitsConfig{
		Minimum:     Config.MinRate,
		Maximum:     Config.MaxRate,
		CapacityMap: Config.CapMap,
	}

	lim, err := NewLimits(newLimitsConfig)
	if err != nil {
		log.Fatal(err)
	}

	throttleMeta := &ReplicationThrottleMeta{
		zk:               zk,
		km:               km,
		events:           events,
		throttles:        make(map[int]float64),
		limits:           lim,
		failureThreshold: Config.FailureThreshold,
	}

	overridePath := fmt.Sprintf("/%s/%s", apiConfig.ZKPrefix, apiConfig.RateSetting)

	// Run.
	var interval int64
	for {
		interval++
		throttleMeta.topics = throttleMeta.topics[:0]

		// Get topics undergoing reassignment.
		reassignments = zk.GetReassignments() // XXX This needs to return an error.
		replicatingNow = make(map[string]struct{})
		for t := range reassignments {
			throttleMeta.topics = append(throttleMeta.topics, t)
			replicatingNow[t] = struct{}{}
		}

		// Check for topics that were previously seen
		// replicating, but are no longer in this interval.
		done = done[:0]
		for t := range replicatingPreviously {
			if _, replicating := replicatingNow[t]; !replicating {
				done = append(done, t)
			}
		}

		// Log and write event.
		if len(done) > 0 {
			m := fmt.Sprintf("Topics done reassigning: %s", done)
			log.Println(m)
			events.Write("Topics done reassigning", m)
		}

		// Rebuild replicatingPreviously with
		// the current replications for the next
		// check iteration.
		replicatingPreviously = make(map[string]struct{})
		for t := range replicatingNow {
			replicatingPreviously[t] = struct{}{}
		}

		// Fetch any throttle override config.
		overrideCfg, err := getThrottleOverride(zk, overridePath)
		if err != nil {
			log.Println(err)
		}

		// If topics are being reassigned, update
		// the replication throttle.
		if len(throttleMeta.topics) > 0 {
			log.Printf("Topics with ongoing reassignments: %s\n", throttleMeta.topics)

			// Update the throttleMeta.
			throttleMeta.overrideRate = overrideCfg.Rate
			throttleMeta.reassignments = reassignments

			err = updateReplicationThrottle(throttleMeta)
			if err != nil {
				log.Println(err)
			}
			// Set knownThrottles.
			knownThrottles = true
		} else {
			log.Println("No topics undergoing reassignment")
			// Unset any throttles.
			if knownThrottles || interval == Config.CleanupAfter {
				// Reset the interval.
				interval = 0

				err := removeAllThrottles(zk, throttleMeta)
				if err != nil {
					log.Printf("Error removing throttles: %s\n", err.Error())
				} else {
					// Only set knownThrottles to
					// false if we've removed all
					// without error.
					knownThrottles = false
				}

				// Remove any configured throttle overrides
				// if AutoRemove is true.
				if overrideCfg.AutoRemove {
					err := setThrottleOverride(zk, overridePath, ThrottleOverrideConfig{})
					if err != nil {
						log.Println(err)
					} else {
						log.Println("throttle override removed")
					}
				}
			}
		}

		// Sleep for the next check interval.
		time.Sleep(time.Second * time.Duration(Config.Interval))
	}

}
