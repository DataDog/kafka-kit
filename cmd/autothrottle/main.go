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

	"github.com/DataDog/topicmappr/kafkametrics"
	"github.com/DataDog/topicmappr/kafkazk"
	"github.com/jamiealquiza/envy"
)

var (
	// Config holds configuration
	// parameters.
	Config struct {
		APIKey         string
		AppKey         string
		NetworkTXQuery string
		MetricsWindow  int
		ZKAddr         string
		ZKPrefix       string
		Interval       int
		APIListen      string
		ConfigZKPrefix string
		DDEventTags    string
		MinRate        float64
		CapMap         map[string]float64
	}

	// Misc.
	topicsRegex = []*regexp.Regexp{regexp.MustCompile(".*")}
)

func init() {
	// log.SetOutput(ioutil.Discard)

	flag.StringVar(&Config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&Config.AppKey, "app-key", "", "Datadog app key")
	flag.StringVar(&Config.NetworkTXQuery, "net-tx-query", "avg:system.net.bytes_sent{service:kafka} by {host}", "Network query for broker outbound bandwidth by host")
	flag.IntVar(&Config.MetricsWindow, "metrics-window", 60, "Time span of metrics to average")
	flag.StringVar(&Config.ZKAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups)")
	flag.StringVar(&Config.ZKPrefix, "zk-prefix", "", "ZooKeeper namespace prefix")
	flag.IntVar(&Config.Interval, "interval", 60, "Autothrottle check interval in seconds")
	flag.StringVar(&Config.APIListen, "api-listen", "localhost:8080", "Admin API listen address:port")
	flag.StringVar(&Config.ConfigZKPrefix, "zk-config-prefix", "autothrottle", "ZooKeeper prefix to store autothrottle configuration")
	flag.StringVar(&Config.DDEventTags, "dd-event-tags", "", "Comma-delimited list of Datadog event tags")
	flag.Float64Var(&Config.MinRate, "min-rate", 10, "Minimum replication throttle rate")
	m := flag.String("cap-map", "", "JSON map of instance types to network capacity (in MB/s)")

	envy.Parse("AUTOTHROTTLE")
	flag.Parse()

	// Decode instance-type capacity map.
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
	log.Println("Authrottle Running")
	// Lazily prevent a tight restart
	// loop from thrashing ZK.
	time.Sleep(1 * time.Second)

	// Init ZK.
	zk, err := kafkazk.NewZK(&kafkazk.ZKConfig{
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

	// Eh.
	err = zk.InitRawClient()
	if err != nil {
		log.Fatal(err)
	}

	// Init a Kafka metrics fetcher.
	km, err := kafkametrics.NewKafkaMetrics(&kafkametrics.Config{
		APIKey:         Config.APIKey,
		AppKey:         Config.AppKey,
		NetworkTXQuery: Config.NetworkTXQuery,
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

	// Default to true on startup.
	// In case throttles were set in
	// an autothrottle session other
	// than the current one.
	knownThrottles := true

	var reassignments kafkazk.Reassignments
	var replicatingPreviously map[string]interface{}
	var replicatingNow map[string]interface{}
	var done []string

	// Params for the updateReplicationThrottle
	// request.
	throttleMeta := &ReplicationThrottleMeta{
		zk:        zk,
		km:        km,
		events:    events,
		throttles: make(map[int]float64),
		limits:    NewLimits(Config.MinRate, Config.CapMap),
	}

	// Run.
	for {
		throttleMeta.topics = throttleMeta.topics[:0]
		// Get topics undergoing reassignment.
		reassignments = zk.GetReassignments() // TODO This needs to return an error.
		replicatingNow = make(map[string]interface{})
		for t := range reassignments {
			throttleMeta.topics = append(throttleMeta.topics, t)
			replicatingNow[t] = nil
		}

		// Check for topics that were
		// previously seen replicating,
		// but are no longer.
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
		replicatingPreviously = make(map[string]interface{})
		for t := range replicatingNow {
			replicatingPreviously[t] = nil
		}

		// If topics are being reassigned, update
		// the replication throttle.
		if len(throttleMeta.topics) > 0 {
			log.Printf("Topics with ongoing reassignments: %s\n", throttleMeta.topics)

			// Check if a throttle override is set.
			// If so, apply that static throttle.
			p := fmt.Sprintf("/%s/%s", apiConfig.ZKPrefix, apiConfig.RateSetting)
			override, err := zk.Get(p)
			if err != nil {
				log.Printf("Error fetching override: %s\n", err)
			}

			// Update the throttleMeta.
			throttleMeta.override = string(override)
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
			if knownThrottles {
				err := removeAllThrottles(zk, throttleMeta)
				if err != nil {
					log.Printf("Error removing throttles: %s\n", err.Error())
				} else {
					// Only set knownThrottles to
					// false if we've removed all
					// without error.
					knownThrottles = false
				}
			}
		}

		// Sleep for the next check interval.
		time.Sleep(time.Second * time.Duration(Config.Interval))
	}

}

// Temp mocks.

type zkmock struct{}

func (z *zkmock) GetReassignments() kafkazk.Reassignments {
	r := kafkazk.Reassignments{
		"test_topic": map[int][]int{
			2: []int{1001, 1002},
			3: []int{1005, 1006},
		},
	}
	return r
}

func (zk *zkmock) GetTopicConfig(t string) (*kafkazk.TopicConfig, error) {
	return &kafkazk.TopicConfig{
		Version: 1,
		Config: map[string]string{
			"leader.replication.throttled.replicas":   "2:1001,2:1002",
			"follower.replication.throttled.replicas": "3:1005,3:1006",
		},
	}, nil
}
