package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/DataDog/topicmappr/kafkametrics"
	"github.com/DataDog/topicmappr/kafkazk"
	"github.com/jamiealquiza/envy"
)

var (
	// Events configs.
	eventTitlePrefix = "kafka-autothrottle"

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
	}

	// Hardcoded for now.
	BWLimits = Limits{
		// Min. config.
		"mininum": 10.00,
		// d2 class.
		"d2.xlarge":  100.00,
		"d2.2xlarge": 120.00,
		"d2.4xlarge": 240.00,
		// i3 class.
		"i3.xlarge":  130.00,
		"i3.2xlarge": 250.00,
		"i3.4xlarge": 500.00,
	}

	// Misc.
	topicsRegex = []*regexp.Regexp{regexp.MustCompile(".*")}
)

// Limits is a map of instance-type
// to network bandwidth limits.
type Limits map[string]float64

// headroom takes an instance type and utilization
// and returns the headroom / free capacity. A minimum
// value of 10MB/s is returned.
func (l Limits) headroom(b *kafkametrics.Broker) (float64, error) {
	if b == nil {
		return l["mininum"], errors.New("Nil broker provided")
	}

	if k, exists := l[b.InstanceType]; exists {
		return math.Max(k-b.NetTX, l["mininum"]), nil
	}

	return l["mininum"], errors.New("Unknown instance type")
}

// ReassigningBrokers is a list of brokers
// with a throttle applied for an ongoing
// reassignment.
type ReassigningBrokers struct {
	Src []*kafkametrics.Broker
	Dst []*kafkametrics.Broker
}

// EventGenerator wraps a channel
// where *kafkametrics.Event are written
// to along with any defaults, such as
// tags.
type EventGenerator struct {
	c           chan *kafkametrics.Event
	tags        []string
	titlePrefix string
}

// Write takes an event title and message string
// and writes a *kafkametrics.Event
// to the event channel, formatted
// with the configured title and tags.
func (e *EventGenerator) Write(t string, m string) {
	e.c <- &kafkametrics.Event{
		Title: fmt.Sprintf("[%s] %s", e.titlePrefix, t),
		Text:  m,
		Tags:  e.tags,
	}
}

// UpdateReplicationThrottleRequest holds all types
// needed to call the updateReplicationThrottle func.
type UpdateReplicationThrottleRequest struct {
	topics        []string
	reassignments kafkazk.Reassignments
	zk            *kafkazk.ZK
	km            *kafkametrics.KafkaMetrics
	override      string
	events        *EventGenerator
}

func init() {
	// log.SetOutput(ioutil.Discard)

	flag.StringVar(&Config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&Config.AppKey, "app-key", "", "Datadog app key")
	flag.StringVar(&Config.NetworkTXQuery, "net-tx-query", "avg:system.net.bytes_sent{service:kafka} by {host}", "Network query for broker outbound bandwidth by host")
	flag.IntVar(&Config.MetricsWindow, "metrics-window", 300, "Time span of metrics to average")
	flag.StringVar(&Config.ZKAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string (for broker metadata or rebuild-topic lookups)")
	flag.StringVar(&Config.ZKPrefix, "zk-prefix", "", "ZooKeeper namespace prefix")
	flag.IntVar(&Config.Interval, "interval", 60, "Autothrottle check interval in seconds")
	flag.StringVar(&Config.APIListen, "api-listen", "localhost:8080", "Admin API listen address:port")
	flag.StringVar(&Config.ConfigZKPrefix, "zk-config-prefix", "autothrottle", "ZooKeeper prefix to store autothrottle configuration")
	flag.StringVar(&Config.DDEventTags, "dd-event-tags", "", "Comma-delimited list of Datadog event tags")

	envy.Parse("AUTOTHROTTLE")
	flag.Parse()
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
	updateParams := &UpdateReplicationThrottleRequest{
		zk:     zk,
		km:     km,
		events: events,
	}

	// Run.
	for {
		updateParams.topics = updateParams.topics[:0]
		// Get topics undergoing reassignment.
		reassignments = zk.GetReassignments() // TODO This needs to return an error.
		replicatingNow = make(map[string]interface{})
		for t := range reassignments {
			updateParams.topics = append(updateParams.topics, t)
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
		if len(updateParams.topics) > 0 {
			log.Printf("Topics with ongoing reassignments: %s\n", updateParams.topics)

			// Check if a throttle override is set.
			// If so, apply that static throttle.
			p := fmt.Sprintf("/%s/%s", apiConfig.ZKPrefix, apiConfig.RateSetting)
			override, err := zk.Get(p)
			if err != nil {
				log.Printf("Error fetching override: %s\n", err)
			}

			// Update the updateParams.
			updateParams.override = string(override)
			updateParams.reassignments = reassignments

			err = updateReplicationThrottle(updateParams)
			if err != nil {
				log.Println(err)
			}
			// Set knownThrottles.
			knownThrottles = true
		} else {
			log.Println("No topics undergoing reassignment")
			// Unset any throttles.
			if knownThrottles {
				err := removeAllThrottles(zk, events)
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

// updateReplicationThrottle takes a UpdateReplicationThrottleRequest
// that holds topics being replicated, any clients, throttle override params,
// and other required metadata.
// Metrics for brokers participating in any ongoing replication
// are fetched to determine replication headroom.
// The replication throttle is then adjusted accordingly.
// If a non-empty override is provided, that static value is used instead
// of a dynamically determined value.
func updateReplicationThrottle(params *UpdateReplicationThrottleRequest) error {
	srcBrokers := map[int]interface{}{}
	dstBrokers := map[int]interface{}{}
	allBrokers := map[int]interface{}{}
	srcBrokersList := []int{}
	dstBrokersList := []int{}

	// Get topic data for each topic
	// undergoing a reassignment.
	for t := range params.reassignments {
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
					srcBrokers[b] = nil
					allBrokers[b] = nil
				}
				// Dst.
				for _, b := range reassigning {
					dstBrokers[b] = nil
					allBrokers[b] = nil
				}
			}
		}
	}

	// Creates lists from the maps.
	allBrokersList := []int{}
	for n, m := range []map[int]interface{}{srcBrokers, dstBrokers} {
		for b := range m {
			if n == 0 {
				srcBrokersList = append(srcBrokersList, b)
			} else {
				dstBrokersList = append(dstBrokersList, b)
			}
			allBrokersList = append(allBrokersList, b)
		}
	}

	// Sort.
	sort.Ints(srcBrokersList)
	sort.Ints(dstBrokersList)
	sort.Ints(allBrokersList)

	log.Printf("Source brokers participating in replication: %v\n", srcBrokersList)
	log.Printf("Destination brokers participating in replication: %v\n", dstBrokersList)

	// Use the throttle override if set. Otherwise, use metrics.
	var tvalue float64
	var replicationHeadRoom float64

	if params.override != "" {
		log.Printf("A throttle override is set: %sMB/s\n", params.override)
		o, _ := strconv.Atoi(params.override)
		tvalue = float64(o) * 1000000.00
		// For log output.
		replicationHeadRoom = float64(o)
	} else {
		// Get broker metrics.
		brokerMetrics, err := params.km.GetMetrics()
		if err != nil {
			return err
		}

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

		constrainingSrc := participatingBrokers.highestSrcNetTX()
		replicationHeadRoom, err = BWLimits.headroom(constrainingSrc)
		if err != nil {
			return err
		}

		tvalue = replicationHeadRoom * 1000000.00

		log.Printf("Source broker %d has the highest outbound network throughput of %.2fMB/s\n",
			constrainingSrc.ID, constrainingSrc.NetTX)
		log.Printf("Replication headroom: %.2fMB/s\n", replicationHeadRoom)
	}

	// Generate a throttle config.
	rateString := fmt.Sprintf("%.0f", tvalue)
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

func removeAllThrottles(zk *kafkazk.ZK, events *EventGenerator) error {
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

	var allBrokers []int

	// Unset throttles.
	for b := range brokers {
		allBrokers = append(allBrokers, b)
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
			log.Printf("Throttle removed on broker %d\n", b)
		}

		// Hardcoded sleep to reduce
		// ZK load.
		time.Sleep(500 * time.Millisecond)
	}

	// Write event.
	m := fmt.Sprintf("Replication throttle removed on the following brokers: %v", allBrokers)
	events.Write("Broker replication throttle removed", m)

	// Lazily check if any
	// errors were encountered,
	// return a generic error.
	if err != nil {
		return errors.New("one or more throttles were not cleared")
	}

	return nil
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

// eventWriter reads from a channel of
// kafkazk.Event and writes them to the
// Datadog API. Errors are logged and
// do not affect progression.
func eventWriter(k *kafkametrics.KafkaMetrics, c chan *kafkametrics.Event) {
	for e := range c {
		err := k.PostEvent(e)
		if err != nil {
			log.Printf("Error writing event: %s\n", err)
		}
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
