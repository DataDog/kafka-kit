package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"regexp"
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
		return errors.New("Nil broker provided")
	}

	if k, exists := l[b.InstanceType]; exists {
		return math.Max(k-b.NetTX, l["mininum"]), nil
	}

	return l["mininum"], errors.New("Unknown instance type")
}

// ThrottledReplicas is a list of brokers
// with a throttle applied for an ongoing
// reassignment.
type ThrottledReplicas struct {
	Leaders   []*kafkametrics.Broker
	Followers []*kafkametrics.Broker
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
	time.Sleep(1*time.Second)

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

	// Init an EventGenerator
	// to pass around.
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

	var topics []string
	var reassignments kafkazk.Reassignments
	var replicatingPreviously map[string]interface{}
	var replicatingNow map[string]interface{}
	var done []string

	// Run.
	for {
		// Get topics undergoing reassignment.
		reassignments = zk.GetReassignments() // TODO This needs to return an error.
		topics = topics[:0]
		replicatingNow = make(map[string]interface{})
		for t := range reassignments {
			topics = append(topics, t)
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
		if len(topics) > 0 {
			log.Printf("Topics with ongoing reassignments: %s\n", topics)

			// Check if a throttle override is set.
			// If so, apply that static throttle.
			p := fmt.Sprintf("/%s/%s", apiConfig.ZKPrefix, apiConfig.RateSetting)
			override, err := zk.Get(p)
			if err != nil {
				log.Printf("Error fetching override: %s\n", err)
			}

			err = updateReplicationThrottle(topics, zk, km, string(override), events)
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

// updateReplicationThrottle takes a list of topics undergoing
// replication, a *kafkazk.ZK and an override param. Metrics for brokers participating
// in any ongoing replication are fetched to determine replication headroom.
// The replication throttle is then adjusted accordingly.
// If a non-empty override is provided, that static value is used instead
// of a dynamically determined value.
func updateReplicationThrottle(topics []string, zk *kafkazk.ZK, km *kafkametrics.KafkaMetrics, override string, events *EventGenerator) error {
	// Populate the topic configs from ZK.
	// Topic configs include a list of partitions
	// undergoing replication and broker IDs being
	// replicated to and from.
	topicConfigs := map[string]*kafkazk.TopicConfig{}

	for _, t := range topics {
		config, err := zk.GetTopicConfig(t)
		if err != nil {
			errS := fmt.Sprintf("Error fetching topic config: %s\n", err.Error())
			return errors.New(errS)
		}

		topicConfigs[t] = config
	}

	// Get broker metrics.
	brokerMetrics, err := km.GetMetrics()
	if err != nil {
		return err
	}

	// Get a ThrottledReplicas from the
	// topic configs retrieved.
	brokers, errs := replicasFromTopicConfigs(brokerMetrics, topicConfigs)
	if len(errs) > 0 {
		return errors.New(fmt.Sprint("Broker lookup error(s): %s", errs))
	}

	var leaders []int
	var followers []int
	allBrokers := map[int]interface{}{}

	for _, b := range brokers.Leaders {
		leaders = append(leaders, b.ID)
		allBrokers[b.ID] = nil
	}

	for _, b := range brokers.Followers {
		followers = append(followers, b.ID)
		allBrokers[b.ID] = nil
	}

	log.Printf("Leaders participating in replication: %v\n", leaders)
	log.Printf("Followers participating in replication: %v\n", followers)

	// Use the throttle override if set. Otherwise, use metrics.
	var tvalue float64
	var replicationHeadRoom float64
	if override != "" {
		log.Printf("A throttle override is set: %sMB/s\n", override)
		o, _ := strconv.Atoi(override)
		tvalue = float64(o) * 1000000.00
		// For log output.
		replicationHeadRoom = float64(o)
	} else {
		constrainingLeader := brokers.highestLeaderNetTX()
		replicationHeadRoom, err = BWLimits.headroom(constrainingLeader)
		if err != nil {
			return err
		}

		tvalue = replicationHeadRoom * 1000000.00

		log.Printf("Broker %d has the highest outbound network throughput of %.2fMB/s\n",
			constrainingLeader.ID, constrainingLeader.NetTX)
		log.Printf("Replication headroom: %.2fMB/s\n", replicationHeadRoom)
	}

	// Apply replication limit to all brokers.
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

		changed, err := zk.UpdateKafkaConfig(config)
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
	var allBrokersList []int
	for b := range allBrokers {
		allBrokersList = append(allBrokersList, b)
	}

	var b bytes.Buffer
	b.WriteString(fmt.Sprintf("Replication throttle of %.2fMB/s set on the following brokers: %v\n",
		replicationHeadRoom, allBrokersList))
	b.WriteString(fmt.Sprintf("Topics currently undergoing replication: %v", topics))
	events.Write("Broker replication throttle set", b.String())

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

// replicasFromTopicConfigs takes a map of kafkametrics.BrokerMetrics and
// kafkazk.TopicConfigs fetched from ZK and returns a ThrottledReplicas
// with lists of leader / follower brokers participating in a replication.
func replicasFromTopicConfigs(b kafkametrics.BrokerMetrics, c map[string]*kafkazk.TopicConfig) (ThrottledReplicas, []error) {
	t := ThrottledReplicas{}
	var errs []error
	for topic := range c {
		// Convert the config leader.replication.throttled.replicas
		// and follower.replication.throttled.replicas lists
		// to *[]Brokers. An array is returned where index 0
		// is the leaders list, index 1 is the followers list.
		brokers, err := brokersFromConfig(b, c[topic])
		if err != nil {
			errs = append(errs, err...)
		}

		// Append the leader/followers.
		t.Leaders = append(t.Leaders, brokers[0]...)
		t.Followers = append(t.Followers, brokers[1]...)
	}

	return t, errs
}

// highestLeaderNetTX takes a ThrottledReplicas and returns
// the leader with the highest outbound network throughput.
func (t ThrottledReplicas) highestLeaderNetTX() *kafkametrics.Broker {
	hwm := 0.00
	var broker *kafkametrics.Broker

	for _, b := range t.Leaders {
		if b.NetTX > hwm {
			hwm = b.NetTX
			broker = b
		}
	}

	return broker
}

// brokersFromThrottleList takes a string list of throttled
// replicas and returns a []int of broker IDs from
// the list.
func brokersFromThrottleList(l string) ([]int, error) {
	brokers := []int{}
	// Split the [partitionID]-[brokerID]
	// replica strings.
	rs := strings.Split(l, ",")
	// For each replica string,
	// get the broker ID.
	for _, r := range rs {
		p := strings.Split(r, ":")
		if len(p) != 2 {
			errS := fmt.Sprintf("Malformed throttle entry %s", r)
			return brokers, errors.New(errS)
		}
		ids := p[1]
		id, err := strconv.Atoi(ids)
		if err != nil {
			errS := fmt.Sprintf("Bad broker ID %s", ids)
			return brokers, errors.New(errS)
		}

		brokers = append(brokers, id)
	}

	return brokers, nil
}

// brokersFromConfig takes a *kafkazk.TopicConfig and maps the
// leader.replication.throttled.replicas and follower.replication.throttled.replicas
// fields to a [2][]*kafkametrics.Broker.
func brokersFromConfig(b kafkametrics.BrokerMetrics, c *kafkazk.TopicConfig) ([2][]*kafkametrics.Broker, []error) {
	brokers := [2][]*kafkametrics.Broker{
		// Leaders.
		[]*kafkametrics.Broker{},
		// Followers.
		[]*kafkametrics.Broker{},
	}

	var errs []error

	// Brokers is a map of leader and follower
	// broker IDs found in the TopicConfig.
	// Using a map rather than appending to the
	// lists directly to avoid duplicates.
	bids := map[string]map[int]interface{}{
		"leader":   map[int]interface{}{},
		"follower": map[int]interface{}{},
	}

	// Get leaders and followers from each
	// list of throttled replicas in the
	// topic config.
	for _, t := range []string{"leader", "follower"} {
		if l, exists := c.Config[t+".replication.throttled.replicas"]; exists {
			blist, err := brokersFromThrottleList(l)
			if err != nil {
				errs = append(errs, err)
			}
			// Ad each broker to the bids map.
			for _, id := range blist {
				bids[t][id] = nil
			}
		}
	}

	if len(errs) > 0 {
		return brokers, errs
	}

	// For each broker in the leaders and followers
	// lists, lookup in the BrokerMetrics. If a
	// corresponding BrokerMetrics exists,
	// append it to the final brokers sublist.
	for list := range bids {
		for id := range bids[list] {
			if broker, exists := b[id]; exists {
				if list == "leader" {
					brokers[0] = append(brokers[0], broker)
				} else {
					brokers[1] = append(brokers[1], broker)
				}
			} else {
				errS := fmt.Sprintf("Broker %d from the topic config not found in the broker metrics map", id)
				errs = append(errs, errors.New(errS))
				return brokers, errs
			}
		}
	}

	return brokers, nil
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
