package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/datadog/topicmappr/kafkametrics"
	"github.com/datadog/topicmappr/kafkazk"
	"github.com/jamiealquiza/envy"
)

// Config holds configuration
// parameters.
var Config struct {
	APIKey         string
	AppKey         string
	NetworkTXQuery string
	MetricsWindow  int
}

// Limits is a map of instance-type
// to network bandwidth limits.
type Limits map[string]float64

// headroom takes an instance type and utilization
// and returns the headroom / free capacity.
func (l Limits) headroom(b *kafkametrics.Broker) (float64, error) {
	if k, exists := l[b.InstanceType]; exists {
		return k-b.NetTX, nil
	}

	return 0.00, errors.New("Unknown instance type")
}

// ThrottledReplicas is a list of brokers
// with a throttle applied for an ongoing
// reassignment.
type ThrottledReplicas struct {
	Leaders   []*kafkametrics.Broker
	Followers []*kafkametrics.Broker
}

func init() {
	flag.StringVar(&Config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&Config.AppKey, "app-key", "", "Datadog app key")
	flag.StringVar(&Config.NetworkTXQuery, "net-tx-query", "avg:system.net.bytes_sent{service:kafka} by {host}", "Network query for broker outbound bandwidth by host")
	flag.IntVar(&Config.MetricsWindow, "metrics-window", 300, "Time span of metrics to average")
	envy.Parse("TEST")
	flag.Parse()
}

func main() {
	// Hardcoded for now.
	bwLimits := Limits{
		"d2.4xlarge": 240.00,
		"d2.2xlarge": 120.00,
		"d2.xlarge": 60.00,
	}

	// Init a Kafka metrics fetcher.
	km, err := kafkametrics.NewKafkaMetrics(&kafkametrics.Config{
		APIKey:         Config.APIKey,
		AppKey:         Config.AppKey,
		NetworkTXQuery: Config.NetworkTXQuery,
		MetricsWindow:  Config.MetricsWindow,
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Get broker metrics.
	brokerMetrics, err := km.GetMetrics()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Get ongoing topic reassignments.
	zk := &zkmock{}
	reassignments := zk.GetReassignments()

	// Get topics undergoing reassignment.
	topics := []string{}
	for t := range reassignments {
		topics = append(topics, t)
	}

	fmt.Printf("Topics with ongoing reassignments: %s\n", topics)

	// Populate the topic configs from ZK.
	// Topic configs include a list of partitions
	// undergoing replication and broker IDs being
	// replicated to and from.
	topicConfigs := map[string]*kafkazk.TopicConfig{}

	for _, t := range topics {
		config, err := zk.GetTopicConfig(t)
		if err != nil {
			fmt.Printf("Error fetching topic config: %s\n", err.Error())
			os.Exit(1)
		}

		topicConfigs[t] = config
	}

	// Get a ThrottledReplicas from the
	// topic configs retrieved.
	brokers := replicasFromTopicConfigs(brokerMetrics, topicConfigs)

	constrainingLeader := brokers.highestLeaderNetTX()
	replicationHeadRoom, err := bwLimits.headroom(constrainingLeader)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	fmt.Printf("Broker %d has the highest outbound network throughput of %.2fMB/s\n",
		constrainingLeader.ID, constrainingLeader.NetTX)

	fmt.Printf("Calculated replication headroom: %.2fMB/s\n", replicationHeadRoom)

	// Calculate headroom.
	// Apply replication limit to all brokers.
}

// replicasFromTopicConfigs takes a map of kafkametrics.BrokerMetrics and
// kafkazk.TopicConfigs fetched from ZK and returns a ThrottledReplicas
// with lists of leader / follower brokers participating in a replication.
func replicasFromTopicConfigs(b kafkametrics.BrokerMetrics, c map[string]*kafkazk.TopicConfig) ThrottledReplicas {
	t := ThrottledReplicas{}
	for topic := range c {
		// Convert the config leader.replication.throttled.replicas
		// and follower.replication.throttled.replicas lists
		// to *[]Brokers. An array is returned where index 0
		// is the leaders list, index 1 is the followers list.
		brokers, err := brokersFromConfig(b, c[topic])
		if err != nil {
			fmt.Println(err)
			// XXX This should return an error.
			os.Exit(1)
		}

		// Append the leader/followers.
		t.Leaders = append(t.Leaders, brokers[0]...)
		t.Followers = append(t.Followers, brokers[1]...)
	}

	return t
}

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

func brokersFromThrottleList(l string) ([]int, error) {
	brokers := []int{}
	// Split the [partitionID]-[brokerID]
	// replica strings.
	rs := strings.Split(l, ",")
	// For each replica string,
	// get the broker ID.
	for _, r := range rs {
		ids := strings.Split(r, ":")[1]
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
func brokersFromConfig(b kafkametrics.BrokerMetrics, c *kafkazk.TopicConfig) ([2][]*kafkametrics.Broker, error) {
	brokers := [2][]*kafkametrics.Broker{
		// Leaders.
		[]*kafkametrics.Broker{},
		// Followers.
		[]*kafkametrics.Broker{},
	}

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
				return brokers, err
			}
			// Ad each broker to the bids map.
			for _, id := range blist {
				bids[t][id] = nil
			}
		}
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
				errS := fmt.Sprintf("Broker %s from the topic config not found in the broker metrics map", id)
				return brokers, errors.New(errS)
			}
		}
	}

	return brokers, nil
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
