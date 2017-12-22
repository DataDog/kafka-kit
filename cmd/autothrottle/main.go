package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
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
		config, err := getTopicConfig(zk, t)
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
	fmt.Printf("Broker %s has the highest outbound network throughput of %.2fMB/s\n",
		constrainingLeader.Host, constrainingLeader.NetTX)
}

// Change this to a kafkazk.ZKHandler.
func getTopicConfig(z *zkmock, t string) (*kafkazk.TopicConfig, error) {
	c, err := z.GetTopicConfig(t)
	if err != nil {
		return nil, err
	}

	return c, nil
}

// replicasFromTopicConfigs takes a map of kafkametrics.BrokerMetrics and
// kafkazk.TopicConfigs fetched from ZK and returns a ThrottledReplicas
// with lists of leader / follower brokers participating in a replication.
func replicasFromTopicConfigs(b kafkametrics.BrokerMetrics, c map[string]*kafkazk.TopicConfig) ThrottledReplicas {
	t := ThrottledReplicas{}
	for topic := range c {
		// Convert the topic's leader.replication.throttled.replicas
		// and follower.replication.throttled.replicas lists
		// to *[]Brokers. An array is returned where index 0
		// is the leaders list, index 1 is the followers list.
		brokers, err := brokersFromConfig(b, c[topic])
		if err != nil {
			fmt.Println(err)
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
	bids := map[string]map[string]interface{}{
		"leaders":   map[string]interface{}{},
		"followers": map[string]interface{}{},
	}

	if s, exists := c.Config["leader.replication.throttled.replicas"]; exists {
		// Split the [partitionID]-[brokerID]
		// replica strings.
		rs := strings.Split(s, ",")
		// For each replica string,
		// get the broker ID.
		for _, r := range rs {
			id := strings.Split(r, ":")[1]
			// If the broker ID is in the BrokerMetrics,
			// append it to the list.
			bids["leaders"][id] = nil
		}
	}

	if s, exists := c.Config["follower.replication.throttled.replicas"]; exists {
		// Split the [partitionID]-[brokerID]
		// replica strings.
		rs := strings.Split(s, ",")
		// For each replica string,
		// get the broker ID.
		for _, r := range rs {
			id := strings.Split(r, ":")[1]
			// If the broker ID is in the BrokerMetrics,
			// append it to the list.
			bids["followers"][id] = nil
		}
	}

	// For each broker in the leaders and followers
	// lists, lookup in the BrokerMetrics.
	for list := range bids {
		for id := range bids[list] {
			if broker, exists := b[id]; exists {
				if list == "leaders" {
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
