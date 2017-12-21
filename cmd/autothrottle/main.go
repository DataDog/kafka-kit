package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/datadog/topicmappr/kafkametrics"
	"github.com/datadog/topicmappr/kafkazk"
	"github.com/jamiealquiza/envy"
)

var Config struct {
	APIKey         string
	AppKey         string
	NetworkTXQuery string
	MetricsWindow  int
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
	brokers, err := km.GetMetrics()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Do things.
	for id, b := range brokers {
		fmt.Printf("Broker %s: %.0fMB/s net TX\n", id, b.NetTX)
	}

	zk := &zkmock{}
	reassignments := zk.GetReassignments()
}

type zkmock struct{}

func (z *zkmock) GetReassignments() reassignments {
	r := reassignments{
		"test_topic": map[int][]int{
			2: []int{1003, 1004},
			3: []int{1004, 1003},
		},
	}
	return r
}

func (z *zkmock) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	t := []string{"test_topic", "test_topic2"}

	match := map[string]bool{}
	// Get all topics that match all
	// provided topic regexps.
	for _, topicRe := range ts {
		for _, topic := range t {
			if topicRe.MatchString(topic) {
				match[topic] = true
			}
		}
	}

	// Add matches to a slice.
	matched := []string{}
	for topic := range match {
		matched = append(matched, topic)
	}

	return matched, nil
}
