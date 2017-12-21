package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/DataDog/topicmappr/kafkametrics"
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
}
