package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/jamiealquiza/envy"
	dd "github.com/zorkian/go-datadog-api"
)

type Config struct {
	APIKey      string
	AppKey      string
	PartnQuery  string
	BrokerQuery string
	Span        int
	Client      *dd.Client
}

var config = &Config{}

func init() {
	flag.StringVar(&config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&config.AppKey, "app-key", "", "Datadog app key")
	bq := flag.String("broker-storage-query", "avg:system.disk.free{service:kafka,device:/data} by {broker_id}", "Datadog metric query to get storage free by broker_id")
	pq := flag.String("partition-size-query", "max:kafka.log.partition.size{service:kafka} by {topic,partition}", "Datadog metric query to get partition size by topic, partition")
	flag.IntVar(&config.Span, "span", 3600, "Query range in seconds (now - span)")

	envy.Parse("METRICSFETCHER")
	flag.Parse()

	// Complete query string.
	config.BrokerQuery = fmt.Sprintf("%s.rollup(avg, %d)", *bq, config.Span)
	config.PartnQuery = fmt.Sprintf("%s.rollup(avg, %d)", *pq, config.Span)
}

func main() {
	// Init, validate dd client.
	config.Client = dd.NewClient(config.APIKey, config.AppKey)

	ok, err := config.Client.Validate()
	exitOnErr(err)
	if !ok {
		fmt.Println("invalid API or app key")
		os.Exit(1)
	}

	// Fetch metrics data.

	fmt.Printf("Submitting %s\n", config.PartnQuery)
	pm, err := partitionMetrics(config)
	exitOnErr(err)

	partnData, err := json.Marshal(pm)
	exitOnErr(err)

	fmt.Printf("Submitting %s\n", config.BrokerQuery)
	bm, err := brokerMetrics(config)
	exitOnErr(err)

	brokerData, err := json.Marshal(bm)
	exitOnErr(err)

	fmt.Println(string(partnData))
	fmt.Println(string(brokerData))
}

func exitOnErr(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}
