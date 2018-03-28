package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/DataDog/topicmappr/kafkazk"

	"github.com/jamiealquiza/envy"
	dd "github.com/zorkian/go-datadog-api"
)

type Config struct {
	Client      *dd.Client
	APIKey      string
	AppKey      string
	PartnQuery  string
	BrokerQuery string
	Span        int
	ZKAddr      string
	ZKPrefix    string
}

var config = &Config{}

func init() {
	flag.StringVar(&config.APIKey, "api-key", "", "Datadog API key")
	flag.StringVar(&config.AppKey, "app-key", "", "Datadog app key")
	bq := flag.String("broker-storage-query", "avg:system.disk.free{service:kafka,device:/data} by {broker_id}", "Datadog metric query to get storage free by broker_id")
	pq := flag.String("partition-size-query", "max:kafka.log.partition.size{service:kafka} by {topic,partition}", "Datadog metric query to get partition size by topic, partition")
	flag.IntVar(&config.Span, "span", 3600, "Query range in seconds (now - span)")
	flag.StringVar(&config.ZKAddr, "zk-addr", "localhost:2181", "ZooKeeper connect string")
	flag.StringVar(&config.ZKPrefix, "zk-prefix", "topicmappr", "ZooKeeper namespace prefix")

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
		exitOnErr(errors.New("Invalid API or app key"))
	}

	// Init ZK client.
	zk, err := kafkazk.NewHandler(&kafkazk.Config{
		Connect: config.ZKAddr,
	})
	exitOnErr(err)

	// Ensure znodes exist.
	paths := zkPaths(config.ZKPrefix)
	err = createZNodesIfNotExist(zk, paths)
	exitOnErr(err)

	// Fetch metrics data.
	fmt.Printf("Submitting %s\n", config.PartnQuery)
	pm, err := partitionMetrics(config)
	exitOnErr(err)
	fmt.Println("success")

	partnData, err := json.Marshal(pm)
	exitOnErr(err)

	fmt.Printf("Submitting %s\n", config.BrokerQuery)
	bm, err := brokerMetrics(config)
	exitOnErr(err)
	fmt.Println("success")

	brokerData, err := json.Marshal(bm)
	exitOnErr(err)

	// Trunc the paths slice if
	// there's a prefix.
	if len(paths) == 3 {
		paths = paths[1:]
	}

	// Write to ZK.
	err = zk.Set(paths[0], string(partnData))
	exitOnErr(err)

	err = zk.Set(paths[1], string(brokerData))
	exitOnErr(err)

	fmt.Println("\nData written to ZooKeeper")
}

func zkPaths(p string) []string {
	paths := []string{}

	var prefix string
	switch p {
	case "/":
		prefix = ""
	case "":
		prefix = ""
	default:
		prefix = fmt.Sprintf("/%s", p)
		paths = append(paths, prefix)
	}

	paths = append(paths, prefix+"/partitionmeta")
	paths = append(paths, prefix+"/brokermetrics")

	return paths
}

func createZNodesIfNotExist(z kafkazk.Handler, p []string) error {
	// Check each path.
	for _, path := range p {
		exist, err := z.Exists(path)
		if err != nil {
			return err
		}
		// Create it if it doesn't exist.
		if !exist {
			err := z.Create(path, "")
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func exitOnErr(e error) {
	if e != nil {
		fmt.Println(e)
		os.Exit(1)
	}
}
