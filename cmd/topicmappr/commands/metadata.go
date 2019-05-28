package commands

import (
	"fmt"
	"os"
	"time"
	"io/ioutil"
	"encoding/json"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

func checkMetaAge(cmd *cobra.Command, zk kafkazk.Handler) {
	age, err := zk.MaxMetaAge()
	if err != nil {
		fmt.Printf("Error fetching metrics metadata: %s\n", err)
		os.Exit(1)
	}

	tol, _ := cmd.Flags().GetInt("metrics-age")

	if age > time.Duration(tol)*time.Minute {
		fmt.Printf("Metrics metadata is older than allowed: %s\n", age)
		os.Exit(1)
	}
}

// getBrokerMeta returns a map of brokers and broker metadata
// for those registered in ZooKeeper. Optionally, metrics metadata
// persisted in ZooKeeper (via an external mechanism*) can be merged
// into the metadata.
func getBrokerMeta(cmd *cobra.Command, zk kafkazk.Handler, m bool) kafkazk.BrokerMetaMap {
	brokerMeta, errs := zk.GetAllBrokerMeta(m)
	// If no data is returned, report and exit.
	// Otherwise, it's possible that complete
	// data for a few brokers wasn't returned.
	// We check in subsequent steps as to whether any
	// brokers that matter are missing metrics.
	if errs != nil && brokerMeta == nil {
		for _, e := range errs {
			fmt.Println(e)
		}
		os.Exit(1)
	}
	// Get a broker map of the brokers in the current partition map.
	// If meta data isn't being looked up, brokerMeta will be empty.
	bmif, _ := cmd.Flags().GetString("brokers-storage-in-file")
	if bmif != ""  {
		jsonFile, err := os.Open(bmif)
		// if we os.Open returns an error then handle it
		if err != nil {
		    fmt.Printf("Error on %s",err)
		    os.Exit(1)
		}
		// defer the closing of our jsonFile so that we can parse it later on
		defer jsonFile.Close()
		data, _ := ioutil.ReadAll(jsonFile)
		bmm := kafkazk.BrokerMetricsMap{}
		err = json.Unmarshal(data, &bmm)
		if err != nil {
			fmt.Errorf("Error unmarshalling broker metrics: %s", err.Error())
			os.Exit(1)
		}
		// Populate each broker with
		// metric data.
		for bid := range brokerMeta {
			m, exists := bmm[bid]
			if exists {
				brokerMeta[bid].StorageFree = m.StorageFree
				brokerMeta[bid].MetricsIncomplete = false
			}
		}
	}

	return brokerMeta
}

// ensureBrokerMetrics takes a map of reference brokers and
// a map of discovered broker metadata. Any non-missing brokers
// in the broker map must be present in the broker metadata map
// and have a non-true MetricsIncomplete value.
func ensureBrokerMetrics(cmd *cobra.Command, bm kafkazk.BrokerMap, bmm kafkazk.BrokerMetaMap) {
	for id, b := range bm {
		// Missing brokers won't be found in the brokerMeta.
		if !b.Missing && id != kafkazk.StubBrokerID && bmm[id].MetricsIncomplete {
			fmt.Printf("Metrics not found for broker %d\n", id)
			os.Exit(1)
		}
	}
}

// getPartitionMeta returns a map of topic, partition metadata
// persisted in ZooKeeper (via an external mechanism*). This is
// primarily partition size metrics data used for the storage
// placement strategy.
func getPartitionMeta(cmd *cobra.Command, zk kafkazk.Handler) kafkazk.PartitionMetaMap {
	partitionMeta, err := zk.GetAllPartitionMeta()
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	// Get a the partitionMetaMap from input file
	psif, _ := cmd.Flags().GetString("partitions-size-in-file")
	if psif != ""  {
		jsonFile, err := os.Open(psif)
		// if we os.Open returns an error then handle it
		if err != nil {
		    fmt.Printf("Error on %s", err)
		    os.Exit(1)
		}
		// defer the closing of our jsonFile so that we can parse it later on
		defer jsonFile.Close()
		data, _ := ioutil.ReadAll(jsonFile)
		err = json.Unmarshal(data, &partitionMeta)
		if err != nil {
			fmt.Errorf("Error unmarshalling broker metrics: %s", err.Error())
			os.Exit(1)
		}
	}
	//fmt.Println(partitionMeta)
	return partitionMeta
}
