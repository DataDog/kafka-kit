package commands

import (
	"fmt"
	"os"
	"time"

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

	return partitionMeta
}

// stripPendingDeletes takes a partition map and zk handler. It looks
// up any topics in a pending delete state and removes them from the
// provided partition map, returning a list of topics removed.
func stripPendingDeletes(pm *kafkazk.PartitionMap, zk kafkazk.Handler) []string {
	// Get pending deletions.
	pd, err := zk.GetPendingDeletion()
	if err != nil {
		fmt.Println("Error fetching topics pending deletion")
	}

	if len(pd) == 0 {
		return []string{}
	}

	// This is used as a set of topic names
	// pending deleting.
	pending := map[string]struct{}{}

	for _, t := range pd {
		pending[t] = struct{}{}
	}

	// Traverse the partition map and drop
	// any pending topics.

	newPL := kafkazk.PartitionList{}
	pendingExcluded := map[string]struct{}{}
	for _, p := range pm.Partitions {
		if _, exists := pending[p.Topic]; !exists {
			newPL = append(newPL, p)
		} else {
			pendingExcluded[p.Topic] = struct{}{}
		}
	}

	pm.Partitions = newPL

	pendingExcludedNames := []string{}
	for t := range pendingExcluded {
		pendingExcludedNames = append(pendingExcludedNames, t)
	}

	return pendingExcludedNames
}
