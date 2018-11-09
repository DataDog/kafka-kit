package commands

import (
	"fmt"
	"os"
	"sort"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

// printTopics takes a partition map and prints out
// the names of all topics referenced in the map.
func printTopics(pm *kafkazk.PartitionMap) {
	topics := map[string]interface{}{}
	for _, p := range pm.Partitions {
		topics[p.Topic] = nil
	}

	fmt.Printf("\nTopics:\n")
	for t := range topics {
		fmt.Printf("%s%s\n", indent, t)
	}
}

// printMapChanges takes the original input PartitionMap
// and the final output PartitionMap and prints what's changed.
func printMapChanges(pm1, pm2 *kafkazk.PartitionMap) {
	// Ensure the topic name and partition
	// order match.
	for i := range pm1.Partitions {
		t1, t2 := pm1.Partitions[i].Topic, pm2.Partitions[i].Topic
		p1, p2 := pm1.Partitions[i].Partition, pm2.Partitions[i].Partition
		if t1 != t2 || p1 != p2 {
			fmt.Println("Unexpected partition map order")
			os.Exit(1)
		}
	}

	// Get a status string of what's changed.
	fmt.Println("\nPartition map changes:")
	for i := range pm1.Partitions {
		change := kafkazk.WhatChanged(pm1.Partitions[i].Replicas,
			pm2.Partitions[i].Replicas)

		fmt.Printf("%s%s p%d: %v -> %v %s\n",
			indent,
			pm1.Partitions[i].Topic,
			pm1.Partitions[i].Partition,
			pm1.Partitions[i].Replicas,
			pm2.Partitions[i].Replicas,
			change)
	}
}

// printBrokerAssignmentStats prints before and after broker usage stats,
// such as leadership counts, total partitions owned, degree distribution,
// and changes in storage usage.
func printBrokerAssignmentStats(cmd *cobra.Command, pm1, pm2 *kafkazk.PartitionMap, bm1, bm2 kafkazk.BrokerMap) {
	fmt.Println("\nBroker distribution:")

	// Get general info.
	dd1, dd2 := pm1.DegreeDistribution().Stats(), pm2.DegreeDistribution().Stats()
	fmt.Printf("%sdegree [min/max/avg]: %.0f/%.0f/%.2f -> %.0f/%.0f/%.2f\n",
		indent, dd1.Min, dd1.Max, dd1.Avg, dd2.Min, dd2.Max, dd2.Avg)

	fmt.Printf("%s-\n", indent)

	// Per-broker info.
	UseStats := pm2.UseStats()
	for _, use := range UseStats {
		fmt.Printf("%sBroker %d - leader: %d, follower: %d, total: %d\n",
			indent, use.ID, use.Leader, use.Follower, use.Leader+use.Follower)
	}

	// If we're using the storage placement strategy,
	// write anticipated storage changes.
	psf, _ := cmd.Flags().GetFloat64("partition-size-factor")

	if cmd.Use == "rebalance" || cmd.Flag("placement").Value.String() == "storage" {
		fmt.Println("\nStorage free change estimations:")
		if psf != 1.0 && cmd.Use != "rebalance" {
			fmt.Printf("%sPartition size factor of %.2f applied\n", indent, psf)
		}

		// Get filtered BrokerMaps. For the 'before' broker statistics, we want
		// all brokers in the original BrokerMap that were also in the input PartitionMap.
		// For the 'after' broker statistics, we want brokers that were not marked
		// for replacement. We don't necessarily want to exclude brokers in the output
		// that aren't mapped in the output PartitionMap. It's possible that a broker is
		// not mapped to any of the input topics but is still holding data for other topics.
		// It's ideal to still include that broker's storage metrics since it was a provided
		// input and wasn't marked for replacement (generally, users are doing storage placements
		// particularly to balance out the storage of the input broker list).
		mb1, mb2 := bm1.MappedBrokers(pm1), bm2.NonReplacedBrokers()

		// Range before/after.
		r1, r2 := mb1.StorageRange(), mb2.StorageRange()
		fmt.Printf("%srange: %.2fGB -> %.2fGB\n", indent, r1/div, r2/div)

		// Range spread before/after.
		rs1, rs2 := mb1.StorageRangeSpread(), mb2.StorageRangeSpread()
		fmt.Printf("%srange spread: %.2f%% -> %.2f%%\n", indent, rs1, rs2)

		// Std dev before/after.
		sd1, sd2 := mb1.StorageStdDev(), mb2.StorageStdDev()
		fmt.Printf("%sstd. deviation: %.2fGB -> %.2fGB\n", indent, sd1/div, sd2/div)

		fmt.Printf("%s-\n", indent)

		// Get changes in storage utilization.
		storageDiffs := bm1.StorageDiff(bm2)

		// Pop IDs into a slice for sorted ouptut.
		ids := []int{}
		for id := range storageDiffs {
			ids = append(ids, id)
		}

		sort.Ints(ids)

		for _, id := range ids {
			// Skip the internal reserved ID.
			if id == 0 {
				continue
			}

			diff := storageDiffs[id]

			// Indicate if the broker
			// is a replacement.
			var replace string
			if bm2[id].Replace {
				replace = "*marked for replacement"
			}

			originalStorage := bm1[id].StorageFree / div
			newStorage := bm2[id].StorageFree / div

			// Skip reporting non-changes when using rebalance.
			if cmd.Use == "rebalance" && diff[1] == 0.00 {
				continue
			}

			fmt.Printf("%sBroker %d: %.2f -> %.2f (%+.2fGB, %.2f%%) %s\n",
				indent, id, originalStorage, newStorage, diff[0]/div, diff[1], replace)
		}
	}
}

// skipReassignmentNoOps removes no-op partition map changes
// from the input and final output PartitionMap
func skipReassignmentNoOps(pm1, pm2 *kafkazk.PartitionMap) (*kafkazk.PartitionMap, *kafkazk.PartitionMap) {
	prunedInputPartitionMap := kafkazk.NewPartitionMap()
	prunedOutputPartitionMap := kafkazk.NewPartitionMap()
	for i := range pm1.Partitions {
		p1, p2 := pm1.Partitions[i], pm2.Partitions[i]
		if !p1.Equal(p2) {
			prunedInputPartitionMap.Partitions = append(prunedInputPartitionMap.Partitions, p1)
			prunedOutputPartitionMap.Partitions = append(prunedOutputPartitionMap.Partitions, p2)
		}
	}

	return prunedInputPartitionMap, prunedOutputPartitionMap
}

// writeMaps takes a PartitionMap and writes out
// files.
func writeMaps(cmd *cobra.Command, pm *kafkazk.PartitionMap) {
	if len(pm.Partitions) == 0 {
		fmt.Println("\nNo partition reassignments, skipping map generation")
		return
	}

	op := cmd.Flag("out-path").Value.String()
	of := cmd.Flag("out-file").Value.String()

	// Map per topic.
	tm := map[string]*kafkazk.PartitionMap{}
	for _, p := range pm.Partitions {
		if tm[p.Topic] == nil {
			tm[p.Topic] = kafkazk.NewPartitionMap()
		}
		tm[p.Topic].Partitions = append(tm[p.Topic].Partitions, p)
	}

	fmt.Println("\nNew partition maps:")
	// Global map if set.
	if of != "" {
		err := kafkazk.WriteMap(pm, op+of)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json [combined map]\n", indent, op, of)
		}
	}

	for t := range tm {
		err := kafkazk.WriteMap(tm[t], op+t)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json\n", indent, op, t)
		}
	}
}
