package commands

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

// initZooKeeper inits a ZooKeeper connection if one is needed.
// Scenarios that would require a connection:
//  - the --use-meta flag is true (default), which requests
//    that broker metadata (such as rack ID or registration liveness).
//  - that topics were specified via --topics, which requires
//    topic discovery` via ZooKeeper.
//  - that the --placement flag was set to 'storage', which expects
//    metrics metadata to be stored in ZooKeeper.
func initZooKeeper(cmd *cobra.Command) kafkazk.Handler {
	m, _ := cmd.Flags().GetBool("use-meta")
	p := cmd.Flag("placement").Value.String()
	zkAddr := cmd.Parent().Flag("zk-addr").Value.String()

	timeout := 250 * time.Millisecond

	if m || len(Config.rebuildTopics) > 0 || p == "storage" {
		zk, err := kafkazk.NewHandler(&kafkazk.Config{
			Connect:       zkAddr,
			Prefix:        cmd.Parent().Flag("zk-prefix").Value.String(),
			MetricsPrefix: cmd.Flag("zk-metrics-prefix").Value.String(),
		})

		if err != nil {
			fmt.Printf("Error connecting to ZooKeeper: %s\n", err)
			os.Exit(1)
		}

		time.Sleep(timeout)

		if !zk.Ready() {
			fmt.Printf("Failed to connect to ZooKeeper %s within %s\n", zkAddr, timeout)
			os.Exit(1)
		}

		return zk
	}

	return nil
}

// *References to metrics metadata persisted in ZooKeeper, see:
// https://github.com/DataDog/kafka-kit/tree/master/cmd/metricsfetcher#data-structures)

// getbrokerMeta returns a map of brokers and broker metadata
// for those registered in ZooKeeper. Optionally, metrics metadata
// persisted in ZooKeeper (via an external mechanism*) can be merged
// into the metadata.
func getbrokerMeta(cmd *cobra.Command, zk kafkazk.Handler) kafkazk.BrokerMetaMap {
	if m, _ := cmd.Flags().GetBool("use-meta"); m {
		// Whether or not we want to include
		// additional broker metrics Metadata.
		var withMetrics bool
		if cmd.Flag("placement").Value.String() == "storage" {
			withMetrics = true
		}

		brokerMeta, errs := zk.GetAllBrokerMeta(withMetrics)
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

	return nil
}

// getPartitionMeta returns a map of topic, partition metadata
// persisted in ZooKeeper (via an external mechanism*). This is
// primarily partition size metrics data used for the storage
// placement strategy.
func getPartitionMeta(cmd *cobra.Command, zk kafkazk.Handler) kafkazk.PartitionMetaMap {
	if cmd.Flag("placement").Value.String() == "storage" {
		partitionMeta, err := zk.GetAllPartitionMeta()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		return partitionMeta
	}

	return nil
}

// getPartitionMap returns a map of of partition, topic config
// (particuarly what brokers compose every replica set) for all
// topics specified. A partition map is either built from a string
// literal input (json from off-the-shelf Kafka tools output) provided
// via the --rebuild-map flag, or, by building a map based on topic
// config found in ZooKeeper for all topics matching input provided
// via the --topics flag.
func getPartitionMap(cmd *cobra.Command, zk kafkazk.Handler) *kafkazk.PartitionMap {
	ms := cmd.Flag("map-string").Value.String()
	switch {
	// The map was provided as text.
	case ms != "":
		pm, err := kafkazk.PartitionMapFromString(ms)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		return pm
	// Build a map using ZooKeeper metadata
	// for all specified topics.
	case len(Config.rebuildTopics) > 0:
		pm, err := kafkazk.PartitionMapFromZK(Config.rebuildTopics, zk)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		return pm
	}

	return nil
}

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

// ensureBrokerMetrics takes a map of reference brokers and
// a map of discovered broker metadata. Any non-missing brokers
// in the broker map must be present in the broker metadata map
// and have a non-true MetricsIncomplete value.
func ensureBrokerMetrics(cmd *cobra.Command, bm kafkazk.BrokerMap, bmm kafkazk.BrokerMetaMap) {
	if m, _ := cmd.Flags().GetBool("use-meta"); m {
		for id, b := range bm {
			// Missing brokers won't even
			// be found in the brokerMeta.
			if !b.Missing && id != 0 && bmm[id].MetricsIncomplete {
				fmt.Printf("Metrics not found for broker %d\n", id)
				os.Exit(1)
			}
		}
	}
}

// getSubAffinities, if enabled via --sub-affinity, takes reference broker maps
// and a partition map and attempts to return a complete SubstitutionAffinities.
func getSubAffinities(cmd *cobra.Command, bm kafkazk.BrokerMap, bmo kafkazk.BrokerMap, pm *kafkazk.PartitionMap) kafkazk.SubstitutionAffinities {
	var affinities kafkazk.SubstitutionAffinities
	sa, _ := cmd.Flags().GetBool("sub-affinity")
	fr, _ := cmd.Flags().GetBool("force-rebuild")

	if sa && !fr {
		var err error
		affinities, err = bm.SubstitutionAffinities(pm)
		if err != nil {
			fmt.Printf("Substitution affinity error: %s\n", err.Error())
			os.Exit(1)
		}
	}

	// Print whether any affinities
	// were inferred.
	for a, b := range affinities {
		var inferred string
		if bmo[a].Missing {
			inferred = "(inferred)"
		}
		fmt.Printf("%sSubstitution affinity: %d -> %d %s\n", indent, a, b.ID, inferred)
	}

	return affinities
}

// getBrokers takes a PartitionMap and BrokerMetaMap and returns a BrokerMap
// along with a BrokerStatus. These two structures hold metadata describing
// broker state (rack IDs, whether they need to be replaced, newly provided, etc.)
// and general statistics.
// - The BrokerMap is later used in map rebuild time as the canonical source of
//   broker state. Brokers that need to be removed (either because they were not
//   registered in ZooKeeper or were removed from the --brokers list) are determined here.
// - The BrokerStatus is used for purely informational output, such as how many missing
//   brokers were discovered or newly provided (i.e. specified in the --brokers flag but
//   not previously holding any partitions for any partitions of the referenced topics
//   being rebuilt by topicmappr)
func getBrokers(cmd *cobra.Command, pm *kafkazk.PartitionMap, bm kafkazk.BrokerMetaMap) (kafkazk.BrokerMap, *kafkazk.BrokerStatus) {
	fmt.Printf("\nBroker change summary:\n")

	// Get a broker map of the brokers in the current partition map.
	// If meta data isn't being looked up, brokerMeta will be empty.
	fr, _ := cmd.Flags().GetBool("force-rebuild")
	brokers := kafkazk.BrokerMapFromPartitionMap(pm, bm, fr)

	// Update the currentBrokers list with
	// the provided broker list.
	// TODO the information output of broker changes
	// comes from within this Update call. Should return
	// this info as a value and print it out here.
	bs := brokers.Update(Config.brokers, bm)

	return brokers, bs
}

// printChangesActions takes a BrokerStatus and prints out
// information output describing changes in broker counts
// and liveness.
func printChangesActions(cmd *cobra.Command, bs *kafkazk.BrokerStatus) {
	change := bs.New - bs.Replace
	r, _ := cmd.Flags().GetInt("replication")

	// Print change summary.
	fmt.Printf("%sReplacing %d, added %d, missing %d, total count changed by %d\n",
		indent, bs.Replace, bs.New, bs.Missing+bs.OldMissing, change)

	// Print action.
	fmt.Printf("\nAction:\n")

	switch {
	case change >= 0 && bs.Replace > 0:
		fmt.Printf("%sRebuild topic with %d broker(s) marked for replacement\n",
			indent, bs.Replace)
	case change > 0 && bs.Replace == 0:
		fmt.Printf("%sExpanding/rebalancing topic with %d additional broker(s) (this is a no-op unless --force-rebuild is specified)\n",
			indent, bs.New)
	case change < 0:
		fmt.Printf("%sShrinking topic by %d broker(s)\n",
			indent, -change)
	case r == 0:
		fmt.Printf("%sno-op\n", indent)
	}
}

// updateReplicationFactor takes a PartitionMap and normalizes
// the replica set length to an optionally provided value.
func updateReplicationFactor(cmd *cobra.Command, pm *kafkazk.PartitionMap) {
	r, _ := cmd.Flags().GetInt("replication")
	// If the replication factor is changed,
	// the partition map input needs to have stub
	// brokers appended (r factor increase) or
	// existing brokers removed (r factor decrease).
	if r > 0 {
		fmt.Printf("%sUpdating replication factor to %d\n", indent, r)

		pm.SetReplication(r)
	}
}

// buildMap takes an input PartitionMap, rebuild parameters, and all partition/broker
// metadata structures required to generate the output PartitionMap. A []string of
// warnings / advisories is returned if any are encountered.
func buildMap(cmd *cobra.Command, pm *kafkazk.PartitionMap, pmm kafkazk.PartitionMetaMap, bm kafkazk.BrokerMap, af kafkazk.SubstitutionAffinities) (*kafkazk.PartitionMap, []string) {
	placement := cmd.Flag("placement").Value.String()
	psf, _ := cmd.Flags().GetFloat64("partition-size-factor")

	rebuildParams := kafkazk.RebuildParams{
		PMM:           pmm,
		BM:            bm,
		Strategy:      placement,
		Optimization:  cmd.Flag("optimize").Value.String(),
		PartnSzFactor: psf,
	}

	if af != nil {
		rebuildParams.Affinities = af
	}

	// If we're doing a force rebuild, the input map
	// must have all brokers stripped out.
	// A few notes about doing force rebuilds:
	// - Map rebuilds should always be called on a stripped PartitionMap copy.
	// - The BrokerMap provided in the Rebuild call should have
	//   been built from the original PartitionMap, not the stripped map.
	// - A force rebuild assumes that all partitions will be lifted from
	//   all brokers and repositioned. This means you should call the
	//   SubStorageAll method on the BrokerMap if we're doing a "storage" placement strategy.
	//   The SubStorageAll takes a PartitionMap and PartitionMetaMap. The PartitionMap is
	//   used to find partition to broker relationships so that the storage used can
	//   be readded to the broker's StorageFree value. The amount to be readded, the
	//   size of the partition, is referenced from the PartitionMetaMap.

	if fr, _ := cmd.Flags().GetBool("force-rebuild"); fr {
		// Get a stripped map that we'll call rebuild on.
		partitionMapInStripped := pm.Strip()
		// If the storage placement strategy is being used,
		// update the broker StorageFree values.
		if placement == "storage" {
			err := rebuildParams.BM.SubStorageAll(pm, pmm)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}

		// Rebuild.
		return partitionMapInStripped.Rebuild(rebuildParams)
	} else {
		// Update the StorageFree only on brokers
		// marked for replacement.
		if placement == "storage" {
			err := rebuildParams.BM.SubStorageReplacements(pm, pmm)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}

		// Rebuild directly on the input map.
		return pm.Rebuild(rebuildParams)
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
	var div = 1073741824.00 // Fixed on GB for now.
	psf, _ := cmd.Flags().GetFloat64("partition-size-factor")

	if cmd.Flag("placement").Value.String() == "storage" {
		fmt.Println("\nStorage free change estimations:")
		if psf != 1.0 {
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
			fmt.Printf("%sBroker %d: %.2f -> %.2f (%+.2fGB, %.2f%%) %s\n",
				indent, id, originalStorage, newStorage, diff[0]/div, diff[1], replace)
		}
	}
}

// writeMaps takes a PartitionMap and writes out
// files.
func writeMaps(cmd *cobra.Command, pm *kafkazk.PartitionMap) {
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
