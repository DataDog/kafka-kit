package main

import (
	"fmt"
	"os"
	"sort"

	"github.com/DataDog/topicmappr/kafkazk"
)

func initZooKeeper() kafkazk.Handler {
	if Config.useMeta || len(Config.rebuildTopics) > 0 || Config.placement == "storage" {
		zk, err := kafkazk.NewHandler(&kafkazk.Config{
			Connect:       Config.zkAddr,
			Prefix:        Config.zkPrefix,
			MetricsPrefix: Config.zkMetricsPrefix,
		})
		if err != nil {
			fmt.Printf("Error connecting to ZooKeeper: %s\n", err)
			os.Exit(1)
		}

		return zk
	}

	return nil
}

func getbrokerMeta(zk kafkazk.Handler) kafkazk.BrokerMetaMap {
	if Config.useMeta {
		// Whether or not we want to include
		// additional broker metrics Metadata.
		var withMetrics bool
		if Config.placement == "storage" {
			withMetrics = true
		}

		brokerMeta, errs := zk.GetAllBrokerMeta(withMetrics)
		// If no data is returned, report and exit.
		// Otherwise, it's possible that complete
		// data for a few brokers wasn't returned.
		// We check later whether any brokers that
		// matter are missing metrics.
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

func getPartitionMeta(zk kafkazk.Handler) kafkazk.PartitionMetaMap {
	if Config.placement == "storage" {
		partitionMeta, err := zk.GetAllPartitionMeta()
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		return partitionMeta
	}

	return nil
}

func getPartitionMap(zk kafkazk.Handler) *kafkazk.PartitionMap {
	switch {
	// Provided as text.
	case Config.rebuildMap != "":
		pm, err := kafkazk.PartitionMapFromString(Config.rebuildMap)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		return pm
	// Fetch from ZK.
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

func getTopics(pm *kafkazk.PartitionMap) {
	topics := map[string]interface{}{}
	for _, p := range pm.Partitions {
		topics[p.Topic] = nil
	}

	fmt.Printf("\nTopics:\n")
	for t := range topics {
		fmt.Printf("%s%s\n", indent, t)
	}
}

func ensureBrokerMetrics(bs kafkazk.BrokerMap, bm kafkazk.BrokerMetaMap) {
	if Config.useMeta {
		for id, b := range bs {
			// Missing brokers won't even
			// be found in the brokerMeta.
			if !b.Missing && id != 0 && bm[id].MetricsIncomplete {
				fmt.Printf("Metrics not found for broker %d\n", id)
				os.Exit(1)
			}
		}
	}
}

func getSubAffinities(bm kafkazk.BrokerMap, bmo kafkazk.BrokerMap, pm *kafkazk.PartitionMap) kafkazk.SubstitutionAffinities {
	var affinities kafkazk.SubstitutionAffinities
	if Config.subAffinity && !Config.forceRebuild {
		var err error
		affinities, err = bm.SubstitutionAffinities(pm)
		if err != nil {
			fmt.Printf("Substitution affinity error: %s\n", err.Error())
			os.Exit(1)
		}
	}

	// Print substitution affinities.
	if affinities != nil {
		fmt.Printf("%s-\n", indent)
	}

	for a, b := range affinities {
		var inferred string
		if bmo[a].Missing {
			inferred = "(inferred)"
		}
		fmt.Printf("%sSubstitution affinity: %d -> %d %s\n", indent, a, b.ID, inferred)
	}

	return affinities
}

func getBrokers(pm *kafkazk.PartitionMap, bm kafkazk.BrokerMetaMap) (kafkazk.BrokerMap, kafkazk.BrokerMap, *kafkazk.BrokerStatus) {
	fmt.Printf("\nBroker change summary:\n")

	// Get a broker map of the brokers in the current topic map.
	// If meta data isn't being looked up, brokerMeta will be empty.
	brokers := kafkazk.BrokerMapFromTopicMap(pm, bm, Config.forceRebuild)

	// Update the currentBrokers list with
	// the provided broker list.
	// TODO the information output of broker changes
	// comes from within this Update call. Should return
	// this info as a value and print it out here.
	bs := brokers.Update(Config.brokers, bm)

	if bs.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	return brokers, brokers.Copy(), bs
}

func printChangesActions(bs *kafkazk.BrokerStatus) {
	change := bs.New - bs.Replace

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
	case Config.replication == 0:
		fmt.Printf("%sno-op\n", indent)
	}
}

func updateReplicationFactor(pm *kafkazk.PartitionMap) {
	// If the replication factor is changed,
	// the partition map input needs to have stub
	// brokers appended (r factor increase) or
	// existing brokers removed (r factor decrease).
	if Config.replication > 0 {
		fmt.Printf("%sUpdating replication factor to %d\n",
			indent, Config.replication)

		pm.SetReplication(Config.replication)
	}
}

func buildMap(pm *kafkazk.PartitionMap, pmm kafkazk.PartitionMetaMap, bm kafkazk.BrokerMap, af kafkazk.SubstitutionAffinities) (*kafkazk.PartitionMap, []string) {
	rebuildParams := kafkazk.RebuildParams{
		PMM:          pmm,
		BM:           bm,
		Strategy:     Config.placement,
		Optimization: Config.optimize,
	}

	if af != nil {
		rebuildParams.Affinities = af
	}

	// If we're doing a force rebuild, the input map
	// must have all brokers stripped out.
	// A few notes about doing force rebuilds:
	//	- Map rebuilds should always be called on a stripped PartitionMap copy.
	//  - The BrokerMap provided in the Rebuild call should have
	//		been built from the original PartitionMap, not the stripped map.
	//  - A force rebuild assumes that all partitions will be lifted from
	// 		all brokers and repositioned. This means you should call the
	// 		SubStorageAll method on the BrokerMap if we're doing a "storage" placement strategy.
	//		The SubStorageAll takes a PartitionMap and PartitionMetaMap. The PartitionMap is
	// 		used to find partition to broker relationships so that the storage used can
	//		be readded to the broker's StorageFree value. The amount to be readded, the
	//		size of the partition, is referenced from the PartitionMetaMap.

	if Config.forceRebuild {
		// Get a stripped map that we'll call rebuild on.
		partitionMapInStripped := pm.Strip()
		// If the storage placement strategy is being used,
		// update the broker StorageFree values.
		if Config.placement == "storage" {
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
		if Config.placement == "storage" {
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

func printBrokerAssignmentStats(pm1, pm2 *kafkazk.PartitionMap, bm1, bm2 kafkazk.BrokerMap) {
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
	if Config.placement == "storage" {
		fmt.Println("\nStorage free change estimations:")

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

func writeMaps(pm *kafkazk.PartitionMap) {
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
	if Config.outFile != "" {
		err := kafkazk.WriteMap(pm, Config.outPath+Config.outFile)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json [combined map]\n", indent, Config.outPath, Config.outFile)
		}
	}

	for t := range tm {
		err := kafkazk.WriteMap(tm[t], Config.outPath+t)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json\n", indent, Config.outPath, t)
		}
	}
}
