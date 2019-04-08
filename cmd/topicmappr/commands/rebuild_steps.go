package commands

import (
	"fmt"
	"os"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

// *References to metrics metadata persisted in ZooKeeper, see:
// https://github.com/DataDog/kafka-kit/tree/master/cmd/metricsfetcher#data-structures)

// getPartitionMap returns a map of of partition, topic config
// (particuarly what brokers compose every replica set) for all
// topics specified. A partition map is either built from a string
// literal input (json from off-the-shelf Kafka tools output) provided
// via the ---map-string flag, or, by building a map based on topic
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
	case len(Config.topics) > 0:
		pm, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		return pm
	}

	return nil
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
	bs, msgs := brokers.Update(Config.brokers, bm)
	for m := range msgs {
		fmt.Printf("%s%s\n", indent, m)
	}

	return brokers, bs
}

// printChangesActions takes a BrokerStatus and prints out
// information output describing changes in broker counts
// and liveness.
func printChangesActions(cmd *cobra.Command, bs *kafkazk.BrokerStatus) {
	change := bs.New - bs.Replace
	r, _ := cmd.Flags().GetInt("replication")
	fr, _ := cmd.Flags().GetBool("force-rebuild")

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
		fmt.Printf("%sShrinking topic by %d broker(s)\n", indent, -change)
	case fr, r > 0:
		if fr {
			fmt.Printf("%sForce rebuilding map\n", indent)
		}
		if r > 0 {
			fmt.Printf("%sSetting replication factor to %d\n", indent, r)
		}
	default:
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
		pm.SetReplication(r)
	}
}

// buildMap takes an input PartitionMap, rebuild parameters, and all partition/broker
// metadata structures required to generate the output PartitionMap. A []string of
// warnings / advisories is returned if any are encountered.
func buildMap(cmd *cobra.Command, pm *kafkazk.PartitionMap, pmm kafkazk.PartitionMetaMap, bm kafkazk.BrokerMap, af kafkazk.SubstitutionAffinities) (*kafkazk.PartitionMap, errors) {
	placement := cmd.Flag("placement").Value.String()
	psf, _ := cmd.Flags().GetFloat64("partition-size-factor")
	mrrid, _ := cmd.Flags().GetInt("min-rack-ids")

	rebuildParams := kafkazk.RebuildParams{
		PMM:              pmm,
		BM:               bm,
		Strategy:         placement,
		Optimization:     cmd.Flag("optimize").Value.String(),
		PartnSzFactor:    psf,
		MinUniqueRackIDs: mrrid,
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
			allBrokers := func(b *kafkazk.Broker) bool { return true }
			err := rebuildParams.BM.SubStorage(pm, pmm, allBrokers)
			if err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
		}

		// Rebuild.
		return partitionMapInStripped.Rebuild(rebuildParams)
	}

	// Update the StorageFree only on brokers
	// marked for replacement.
	if placement == "storage" {
		replacedBrokers := func(b *kafkazk.Broker) bool { return b.Replace }
		err := rebuildParams.BM.SubStorage(pm, pmm, replacedBrokers)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Rebuild directly on the input map.
	return pm.Rebuild(rebuildParams)
}
