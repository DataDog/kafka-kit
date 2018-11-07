package commands

import (
	"fmt"
	"os"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

var rebalanceCmd = &cobra.Command{
	Use:   "rebalance",
	Short: "Rebalance partition allotments among a set of topics and brokers",
	Long:  `Rebalance partition allotments among a set of topics and brokers`,
	Run:   rebalance,
}

func init() {
	rootCmd.AddCommand(rebalanceCmd)

	rebalanceCmd.Flags().String("topics", "", "Rebuild topics (comma delim. list) by lookup in ZooKeeper")
	rebalanceCmd.Flags().String("out-path", "", "Path to write output map files to")
	rebalanceCmd.Flags().String("out-file", "", "If defined, write a combined map of all topics to a file")
	rebalanceCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to")
	rebalanceCmd.Flags().Float64("storage-threshold", 0.20, "Percent below the mean storage free to target for partition offload")
	rebalanceCmd.Flags().Float64("tolerance", 0.10, "Percent below the source broker or mean storage free that a destination target will tolerate")
	rebalanceCmd.Flags().Bool("locality-scoped", true, "Disallow a relocation to traverse rack.id values among brokers")
	rebalanceCmd.Flags().Bool("verbose", false, "Verbose output")
	rebalanceCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics (when using storage placement)")

	// Required.
	rebalanceCmd.MarkFlagRequired("brokers")
}

func rebalance(cmd *cobra.Command, _ []string) {
	bootstrap(cmd)

	verbose, _ := cmd.Flags().GetBool("verbose")

	// ZooKeeper init.
	zk, err := initZooKeeper(cmd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer zk.Close()

	// Get broker and partition metadata.
	brokerMeta := getBrokerMeta(cmd, zk, true)
	partitionMeta, err := getPartitionMeta(cmd, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Get the current partition map.
	pm, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	partitionMapOrig := pm.Copy()

	// Get a mapping of broker IDs to topics, partitions.
	mappings := pm.Mappings()

	// Get a broker map.
	brokers := kafkazk.BrokerMapFromPartitionMap(pm, brokerMeta, false)
	brokersOrig := brokers.Copy()

	// Update the currentBrokers list with
	// the provided broker list.
	// TODO we should only take New brokers in a rebalance.
	bs := brokers.Update(Config.brokers, brokerMeta)
	_ = bs

	// Find brokers where the storage free is t %
	// below the harmonic mean.
	t, _ := cmd.Flags().GetFloat64("storage-threshold")
	offloadTargets := brokers.BelowMean(t, brokers.HMean)

	// Use the arithmetic mean for target
	// thresholds.
	// TODO test what is best.
	meanStorageFree := brokers.Mean()

	tolerance, _ := cmd.Flags().GetFloat64("tolerance")
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")

	fmt.Printf("Brokers targeted for partition offloading: %v\n", offloadTargets)

	plan := relocationPlan{}

	// This map tracks planned moves from source
	// broker ID to a []relocation.
	var relos = map[int][]relocation{}

	// Iterate over offload target brokers.
	// TODO we should move only one partition per pass,
	// continue performing passes until either no more
	// movements can be done or a configured limit is hit.
	for _, sourceID := range offloadTargets {

		// Get the top partitions for the target broker.
		topPartn, _ := mappings.LargestPartitions(sourceID, 10, partitionMeta)

		if verbose {
			fmt.Printf("\nBroker %d has a storage free of %.2fGB. Top partitions:\n",
				sourceID, brokers[sourceID].StorageFree/div)

			for _, p := range topPartn {
				pSize, _ := partitionMeta.Size(p)
				fmt.Printf("%s%s p%d: %.2fGB\n",
					indent, p.Topic, p.Partition, pSize/div)
			}
		}

		targetLocality := brokers[sourceID].Locality

		// Plan partition movements.
		// Each time a partition is planned
		// to be moved, it's unmapped from the
		// broker so that it's not retried the
		// next iteration.
		for _, p := range topPartn {
			// Get a storage sorted brokerList.
			brokerList := brokers.List()
			brokerList.SortByStorage()

			pSize, _ := partitionMeta.Size(p)

			// Find a destination broker.
			var dest *kafkazk.Broker

			// Whether or not the destination broker should have the same
			// rack.id as the target. If so, choose the lowest utilized broker
			// in same locality. If not, choose the lowest utilized broker.
			switch localityScoped {
			case true:
				for _, b := range brokers {
					if b.Locality == targetLocality {
						dest = b
						break
					}
				}
			default:
				dest = brokerList[0]
			}

			if verbose {
				fmt.Printf("%s-\n", indent)
				fmt.Printf("%sAttempting migration plan for %s p%d\n", indent, p.Topic, p.Partition)
				fmt.Printf("%sCandidate destination broker %d has a storage free of %.2fGB\n",
					indent, dest.ID, dest.StorageFree/div)
			}

			sourceFree := brokers[sourceID].StorageFree + pSize
			destFree := dest.StorageFree - pSize

			// Don't plan a migration that'd push a destination
			// beyond the point the source already is at + the
			// allowable tolerance.
			destinationTolerance := sourceFree * (1 - tolerance)
			if destFree < destinationTolerance {
				if verbose {
					fmt.Printf("%sCannot move partition to candidate: "+
						"expected storage free %.2fGB below tolerated threshold of %.2fGB\n",
						indent, destFree/div, destinationTolerance/div)

				}

				continue
			}

			// If the estimated storage change pushes neither the
			// target nor destination beyond the threshold distance
			// from the mean, plan the partition migration.
			if absDistance(sourceFree, meanStorageFree) <= tolerance && absDistance(destFree, meanStorageFree) <= tolerance {
				relos[sourceID] = append(relos[sourceID], relocation{partition: p, destination: dest.ID})

				// Add to plan.
				plan.add(p, [2]int{sourceID, dest.ID})

				// Update StorageFree values.
				brokers[sourceID].StorageFree = sourceFree
				brokers[dest.ID].StorageFree = destFree

				// Remove the partition as being mapped
				// to the source broker.
				mappings.Remove(sourceID, p)

				if verbose {
					fmt.Printf("%sPlanning relocation to candidate\n", indent)
				}

				// Move on to the next broker.
				// break
			}
		}
	}

	// Print planned relocations.``
	printPlannedRelocations(offloadTargets, relos, partitionMeta)

	// Print map change results.
	printMapChanges(partitionMapOrig, pm)

	// Print broker assignment statistics.
	printBrokerAssignmentStats(cmd, partitionMapOrig, pm, brokersOrig, brokers)
}
