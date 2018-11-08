package commands

import (
	"fmt"
	"os"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

var rebalanceCmd = &cobra.Command{
	Use:   "rebalance",
	Short: "[BETA] Rebalance partition allotments among a set of topics and brokers",
	Long:  `[BETA] Rebalance partition allotments among a set of topics and brokers`,
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
	rebalanceCmd.Flags().Bool("locality-scoped", true, "[WARN: disabling breaks rack.id constraints] Disallow a relocation to traverse rack.id values among brokers")
	rebalanceCmd.Flags().Bool("verbose", false, "Verbose output")
	rebalanceCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics (when using storage placement)")

	// Required.
	rebalanceCmd.MarkFlagRequired("brokers")
}

func rebalance(cmd *cobra.Command, _ []string) {
	bootstrap(cmd)

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
	partitionMap, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	partitionMapOrig := partitionMap.Copy()

	// Get a mapping of broker IDs to topics, partitions.
	mappings := partitionMap.Mappings()

	// Get a broker map.
	brokers := kafkazk.BrokerMapFromPartitionMap(partitionMap, brokerMeta, false)
	brokersOrig := brokers.Copy()

	// Update the currentBrokers list with
	// the provided broker list.
	// TODO we should only take New brokers in a rebalance.
	_ = brokers.Update(Config.brokers, brokerMeta)

	// Find brokers where the storage free is t %
	// below the harmonic mean.
	t, _ := cmd.Flags().GetFloat64("storage-threshold")
	offloadTargets := brokers.BelowMean(t, brokers.HMean)

	fmt.Printf("Brokers targeted for partition offloading: %v\n", offloadTargets)

	// Bundle planRelocationsForBrokerParams.
	params := planRelocationsForBrokerParams{
		relos:         map[int][]relocation{},
		mappings:      mappings,
		brokers:       brokers,
		partitionMeta: partitionMeta,
		plan:          relocationPlan{},
	}

	// Iterate over offload targets, planning
	// at most one relocation per iteration.
	// Continue this loop until no more relocations
	// can be planned.
	for exhaustedCount := 0; exhaustedCount < len(offloadTargets); {
		for _, sourceID := range offloadTargets {
			// Update the source broker ID.
			params.sourceID = sourceID
			relos := planRelocationsForBroker(cmd, params)

			// If no relocations could be planned,
			// increment the exhaustion counter.
			if relos == 0 {
				exhaustedCount++
			}
		}
	}

	// Print planned relocations.
	printPlannedRelocations(offloadTargets, params.relos, partitionMeta)

	// Update the partition map with the relocation plan.
	applyRelocationPlan(partitionMap, params.plan)

	// Print map change results.
	printMapChanges(partitionMapOrig, partitionMap)

	// Print broker assignment statistics.
	printBrokerAssignmentStats(cmd, partitionMapOrig, partitionMap, brokersOrig, brokers)

	// Ignore no-ops; ebalances will naturally have
	// a high percentage.
	partitionMapOrig, partitionMap = skipReassignmentNoOps(partitionMapOrig, partitionMap)

	// Write maps.
	writeMaps(cmd, partitionMap)
}
