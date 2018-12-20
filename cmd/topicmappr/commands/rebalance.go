package commands

import (
	"fmt"
	"os"
	"sort"

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
	rebalanceCmd.Flags().Float64("storage-threshold", 0.20, "Percent below the harmonic mean storage free to target for partition offload")
	rebalanceCmd.Flags().Float64("storage-threshold-gb", 0.00, "Storage free in gigabytes to target for partition offload (those below the specified value); 0 [default] defers target selection to --storage-threshold")
	rebalanceCmd.Flags().Float64("tolerance", 0.10, "Percent distance from the mean storage free to limit storage scheduling (0 targets a brokers)")
	rebalanceCmd.Flags().Int("partition-limit", 30, "Limit the number of top partitions by size eligible for relocation per broker")
	rebalanceCmd.Flags().Bool("locality-scoped", false, "Disallow a relocation to traverse rack.id values among brokers")
	rebalanceCmd.Flags().Bool("verbose", false, "Verbose output")
	rebalanceCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics")
	rebalanceCmd.Flags().Int("metrics-age", 60, "Kafka metrics age tolerance (in minutes)")
	rebalanceCmd.Flags().Bool("optimize-leaders", false, "Perform a naive leadership optimization")

	// Required.
	rebalanceCmd.MarkFlagRequired("brokers")
	rebalanceCmd.MarkFlagRequired("topics")
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
	checkMetaAge(cmd, zk)
	brokerMeta := getBrokerMeta(cmd, zk, true)
	partitionMeta := getPartitionMeta(cmd, zk)

	// Get the current partition map.
	partitionMap, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	partitionMapOrig := partitionMap.Copy()

	// Print topics matched to input params.
	printTopics(partitionMap)

	// Get a mapping of broker IDs to topics, partitions.
	mappings := partitionMap.Mappings()

	// Get a broker map.
	brokers := kafkazk.BrokerMapFromPartitionMap(partitionMap, brokerMeta, false)

	// Validate all broker params, get a copy of the
	// broker IDs targeted for partition offloading.
	offloadTargets := validateBrokersForRebalance(cmd, brokers, brokerMeta)

	// Store a copy of the original
	// broker map, post updates.
	brokersOrig := brokers.Copy()

	partitionLimit, _ := cmd.Flags().GetInt("partition-limit")

	otm := map[int]struct{}{}
	for _, id := range offloadTargets {
		otm[id] = struct{}{}
	}

	// Bundle planRelocationsForBrokerParams.
	params := planRelocationsForBrokerParams{
		relos:              map[int][]relocation{},
		mappings:           mappings,
		brokers:            brokers,
		partitionMeta:      partitionMeta,
		plan:               relocationPlan{},
		topPartitionsLimit: partitionLimit,
		offloadTargetsMap:  otm,
	}

	// Sort offloadTargets by storage free ascending.
	sort.Sort(offloadTargetsBySize{t: offloadTargets, bm: brokers})

	// Iterate over offload targets, planning
	// at most one relocation per iteration.
	// Continue this loop until no more relocations
	// can be planned.
	for exhaustedCount := 0; exhaustedCount < len(offloadTargets); {
		params.pass++
		for _, sourceID := range offloadTargets {
			// Update the source broker ID
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
	applyRelocationPlan(cmd, partitionMap, params.plan)

	// Print map change results.
	printMapChanges(partitionMapOrig, partitionMap)

	// Print broker assignment statistics.
	errs := printBrokerAssignmentStats(cmd, partitionMapOrig, partitionMap, brokersOrig, brokers)

	// Handle errors that are possible
	// to be overridden by the user (aka
	// 'WARN' in topicmappr console output).
	handleOverridableErrs(cmd, errs)

	// Ignore no-ops; rebalances will naturally have
	// a high percentage of these.
	partitionMapOrig, partitionMap = skipReassignmentNoOps(partitionMapOrig, partitionMap)

	// Write maps.
	writeMaps(cmd, partitionMap)
}
