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
	rebalanceCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to ('-1' automatically expands to all currently mapped brokers)")
	rebalanceCmd.Flags().Float64("storage-threshold", 0.20, "Percent below the harmonic mean storage free to target for partition offload (0 targets a brokers)")
	rebalanceCmd.Flags().Float64("storage-threshold-gb", 0.00, "Storage free in gigabytes to target for partition offload (those below the specified value); 0 [default] defers target selection to --storage-threshold")
	rebalanceCmd.Flags().Float64("tolerance", 0.10, "Percent distance from the mean storage free to limit storage scheduling")
	rebalanceCmd.Flags().Int("partition-limit", 30, "Limit the number of top partitions by size eligible for relocation per broker")
	rebalanceCmd.Flags().Int("partition-size-threshold", 512, "Size in megabytes where partitions below this value will not be moved in a rebalance")
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
	partitionMapOrig, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Print topics matched to input params.
	printTopics(partitionMapOrig)

	// Get a broker map.
	brokersOrig := kafkazk.BrokerMapFromPartitionMap(partitionMapOrig, brokerMeta, false)

	// Validate all broker params, get a copy of the
	// broker IDs targeted for partition offloading.
	offloadTargets := validateBrokersForRebalance(cmd, brokersOrig, brokerMeta)

	// XXX brokersOrig, partitionMapOrig need to be consistent.

	partitionLimit, _ := cmd.Flags().GetInt("partition-limit")
	partitionSizeThreshold, _ := cmd.Flags().GetInt("partition-size-threshold")

	otm := map[int]struct{}{}
	for _, id := range offloadTargets {
		otm[id] = struct{}{}
	}

	type rebalanceMap struct {
		storageRange float64
		tolerance    float64
		partitionMap *kafkazk.PartitionMap
		relocations  map[int][]relocation
		brokers      kafkazk.BrokerMap
	}

	mapsByRange := []rebalanceMap{}

	for i := 0.01; i < 0.99; i += 0.01 {
		partitionMap := partitionMapOrig.Copy()

		// Bundle planRelocationsForBrokerParams.
		params := planRelocationsForBrokerParams{
			relos:                  map[int][]relocation{},
			mappings:               partitionMap.Mappings(),
			brokers:                brokersOrig.Copy(),
			partitionMeta:          partitionMeta,
			plan:                   relocationPlan{},
			topPartitionsLimit:     partitionLimit,
			partitionSizeThreshold: partitionSizeThreshold,
			offloadTargetsMap:      otm,
			tolerance:              i,
		}

		// Sort offloadTargets by storage free ascending.
		sort.Sort(offloadTargetsBySize{t: offloadTargets, bm: params.brokers})

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

		// Update the partition map with the relocation plan.
		applyRelocationPlan(cmd, partitionMap, params.plan)

		mapsByRange = append(mapsByRange, rebalanceMap{
			storageRange: params.brokers.StorageRange(),
			tolerance:    i,
			partitionMap: partitionMap,
			relocations:  params.relos,
			brokers:      params.brokers,
		})
	}

	sort.Slice(mapsByRange, func(i, j int) bool {
		return mapsByRange[i].storageRange < mapsByRange[j].storageRange
	})

	m := mapsByRange[0]
	partitionMap, relos, brokers := m.partitionMap, m.relocations, m.brokers

	fmt.Printf("xxx using a tolerance of %f\n", m.tolerance)

	for i := range mapsByRange {
		fmt.Printf("range for map %d: %f\n", i, mapsByRange[i].storageRange)
	}

	// Print planned relocations.
	printPlannedRelocations(offloadTargets, relos, partitionMeta)

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
