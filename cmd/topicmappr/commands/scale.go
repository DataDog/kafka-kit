// The scale command is mostly the same as rebalance, so several common functions
// are shared from the rebalance_steps file.
package commands

import (
	"fmt"
	"os"
	"sort"
	"sync"

	"github.com/DataDog/kafka-kit/v3/kafkazk"

	"github.com/spf13/cobra"
)

var scaleCmd = &cobra.Command{
	Use:   "scale",
	Short: "Redistribute partitions to additional brokers",
	Long:  `Redistribute partitions to additional brokers`,
	Run:   scale,
}

func init() {
	rootCmd.AddCommand(scaleCmd)

	scaleCmd.Flags().String("topics", "", "Rebuild topics (comma delim. list) by lookup in ZooKeeper")
	scaleCmd.Flags().String("topics-exclude", "", "Exclude topics")
	scaleCmd.Flags().String("out-path", "", "Path to write output map files to")
	scaleCmd.Flags().String("out-file", "", "If defined, write a combined map of all topics to a file")
	scaleCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to ('-1' for all currently mapped brokers, '-2' for all brokers in cluster)")
	scaleCmd.Flags().Float64("tolerance", 0.0, "Percent distance from the mean storage free to limit storage scheduling (0 performs automatic tolerance selection)")
	scaleCmd.Flags().Int("partition-limit", 30, "Limit the number of top partitions by size eligible for relocation per broker")
	scaleCmd.Flags().Int("partition-size-threshold", 512, "Size in megabytes where partitions below this value will not be moved in a scale")
	scaleCmd.Flags().Bool("locality-scoped", false, "Ensure that all partition movements are scoped by rack.id")
	scaleCmd.Flags().Bool("verbose", false, "Verbose output")
	scaleCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics")
	scaleCmd.Flags().Int("metrics-age", 60, "Kafka metrics age tolerance (in minutes)")
	scaleCmd.Flags().Bool("optimize-leadership", false, "Scale all broker leader/follower ratios")

	// Required.
	scaleCmd.MarkFlagRequired("brokers")
	scaleCmd.MarkFlagRequired("topics")
}

func scale(cmd *cobra.Command, _ []string) {
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
	partitionMapIn, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Exclude any topics that are pending deletion.
	pending := stripPendingDeletes(partitionMapIn, zk)

	// Exclude any explicit exclusions.
	excluded := removeTopics(partitionMapIn, Config.topicsExclude)

	// Print topics matched to input params.
	printTopics(partitionMapIn)

	// Print if any topics were excluded due to pending deletion.
	printExcludedTopics(pending, excluded)

	// Get a broker map.
	brokersIn := kafkazk.BrokerMapFromPartitionMap(partitionMapIn, brokerMeta, false)

	// Validate all broker params, get a copy of the
	// broker IDs targeted for partition offloading.
	offloadTargets := validateBrokersForScale(cmd, brokersIn, brokerMeta)

	// Sort offloadTargets by storage free ascending.
	sort.Sort(offloadTargetsBySize{t: offloadTargets, bm: brokersIn})

	partitionLimit, _ := cmd.Flags().GetInt("partition-limit")
	partitionSizeThreshold, _ := cmd.Flags().GetInt("partition-size-threshold")

	otm := map[int]struct{}{}
	for _, id := range offloadTargets {
		otm[id] = struct{}{}
	}

	results := make(chan reassignmentBundle, 100)
	wg := &sync.WaitGroup{}

	// Compute a reassignmentBundle output for all tolerance
	// values 0.01..0.99 in parallel.
	for i := 0.01; i < 0.99; i += 0.01 {
		// Whether we're using a fixed tolerance
		// (non 0.00) set via flag or an iterative value.
		tolFlag, _ := cmd.Flags().GetFloat64("tolerance")
		var tol float64

		if tolFlag == 0.00 {
			tol = i
		} else {
			tol = tolFlag
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			partitionMap := partitionMapIn.Copy()

			// Bundle planRelocationsForBrokerParams.
			params := planRelocationsForBrokerParams{
				relos:                  map[int][]relocation{},
				mappings:               partitionMap.Mappings(),
				brokers:                brokersIn.Copy(),
				partitionMeta:          partitionMeta,
				plan:                   relocationPlan{},
				topPartitionsLimit:     partitionLimit,
				partitionSizeThreshold: partitionSizeThreshold,
				offloadTargetsMap:      otm,
				tolerance:              tol,
			}

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

			// Insert the reassignmentBundle.
			results <- reassignmentBundle{
				storageRange: params.brokers.StorageRange(),
				stdDev:       params.brokers.StorageStdDev(),
				tolerance:    tol,
				partitionMap: partitionMap,
				relocations:  params.relos,
				brokers:      params.brokers,
			}

		}()

		// Break early if we're using a fixed tolerance value.
		if tolFlag != 0.00 {
			break
		}
	}

	wg.Wait()
	close(results)

	// Merge all results into a slice.
	resultsByRange := []reassignmentBundle{}
	for r := range results {
		resultsByRange = append(resultsByRange, r)
	}

	// Sort the scale results by range ascending.
	sort.Slice(resultsByRange, func(i, j int) bool {
		switch {
		case resultsByRange[i].storageRange < resultsByRange[j].storageRange:
			return true
		case resultsByRange[i].storageRange > resultsByRange[j].storageRange:
			return false
		}

		return resultsByRange[i].stdDev < resultsByRange[j].stdDev
	})

	// Chose the results with the lowest range.
	m := resultsByRange[0]
	partitionMapOut, brokersOut, relos := m.partitionMap, m.brokers, m.relocations

	// Print parameters used for scale decisions.
	printScaleParams(cmd, resultsByRange, brokersIn, m.tolerance)

	// Print planned relocations.
	printPlannedRelocations(offloadTargets, relos, partitionMeta)

	// Print map change results.
	printMapChanges(partitionMapIn, partitionMapOut)

	// Print broker assignment statistics.
	errs := printBrokerAssignmentStats(cmd, partitionMapIn, partitionMapOut, brokersIn, brokersOut)

	// Handle errors that are possible
	// to be overridden by the user (aka
	// 'WARN' in topicmappr console output).
	handleOverridableErrs(cmd, errs)

	// Ignore no-ops; scales will naturally have
	// a high percentage of these.
	partitionMapIn, partitionMapOut = skipReassignmentNoOps(partitionMapIn, partitionMapOut)

	// Write maps.
	writeMaps(cmd, partitionMapOut, nil)
}
