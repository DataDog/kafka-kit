package commands

import (
	"fmt"
	"os"
	"sort"

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
	maxMetadataAge, _ := cmd.Flags().GetInt("metrics-age")
	checkMetaAge(zk, maxMetadataAge)
	brokerMeta := getBrokerMeta(zk, true)
	partitionMeta := getPartitionMeta(zk)

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
	tolerance, _ := cmd.Flags().GetFloat64("tolerance")
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")
	verbose, _ := cmd.Flags().GetBool("verbose")

	// Generate reassignmentBundles for a scale up.
	params := computeReassignmentBundlesParams{
		offloadTargets:         offloadTargets,
		tolerance:              tolerance,
		partitionMap:           partitionMapIn,
		partitionMeta:          partitionMeta,
		brokerMap:              brokersIn,
		partitionLimit:         partitionLimit,
		partitionSizeThreshold: partitionSizeThreshold,
		localityScoped:         localityScoped,
		verbose:                verbose,
	}

	results := computeReassignmentBundles(params)

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
	printReassignmentParams(cmd, resultsByRange, brokersIn, m.tolerance)

	// Optimize leaders.
	if t, _ := cmd.Flags().GetBool("optimize-leadership"); t {
		partitionMapOut.OptimizeLeaderFollower()
	}

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

func validateBrokersForScale(cmd *cobra.Command, brokers kafkazk.BrokerMap, bm kafkazk.BrokerMetaMap) []int {
	// No broker changes are permitted in rebalance other than new broker additions.
	fmt.Println("\nValidating broker list:")

	// Update the current BrokerList with
	// the provided broker list.
	c, msgs := brokers.Update(Config.brokers, bm)
	for m := range msgs {
		fmt.Printf("%s%s\n", indent, m)
	}

	if c.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	// Check if any referenced brokers are marked as having missing/partial metrics data.
	ensureBrokerMetrics(brokers, bm)

	switch {
	case c.Missing > 0, c.OldMissing > 0, c.Replace > 0:
		fmt.Printf("%s[ERROR] scale only allows broker additions\n", indent)
		os.Exit(1)
	case c.New > 0:
		fmt.Printf("%s%d additional brokers added\n", indent, c.New)
		fmt.Printf("%s-\n", indent)
		fmt.Printf("%sOK\n", indent)
	default:
		fmt.Printf("%s[ERROR] scale requires additional brokers\n", indent)
		os.Exit(1)
	}

	var offloadTargets []int

	// Offload targets are all non-new brokers.
	f := func(b *kafkazk.Broker) bool { return !b.New }

	matches := brokers.Filter(f)
	for _, b := range matches {
		offloadTargets = append(offloadTargets, b.ID)
	}

	sort.Ints(offloadTargets)

	fmt.Println("\nBrokers targeted for partition offloading:")

	// Exit if we've hit insufficient broker counts.
	if len(offloadTargets) == 0 {
		fmt.Printf("%s[none]\n", indent)
		os.Exit(0)
	} else {
		for _, id := range offloadTargets {
			fmt.Printf("%s%d\n", indent, id)
		}
	}

	return offloadTargets
}
