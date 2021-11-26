package commands

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/DataDog/kafka-kit/v3/kafkazk"

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
	rebalanceCmd.Flags().String("topics-exclude", "", "Exclude topics")
	rebalanceCmd.Flags().String("out-path", "", "Path to write output map files to")
	rebalanceCmd.Flags().String("out-file", "", "If defined, write a combined map of all topics to a file")
	rebalanceCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to ('-1' for all currently mapped brokers, '-2' for all brokers in cluster)")
	rebalanceCmd.Flags().Float64("storage-threshold", 0.20, "Percent below the harmonic mean storage free to target for partition offload (0 targets a brokers)")
	rebalanceCmd.Flags().Float64("storage-threshold-gb", 0.00, "Storage free in gigabytes to target for partition offload (those below the specified value); 0 [default] defers target selection to --storage-threshold")
	rebalanceCmd.Flags().Float64("tolerance", 0.0, "Percent distance from the mean storage free to limit storage scheduling (0 performs automatic tolerance selection)")
	rebalanceCmd.Flags().Int("partition-limit", 30, "Limit the number of top partitions by size eligible for relocation per broker")
	rebalanceCmd.Flags().Int("partition-size-threshold", 512, "Size in megabytes where partitions below this value will not be moved in a rebalance")
	rebalanceCmd.Flags().Bool("locality-scoped", false, "Ensure that all partition movements are scoped by rack.id")
	rebalanceCmd.Flags().Bool("verbose", false, "Verbose output")
	rebalanceCmd.Flags().Int("metrics-age", 60, "Kafka metrics age tolerance (in minutes)")
	rebalanceCmd.Flags().Bool("optimize-leadership", false, "Rebalance all broker leader/follower ratios")

	// Required.
	rebalanceCmd.MarkFlagRequired("brokers")
	rebalanceCmd.MarkFlagRequired("topics")
}

func rebalance(cmd *cobra.Command, _ []string) {
	bootstrap(cmd)

	// ZooKeeper init.
	zkAddr := cmd.Parent().Flag("zk-addr").Value.String()
	kafkaPrefix := cmd.Parent().Flag("zk-prefix").Value.String()
	metricsPrefix := cmd.Flag("zk-metrics-prefix").Value.String()
	zk, err := initZooKeeper(zkAddr, kafkaPrefix, metricsPrefix)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	defer zk.Close()

	// Get broker and partition metadata.
	maxMetadataAge, _ := cmd.Flags().GetInt("metrics-age")
	if err := checkMetaAge(zk, maxMetadataAge); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	brokerMeta, errs := getBrokerMeta(zk, true)
	if errs != nil && brokerMeta == nil {
		for _, e := range errs {
			fmt.Println(e)
		}
		os.Exit(1)
	}
	partitionMeta, err := getPartitionMeta(zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Get the current partition map.
	partitionMapIn, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Exclude any topics that are pending deletion.
	pending, err := stripPendingDeletes(partitionMapIn, zk)
	if err != nil {
		fmt.Println("Error fetching topics pending deletion")
	}

	// Exclude any explicit exclusions.
	excluded := removeTopics(partitionMapIn, Config.topicsExclude)

	// Print topics matched to input params.
	printTopics(partitionMapIn)

	// Print if any topics were excluded due to pending deletion.
	printExcludedTopics(pending, excluded)

	// Get a broker map.
	brokersIn := kafkazk.BrokerMapFromPartitionMap(partitionMapIn, brokerMeta, false)

	// Validate all broker params, get a copy of the broker IDs targeted for
	// partition offloading.
	offloadTargets := validateBrokersForRebalance(cmd, brokersIn, brokerMeta)

	// Sort offloadTargets by storage free ascending.
	sort.Sort(offloadTargetsBySize{t: offloadTargets, bm: brokersIn})

	partitionLimit, _ := cmd.Flags().GetInt("partition-limit")
	partitionSizeThreshold, _ := cmd.Flags().GetInt("partition-size-threshold")
	tolerance, _ := cmd.Flags().GetFloat64("tolerance")
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")
	verbose, _ := cmd.Flags().GetBool("verbose")

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

	// Generate reassignmentBundles for a rebalance.
	results := computeReassignmentBundles(params)

	// Merge all results into a slice.
	resultsByRange := []reassignmentBundle{}
	for r := range results {
		resultsByRange = append(resultsByRange, r)
	}

	// Sort the rebalance results by range ascending.
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

	// Print parameters used for rebalance decisions.
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
	errs = printBrokerAssignmentStats(cmd, partitionMapIn, partitionMapOut, brokersIn, brokersOut)

	// Handle errors that are possible to be overridden by the user (aka 'WARN'
	// in topicmappr console output).
	handleOverridableErrs(cmd, errs)

	// Ignore no-ops; rebalances will naturally have a high percentage of these.
	partitionMapIn, partitionMapOut = skipReassignmentNoOps(partitionMapIn, partitionMapOut)

	// Write maps.
	writeMaps(cmd, partitionMapOut, nil)
}

func validateBrokersForRebalance(cmd *cobra.Command, brokers kafkazk.BrokerMap, bm kafkazk.BrokerMetaMap) []int {
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
	if errs := ensureBrokerMetrics(brokers, bm); len(errs) > 0 {
		for _, e := range errs {
			fmt.Println(e)
		}
		os.Exit(1)
	}

	switch {
	case c.Missing > 0, c.OldMissing > 0, c.Replace > 0:
		fmt.Printf("%s[ERROR] rebalance only allows broker additions\n", indent)
		os.Exit(1)
	case c.New > 0:
		fmt.Printf("%s%d additional brokers added\n", indent, c.New)
		fmt.Printf("%s-\n", indent)
		fallthrough
	default:
		fmt.Printf("%sOK\n", indent)
	}

	st, _ := cmd.Flags().GetFloat64("storage-threshold")
	stg, _ := cmd.Flags().GetFloat64("storage-threshold-gb")

	var selectorMethod bytes.Buffer
	selectorMethod.WriteString("Brokers targeted for partition offloading ")

	var offloadTargets []int

	// Switch on the target selection method. If a storage threshold in gigabytes
	// is specified, prefer this. Otherwise, use the percentage below mean threshold.
	switch {
	case stg > 0.00:
		selectorMethod.WriteString(fmt.Sprintf("(< %.2fGB storage free)", stg))

		// Get all non-new brokers with a StorageFree below the storage threshold in GB.
		f := func(b *kafkazk.Broker) bool {
			if !b.New && b.StorageFree < stg*div {
				return true
			}
			return false
		}

		matches := brokers.Filter(f)
		for _, b := range matches {
			offloadTargets = append(offloadTargets, b.ID)
		}

		sort.Ints(offloadTargets)
	default:
		selectorMethod.WriteString(fmt.Sprintf("(>= %.2f%% threshold below hmean)", st*100))

		// Find brokers where the storage free is t % below the harmonic mean.
		// Specifying 0 targets all non-new brokers.
		switch st {
		case 0.00:
			f := func(b *kafkazk.Broker) bool { return !b.New }

			matches := brokers.Filter(f)
			for _, b := range matches {
				offloadTargets = append(offloadTargets, b.ID)
			}

			sort.Ints(offloadTargets)
		default:
			offloadTargets = brokers.BelowMean(st, brokers.HMean)
		}
	}

	fmt.Printf("\n%s:\n", selectorMethod.String())

	// Exit if no target brokers were found.
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
