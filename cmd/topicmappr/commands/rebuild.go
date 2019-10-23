package commands

import (
	"fmt"
	"os"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

var rebuildCmd = &cobra.Command{
	Use:   "rebuild",
	Short: "Rebuild a partition map for one or more topics",
	Long: `rebuild requires at least two inputs: a reference of
target topics and a list of broker IDs to which those topics should be mapped.
Target topics are provided as a comma delimited list of topic names and/or regex patterns
via the --topics parameter, which discovers matching topics in ZooKeeper (additionally,
the --zk-addr and --zk-prefix global flags should be set). Alternatively, a JSON map can be
provided via the --map-string flag. Target broker IDs are provided via the --broker flag.`,
	Run: rebuild,
}

func init() {
	rootCmd.AddCommand(rebuildCmd)

	rebuildCmd.Flags().String("topics", "", "Rebuild topics (comma delim. list) by lookup in ZooKeeper")
	rebuildCmd.Flags().String("map-string", "", "Rebuild a partition map provided as a string literal")
	rebuildCmd.Flags().Bool("use-meta", true, "Use broker metadata in placement constraints")
	rebuildCmd.Flags().String("out-path", "", "Path to write output map files to")
	rebuildCmd.Flags().String("out-file", "", "If defined, write a combined map of all topics to a file")
	rebuildCmd.Flags().Bool("force-rebuild", false, "Forces a complete map rebuild")
	rebuildCmd.Flags().Int("replication", 0, "Normalize the topic replication factor across all replica sets (0 results in a no-op)")
	rebuildCmd.Flags().Bool("sub-affinity", false, "Replacement broker substitution affinity")
	rebuildCmd.Flags().String("placement", "count", "Partition placement strategy: [count, storage]")
	rebuildCmd.Flags().Int("min-rack-ids", 0, "Minimum number of required of unique rack IDs per replica set (0 requires that all are unique)")
	rebuildCmd.Flags().String("optimize", "distribution", "Optimization priority for the storage placement strategy: [distribution, storage]")
	rebuildCmd.Flags().Float64("partition-size-factor", 1.0, "Factor by which to multiply partition sizes when using storage placement")
	rebuildCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to ('-1' automatically expands to all currently mapped brokers)")
	rebuildCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics (when using storage placement)")
	rebuildCmd.Flags().Int("metrics-age", 60, "Kafka metrics age tolerance (in minutes) (when using storage placement)")
	rebuildCmd.Flags().Bool("skip-no-ops", false, "Skip no-op partition assigments")
	rebuildCmd.Flags().Bool("optimize-leadership", false, "Rebalance all broker leader/follower ratios")

	// Required.
	rebuildCmd.MarkFlagRequired("brokers")
}

func rebuild(cmd *cobra.Command, _ []string) {
	// Sanity check params.
	t, _ := cmd.Flags().GetString("topics")
	ms, _ := cmd.Flags().GetString("map-string")
	p := cmd.Flag("placement").Value.String()
	o := cmd.Flag("optimize").Value.String()
	fr, _ := cmd.Flags().GetBool("force-rebuild")
	sa, _ := cmd.Flags().GetBool("sub-affinity")
	m, _ := cmd.Flags().GetBool("use-meta")

	switch {
	case ms == "" && t == "":
		fmt.Println("\n[ERROR] must specify either --topics or --map-string")
		defaultsAndExit()
	case p != "count" && p != "storage":
		fmt.Println("\n[ERROR] --placement must be either 'count' or 'storage'")
		defaultsAndExit()
	case o != "distribution" && o != "storage":
		fmt.Println("\n[ERROR] --optimize must be either 'distribution' or 'storage'")
		defaultsAndExit()
	case !m && p == "storage":
		fmt.Println("\n[ERROR] --placement=storage requires --use-meta=true")
		defaultsAndExit()
	case fr && sa:
		fmt.Println("\n[INFO] --force-rebuild disables --sub-affinity")
	}

	bootstrap(cmd)

	// ZooKeeper init.
	var zk kafkazk.Handler
	if m || len(Config.topics) > 0 || p == "storage" {
		var err error
		zk, err = initZooKeeper(cmd)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer zk.Close()
	}

	// General flow:
	// 1) A PartitionMap is formed (either unmarshaled from the literal
	//   map input via --rebuild-map or generated from ZooKeeper Metadata
	//   for topics matching --topics).
	// 2) A BrokerMap is formed from brokers found in the PartitionMap
	//   along with any new brokers provided via the --brokers param.
	// 3) The PartitionMap and BrokerMap are fed to a rebuild
	//   function. Missing brokers, brokers marked for replacement,
	//   and all other placements are performed, returning a new
	//   PartitionMap.
	// 4) Differences between the original and new PartitionMap
	//   are detected and reported.
	// 5) The new PartitionMap is split by topic. Map(s) are written.

	// Fetch broker metadata.
	var withMetrics bool
	if cmd.Flag("placement").Value.String() == "storage" {
		checkMetaAge(cmd, zk)
		withMetrics = true
	}

	var brokerMeta kafkazk.BrokerMetaMap
	if m, _ := cmd.Flags().GetBool("use-meta"); m {
		brokerMeta = getBrokerMeta(cmd, zk, withMetrics)
	}

	// Fetch partition metadata.
	var partitionMeta kafkazk.PartitionMetaMap
	if cmd.Flag("placement").Value.String() == "storage" {
		partitionMeta = getPartitionMeta(cmd, zk)
	}

	// Build a partition map either from literal map text input or by fetching the
	// map data from ZooKeeper. Store a copy of the original.
	partitionMapIn, pending := getPartitionMap(cmd, zk)
	originalMap := partitionMapIn.Copy()

	// Get a list of affected topics.
	printTopics(partitionMapIn)

	// Print if any topics were excluded due to pending deletion.
	printExcludedTopics(pending)

	brokers, bs := getBrokers(cmd, partitionMapIn, brokerMeta)
	brokersOrig := brokers.Copy()

	if bs.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	// Check if any referenced brokers are marked as having
	// missing/partial metrics data.
	if m, _ := cmd.Flags().GetBool("use-meta"); m {
		ensureBrokerMetrics(cmd, brokers, brokerMeta)
	}

	// Create substitution affinities.
	affinities := getSubAffinities(cmd, brokers, brokersOrig, partitionMapIn)

	if affinities != nil {
		fmt.Printf("%s-\n", indent)
	}

	// Print changes, actions.
	printChangesActions(cmd, bs)

	// Apply any replication factor settings.
	updateReplicationFactor(cmd, partitionMapIn)

	// Build a new map using the provided list of brokers.
	// This is OK to run even when a no-op is intended.
	partitionMapOut, errs := buildMap(cmd, partitionMapIn, partitionMeta, brokers, affinities)

	// Optimize leaders.
	if t, _ := cmd.Flags().GetBool("optimize-leadership"); t {
		partitionMapOut.OptimizeLeaderFollower()
	}

	// Count missing brokers as a warning.
	if bs.Missing > 0 {
		errs = append(errs, fmt.Errorf("%d provided brokers not found in ZooKeeper", bs.Missing))
	}

	// Print map change results.
	printMapChanges(originalMap, partitionMapOut)

	// Print broker assignment statistics.
	printBrokerAssignmentStats(cmd, originalMap, partitionMapOut, brokersOrig, brokers)

	// Print error/warnings.
	handleOverridableErrs(cmd, errs)

	// Skip no-ops if configured.
	if sno, _ := cmd.Flags().GetBool("skip-no-ops"); sno {
		originalMap, partitionMapOut = skipReassignmentNoOps(originalMap, partitionMapOut)
	}

	writeMaps(cmd, partitionMapOut)
}
