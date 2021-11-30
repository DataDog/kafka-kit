package commands

import (
	"fmt"
	"os"
	"regexp"

	"github.com/DataDog/kafka-kit/v3/kafkazk"

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
	rebuildCmd.Flags().String("topics-exclude", "", "Exclude topics")
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
	rebuildCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to ('-1' for all currently mapped brokers, '-2' for all brokers in cluster)")
	rebuildCmd.Flags().Int("metrics-age", 60, "Kafka metrics age tolerance (in minutes) (when using storage placement)")
	rebuildCmd.Flags().Bool("skip-no-ops", false, "Skip no-op partition assigments")
	rebuildCmd.Flags().Bool("optimize-leadership", false, "Rebalance all broker leader/follower ratios")
	rebuildCmd.Flags().Bool("phased-reassignment", false, "Create two-phase output maps")

	// new params for evac leadership
	rebuildCmd.Flags().String("leader-evac-brokers", "", "Broker list to remove leadership for topics in leader-evac-topics.")
	rebuildCmd.Flags().String("leader-evac-topics", "", "Topics list to remove leadership for the brokers given in leader-evac-brokers")

	// Required.
	rebuildCmd.MarkFlagRequired("brokers")
}

type rebuildParams struct {
	brokers             []int
	forceRebuild        bool
	mapString           string
	maxMetadataAge      int
	minRackIds          int
	optimize            string
	optimizeLeadership  bool
	partitionSizeFactor float64
	phasedReassignment  bool
	placement           string
	replication         int
	skipNoOps           bool
	subAffinity         bool
	topics              []*regexp.Regexp
	topicsExclude       []*regexp.Regexp
	useMetadata         bool
	leaderEvacTopics    []*regexp.Regexp
	leaderEvacBrokers   []int
}

func rebuildParamsFromCmd(cmd *cobra.Command) (c rebuildParams) {
	brokers, _ := cmd.Flags().GetString("brokers")
	c.brokers = brokerStringToSlice(brokers)
	forceRebuild, _ := cmd.Flags().GetBool("force-rebuild")
	c.forceRebuild = forceRebuild
	mapString, _ := cmd.Flags().GetString("map-string")
	c.mapString = mapString
	maxMetadataAge, _ := cmd.Flags().GetInt("metrics-age")
	c.maxMetadataAge = maxMetadataAge
	minRackIds, _ := cmd.Flags().GetInt("min-rack-ids")
	c.minRackIds = minRackIds
	optimize, _ := cmd.Flags().GetString("optimize")
	c.optimize = optimize
	optimizeLeadership, _ := cmd.Flags().GetBool("optimize-leadership")
	c.optimizeLeadership = optimizeLeadership
	partitionSizeFactor, _ := cmd.Flags().GetFloat64("partition-size-factor")
	c.partitionSizeFactor = partitionSizeFactor
	phasedReassignment, _ := cmd.Flags().GetBool("phased-reassignment")
	c.phasedReassignment = phasedReassignment
	placement, _ := cmd.Flags().GetString("placement")
	c.placement = placement
	replication, _ := cmd.Flags().GetInt("replication")
	c.replication = replication
	skipNoOps, _ := cmd.Flags().GetBool("skip-no-ops")
	c.skipNoOps = skipNoOps
	subAffinity, _ := cmd.Flags().GetBool("sub-affinity")
	c.subAffinity = subAffinity
	topics, _ := cmd.Flags().GetString("topics")
	c.topics = topicRegex(topics)
	topicsExclude, _ := cmd.Flags().GetString("topics-exclude")
	c.topicsExclude = topicRegex(topicsExclude)
	useMetadata, _ := cmd.Flags().GetBool("use-meta")
	c.useMetadata = useMetadata
	let, _ := cmd.Flags().GetString("leader-evac-topics")
	if let != "" {
		c.leaderEvacTopics = topicRegex(let)
	}
	leb, _ := cmd.Flags().GetString("leader-evac-brokers")
	if leb != "" {
		c.leaderEvacBrokers = brokerStringToSlice(leb)
	}
	return c
}

func (c rebuildParams) validate() error {
	switch {
	case c.mapString == "" && len(c.topics) == 0:
		return fmt.Errorf("\n[ERROR] must specify either --topics or --map-string")
	case c.placement != "count" && c.placement != "storage":
		return fmt.Errorf("\n[ERROR] --placement must be either 'count' or 'storage'")
	case c.optimize != "distribution" && c.optimize != "storage":
		return fmt.Errorf("\n[ERROR] --optimize must be either 'distribution' or 'storage'")
	case !c.useMetadata && c.placement == "storage":
		return fmt.Errorf("\n[ERROR] --placement=storage requires --use-meta=true")
	case c.forceRebuild && c.subAffinity:
		return fmt.Errorf("\n[INFO] --force-rebuild disables --sub-affinity")
	case (len(c.leaderEvacBrokers) != 0 || len(c.leaderEvacTopics) != 0) && (len(c.leaderEvacBrokers) == 0 || len(c.leaderEvacTopics) == 0):
		return fmt.Errorf("\n[ERROR] --leader-evac-topics and --leader-evac-brokers must both be specified for leadership evacuation.")
	}
	return nil
}

func rebuild(cmd *cobra.Command, _ []string) {
	bootstrap(cmd)
	params := rebuildParamsFromCmd(cmd)
	err := params.validate()
	if err != nil {
		fmt.Println(err)
		defaultsAndExit()
	}
	if params.forceRebuild && params.subAffinity {
		fmt.Println("\n[INFO] --force-rebuild disables --sub-affinity")
	}

	// ZooKeeper init.
	var zk kafkazk.Handler
	if params.useMetadata || len(params.topics) > 0 || params.placement == "storage" {
		zkAddr := cmd.Parent().Flag("zk-addr").Value.String()
		kafkaPrefix := cmd.Parent().Flag("zk-prefix").Value.String()
		metricsPrefix := cmd.Flag("zk-metrics-prefix").Value.String()
		zk, err = initZooKeeper(zkAddr, kafkaPrefix, metricsPrefix)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		defer zk.Close()
	}

	// In addition to the global topic regex, we have leader-evac topic regex as well.
	var evacTopics []string
	if len(params.leaderEvacTopics) != 0 {
		evacTopics, err = zk.GetTopics(params.leaderEvacTopics)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
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
	if params.placement == "storage" {
		if err := checkMetaAge(zk, params.maxMetadataAge); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		withMetrics = true
	}

	var brokerMeta kafkazk.BrokerMetaMap
	var errs []error
	if params.useMetadata {
		if brokerMeta, errs = getBrokerMeta(zk, withMetrics); errs != nil && brokerMeta == nil {
			for _, e := range errs {
				fmt.Println(e)
			}
			os.Exit(1)
		}
	}

	// Fetch partition metadata.
	var partitionMeta kafkazk.PartitionMetaMap
	if params.placement == "storage" {
		if partitionMeta, err = getPartitionMeta(zk); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	// Build a partition map either from literal map text input or by fetching the
	// map data from ZooKeeper. Store a copy of the original.
	partitionMapIn, pending, excluded := getPartitionMap(params, zk)
	originalMap := partitionMapIn.Copy()

	// Get a list of affected topics.
	printTopics(partitionMapIn)

	// Print if any topics were excluded due to pending deletion or explicit
	// exclusion.
	printExcludedTopics(pending, excluded)

	brokers, bs := getBrokers(params, partitionMapIn, brokerMeta)
	brokersOrig := brokers.Copy()

	if bs.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	// Check if any referenced brokers are marked as having
	// missing/partial metrics data.
	if params.useMetadata {
		if errs := ensureBrokerMetrics(brokers, brokerMeta); len(errs) > 0 {
			for _, e := range errs {
				fmt.Println(e)
			}
			os.Exit(1)
		}
	}

	// Create substitution affinities.
	affinities := getSubAffinities(params, brokers, brokersOrig, partitionMapIn)

	if affinities != nil {
		fmt.Printf("%s-\n", indent)
	}

	// Print changes, actions.
	printChangesActions(params, bs)

	// Apply any replication factor settings.
	updateReplicationFactor(params, partitionMapIn)

	// Build a new map using the provided list of brokers. This is OK to run even
	// when a no-op is intended.
	partitionMapOut, errs := buildMap(params, partitionMapIn, partitionMeta, brokers, affinities)

	// Optimize leaders.
	if params.optimizeLeadership {
		partitionMapOut.OptimizeLeaderFollower()
	}

	// Count missing brokers as a warning.
	if bs.Missing > 0 {
		errs = append(errs, fmt.Errorf("%d provided brokers not found in ZooKeeper", bs.Missing))
	}

	// Count missing rack info as warning
	if bs.RackMissing > 0 {
		errs = append(
			errs, fmt.Errorf("%d provided broker(s) do(es) not have a rack.id defined", bs.RackMissing),
		)
	}

	// Generate phased map if enabled.
	var phasedMap *kafkazk.PartitionMap
	if params.phasedReassignment {
		phasedMap = phasedReassignment(originalMap, partitionMapOut)
	}

	partitionMapOut = evacuateLeadership(*partitionMapOut, params.leaderEvacBrokers, evacTopics)

	// Print map change results.
	printMapChanges(originalMap, partitionMapOut)

	// Print broker assignment statistics.
	printBrokerAssignmentStats(cmd, originalMap, partitionMapOut, brokersOrig, brokers)

	// Print error/warnings.
	handleOverridableErrs(cmd, errs)

	// Skip no-ops if configured.
	if params.skipNoOps {
		originalMap, partitionMapOut = skipReassignmentNoOps(originalMap, partitionMapOut)
	}

	outPath := cmd.Flag("out-path").Value.String()
	outFile := cmd.Flag("out-file").Value.String()
	writeMaps(outPath, outFile, partitionMapOut, phasedMap)
}
