package commands

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	"github.com/DataDog/kafka-kit/v4/kafkazk"

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
	rebuildCmd.Flags().String("leader-evac-brokers", "", "Broker list to remove leadership for topics in leader-evac-topics.")
	rebuildCmd.Flags().String("leader-evac-topics", "", "Topics list to remove leadership for the brokers given in leader-evac-brokers")
	rebuildCmd.Flags().Int("chunk-step-size", 0, "Number of brokers to move data at a time for with a chunked operation.")

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
	topics              []string
	topicsExclude       []*regexp.Regexp
	useMetadata         bool
	leaderEvacTopics    []string
	leaderEvacBrokers   []int
	chunkStepSize       int
}

func rebuildParamsFromCmd(cmd *cobra.Command) (params rebuildParams) {
	brokers, _ := cmd.Flags().GetString("brokers")
	params.brokers = brokerStringToSlice(brokers)
	forceRebuild, _ := cmd.Flags().GetBool("force-rebuild")
	params.forceRebuild = forceRebuild
	mapString, _ := cmd.Flags().GetString("map-string")
	params.mapString = mapString
	maxMetadataAge, _ := cmd.Flags().GetInt("metrics-age")
	params.maxMetadataAge = maxMetadataAge
	minRackIds, _ := cmd.Flags().GetInt("min-rack-ids")
	params.minRackIds = minRackIds
	optimize, _ := cmd.Flags().GetString("optimize")
	params.optimize = optimize
	optimizeLeadership, _ := cmd.Flags().GetBool("optimize-leadership")
	params.optimizeLeadership = optimizeLeadership
	partitionSizeFactor, _ := cmd.Flags().GetFloat64("partition-size-factor")
	params.partitionSizeFactor = partitionSizeFactor
	phasedReassignment, _ := cmd.Flags().GetBool("phased-reassignment")
	params.phasedReassignment = phasedReassignment
	placement, _ := cmd.Flags().GetString("placement")
	params.placement = placement
	replication, _ := cmd.Flags().GetInt("replication")
	params.replication = replication
	skipNoOps, _ := cmd.Flags().GetBool("skip-no-ops")
	params.skipNoOps = skipNoOps
	subAffinity, _ := cmd.Flags().GetBool("sub-affinity")
	params.subAffinity = subAffinity
	topics, _ := cmd.Flags().GetString("topics")
	params.topics = strings.Split(topics, ",")
	topicsExclude, _ := cmd.Flags().GetString("topics-exclude")
	params.topicsExclude = topicRegex(topicsExclude)
	useMetadata, _ := cmd.Flags().GetBool("use-meta")
	params.useMetadata = useMetadata
	chunkStepSize, _ := cmd.Flags().GetInt("chunk-step-size")
	params.chunkStepSize = chunkStepSize
	let, _ := cmd.Flags().GetString("leader-evac-topics")
	if let != "" {
		params.leaderEvacTopics = strings.Split(let, ",")
	}
	leb, _ := cmd.Flags().GetString("leader-evac-brokers")
	if leb != "" {
		params.leaderEvacBrokers = brokerStringToSlice(leb)
	}
	return params
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
	sanitizeInput(cmd)
	params := rebuildParamsFromCmd(cmd)

	err := params.validate()
	if err != nil {
		fmt.Println(err)
		defaultsAndExit()
	}
	if params.forceRebuild && params.subAffinity {
		fmt.Println("\n[INFO] --force-rebuild disables --sub-affinity")
	}

	// Init kafkaadmin client.
	ka, err := newKafkaAdminClient(cmd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
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

	maps, errs := runRebuild(params, ka, zk)

	// Print error/warnings.
	handleOverridableErrs(cmd, errs)

	outPath := cmd.Flag("out-path").Value.String()
	outFile := cmd.Flag("out-file").Value.String()
	writeMaps(outPath, outFile, maps)
}
