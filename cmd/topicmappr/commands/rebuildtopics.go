package commands

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

var (
	// Characters allowed in Kafka topic names
	topicNormalChar, _ = regexp.Compile(`[a-zA-Z0-9_\\-]`)

	Config struct {
		rebuildTopics []*regexp.Regexp
		brokers       []int
	}
)

var rebuildTopicsCmd = &cobra.Command{
	Use:   "rebuild-topics",
	Short: "Build a partition map for one or more topics",
	Long: `rebuild-topics requires at least two inputs: a reference of
target topics and a list of broker IDs to which those topics should be mapped.
Target topics are provided as a comma delimited list of topic names and/or regex patterns
via the --topics parameter, which discovers matching topics in ZooKeeper (additionally,
the --zk-addr and --zk-prefix global flags should be set). Alternatively, a JSON map can be
provided via the --map-string flag. Targe broker IDs are provided via the --broker flag.`,
	Run: rebuild,
}

func init() {
	rootCmd.AddCommand(rebuildTopicsCmd)

	rebuildTopicsCmd.Flags().String("topics", "", "Rebuild topics (comma delim. list) by lookup in ZooKeeper")
	rebuildTopicsCmd.Flags().String("map-string", "", "Rebuild a partition map provided as a string literal")
	rebuildTopicsCmd.Flags().Bool("use-meta", true, "Use broker metadata in placement constraints")
	rebuildTopicsCmd.Flags().String("out-path", "", "Path to write output map files to")
	rebuildTopicsCmd.Flags().String("out-file", "", "If defined, write a combined map of all topics to a file")
	rebuildTopicsCmd.Flags().Bool("ignore-warns", false, "Produce a map even if warnings are encountered")
	rebuildTopicsCmd.Flags().Bool("force-rebuild", false, "Forces a complete map rebuild")
	rebuildTopicsCmd.Flags().Int("replication", 0, "Normalize the topic replication factor across all replica sets (0 results in a no-op)")
	rebuildTopicsCmd.Flags().Bool("sub-affinity", false, "Replacement broker substitution affinity")
	rebuildTopicsCmd.Flags().String("placement", "count", "Partition placement strategy: [count, storage]")
	rebuildTopicsCmd.Flags().String("optimize", "distribution", "Optimization priority for the storage placement strategy: [distribution, storage]")
	rebuildTopicsCmd.Flags().Float64("partition-size-factor", 1.0, "Factor by which to multiply partition sizes when using storage placement")
	rebuildTopicsCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to")
	rebuildTopicsCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics (when using storage placement)")

	// Required.
	rebuildTopicsCmd.MarkFlagRequired("brokers")
}

func rebuild(cmd *cobra.Command, _ []string) {
	// Suppress underlying ZK client noise.
	log.SetOutput(ioutil.Discard)

	b, _ := cmd.Flags().GetString("brokers")
	Config.brokers = kafkazk.BrokerStringToSlice(b)
	topics, _ := cmd.Flags().GetString("topics")

	// Sanity check params.

	p := cmd.Flag("placement").Value.String()
	o := cmd.Flag("optimize").Value.String()
	fr, _ := cmd.Flags().GetBool("force-rebuild")
	sa, _ := cmd.Flags().GetBool("sub-affinity")
	m, _ := cmd.Flags().GetBool("use-meta")

	switch {
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

	// Append trailing slash if not included.
	op := cmd.Flag("out-path").Value.String()
	if op != "" && !strings.HasSuffix(op, "/") {
		cmd.Flags().Set("out-path", op+"/")
	}

	// Determine if regexp was provided in the topic
	// name. If not, set the topic name to ^name$.
	topicNames := strings.Split(topics, ",")
	for n, t := range topicNames {
		if !containsRegex(t) {
			topicNames[n] = fmt.Sprintf(`^%s$`, t)
		}
	}

	// Compile topic regex.
	for _, t := range topicNames {
		r, err := regexp.Compile(t)
		if err != nil {
			fmt.Printf("Invalid topic regex: %s\n", t)
			os.Exit(1)
		}

		Config.rebuildTopics = append(Config.rebuildTopics, r)
	}

	// ZooKeeper init.
	zk := initZooKeeper(cmd)
	if zk != nil {
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

	// Fetch broker and partition Metadata.
	brokerMeta := getbrokerMeta(cmd, zk)
	partitionMeta := getPartitionMeta(cmd, zk)

	// Build a partition map either from literal map text input or by fetching the
	// map data from ZooKeeper. Store a copy of the original.
	partitionMapIn := getPartitionMap(cmd, zk)
	originalMap := partitionMapIn.Copy()

	// Get a list of affected topics.
	printTopics(partitionMapIn)

	brokers, bs := getBrokers(cmd, partitionMapIn, brokerMeta)
	brokersOrig := brokers.Copy()

	if bs.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	// Check if any referenced brokers are marked as having
	// missing/partial metrics data.
	ensureBrokerMetrics(cmd, brokers, brokerMeta)

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
	partitionMapOut, warns := buildMap(cmd, partitionMapIn, partitionMeta, brokers, affinities)

	// Sort by topic, partition.
	// TODO all functions should return lex sorted partition maps. Review for
	// removal. Also, partitionMapIn shouldn't be further referenced at this point.
	sort.Sort(partitionMapIn.Partitions)
	sort.Sort(partitionMapOut.Partitions)

	// Count missing brokers as a warning.
	if bs.Missing > 0 {
		w := fmt.Sprintf("%d provided brokers not found in ZooKeeper\n", bs.Missing)
		warns = append(warns, w)
	}

	// Print warnings.
	fmt.Println("\nWARN:")
	if len(warns) > 0 {
		sort.Strings(warns)
		for _, e := range warns {
			fmt.Printf("%s%s\n", indent, e)
		}
	} else {
		fmt.Printf("%s[none]\n", indent)
	}

	// Print map change results.
	printMapChanges(originalMap, partitionMapOut)

	// Print broker assignment statistics.
	printBrokerAssignmentStats(cmd, originalMap, partitionMapOut, brokersOrig, brokers)

	// If no warnings were encountered, write out the output partition map(s).
	iw, _ := cmd.Flags().GetBool("ignore-warns")
	if !iw && len(warns) > 0 {
		fmt.Printf("\n%sWarnings encountered, partition map not created. Override with --ignore-warns.\n", indent)
		os.Exit(1)
	}

	writeMaps(cmd, partitionMapOut)
}
