package commands

import (
	"fmt"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/spf13/cobra"
	"os"
)

package commands

import (
"fmt"
"os"

"github.com/DataDog/kafka-kit/v3/kafkazk"

"github.com/spf13/cobra"
)

var evacLeadershipCmd = &cobra.Command{
	Use:   "evac-leader",
	Short: "Remove leadership from a set of brokers",
	Long: ``,
	Run: evac-leader,
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
	rebuildCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics (when using storage placement)")
	rebuildCmd.Flags().Int("metrics-age", 60, "Kafka metrics age tolerance (in minutes) (when using storage placement)")
	rebuildCmd.Flags().Bool("skip-no-ops", false, "Skip no-op partition assigments")
	rebuildCmd.Flags().Bool("optimize-leadership", false, "Rebalance all broker leader/follower ratios")
	rebuildCmd.Flags().Bool("phased-reassignment", false, "Create two-phase output maps")

	// Required.
	rebuildCmd.MarkFlagRequired("brokers")
}

func evac_leadership(cmd *cobra.Command, _ []string) {
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
	partitionMapIn, pending, excluded := getPartitionMap(cmd, zk)
	originalMap := partitionMapIn.Copy()

	// Get a list of affected topics.
	printTopics(partitionMapIn)

	// Print if any topics were excluded due to pending deletion or explicit
	// exclusion.
	printExcludedTopics(pending, excluded)

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

	// evacuation algo starts here
	partitionMapOut := partitionMapIn

	// make a lookup map of topics
	topicsMap := map[string]struct{}{}
	for _, topic := range partitionMapIn.Topics() {
		topicsMap[topic] = struct{}{}
	}

	// get the list of brokers to offload leadership from
	var targetBrokers = kafkazk.BrokerMap{}

	// TODO get the list of partitions to offload leadership for

	// We will store any broker/partitions that are issues here
	var problemBrokersPartitions = map[int]kafkazk.Partition{}

	// swap leadership for all broker/partition/topic combos
	// If any partitions don't have a new home, make an error state for it (?)

	for i,p := range partitionMapIn.Partitions {


		// check the topic is one of the target topics
		if _,correctTopic := topicsMap[p.Topic]; !correctTopic {
			continue
		}

		// check the leader to see if its one of the evac brokers
		if _, contains := targetBrokers[p.Replicas[0]]; !contains {
			continue
		}

		// TODO check if the current partition is one we want to move

		for j,replica := range p.Replicas {
			if replica == p.Replicas[len(p.Replicas) -1] {
				problemBrokersPartitions[p.Replicas[0]] = p
				continue
			}
			// If one of the replicas is not being leadership evacuated, use that one and swap.
			if _, contains := targetBrokers[replica]; !contains {
				partitionMapOut.Partitions[i].Replicas[j] = p.Replicas[0]
				partitionMapOut.Partitions[i].Replicas[0] = p.Replicas[j]
			}
		}
	}

	// Build a new map using the provided list of brokers. This is OK to run even
	// when a no-op is intended.
	partitionMapOut, errs := buildMap(cmd, partitionMapIn, partitionMeta, brokers, affinities)

	// Optimize leaders.
	if t, _ := cmd.Flags().GetBool("optimize-leadership"); t {
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
	if phased, _ := cmd.Flags().GetBool("phased-reassignment"); phased {
		phasedMap = phasedReassignment(originalMap, partitionMapOut)
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

	writeMaps(cmd, partitionMapOut, phasedMap)
}
