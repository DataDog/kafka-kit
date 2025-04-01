package commands

import (
	"fmt"
	"os"


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
	sanitizeInput(cmd)
	params := reassignParamsFromCmd(cmd)
	params.requireNewBrokers = false

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

	// Init kafkaadmin client.
	ka, err := newKafkaAdminClient(cmd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	partitionMaps, errs := reassign(params, ka, zk)

	// Handle errors that are possible to be overridden by the user (aka 'WARN'
	// in topicmappr console output).
	handleOverridableErrs(cmd, errs)

	// Write maps.
	outPath := cmd.Flag("out-path").Value.String()
	outFile := cmd.Flag("out-file").Value.String()
	writeMaps(outPath, outFile, partitionMaps)
}
