package commands

import (
	"fmt"
	"os"

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
	sanitizeInput(cmd)
	params := reassignParamsFromCmd(cmd)
	params.requireNewBrokers = true

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

	partitionMaps, _ := reassign(params, ka, zk)

	// TODO intentionally not handling the one error that can be returned here
	// right now, but would be better to distinguish errors
	// handleOverridableErrs(cmd, errs)

	outPath := cmd.Flag("out-path").Value.String()
	outFile := cmd.Flag("out-file").Value.String()
	writeMaps(outPath, outFile, partitionMaps)
}
