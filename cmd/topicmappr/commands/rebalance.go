package commands

import (
	"fmt"
	"os"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

var rebalanceCmd = &cobra.Command{
	Use:   "rebalance",
	Short: "Rebalance a partition map for one or more topics",
	Long:  `Rebalance`,
	Run:   rebalance,
}

func init() {
	rootCmd.AddCommand(rebalanceCmd)

	rebalanceCmd.Flags().String("topics", "", "Rebuild topics (comma delim. list) by lookup in ZooKeeper")
	rebalanceCmd.Flags().String("out-path", "", "Path to write output map files to")
	rebalanceCmd.Flags().String("out-file", "", "If defined, write a combined map of all topics to a file")
	rebalanceCmd.Flags().String("brokers", "", "Broker list to scope all partition placements to")
	rebalanceCmd.Flags().Float64("storage-threshold", 0.20, "storage-threshold")
	rebalanceCmd.Flags().String("zk-metrics-prefix", "topicmappr", "ZooKeeper namespace prefix for Kafka metrics (when using storage placement)")

	// Required.
	rebalanceCmd.MarkFlagRequired("brokers")
}

func rebalance(cmd *cobra.Command, _ []string) {
	bootstrap(cmd)

	// ZooKeeper init.
	zk, err := initZooKeeper(cmd)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer zk.Close()

	// Get broker and partition metadata.
	brokerMeta := getBrokerMeta(cmd, zk, true)
	partitionMeta, err := getPartitionMeta(cmd, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	_ = partitionMeta

	// Get the current partition map.
	pm, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	brokers := kafkazk.BrokerMapFromPartitionMap(pm, brokerMeta, false)

	// Update the currentBrokers list with
	// the provided broker list.
	bs := brokers.Update(Config.brokers, brokerMeta)
	fmt.Printf("%+v\n", bs)

	BrokerList := brokers.List()
	BrokerList.SortByStorage()

	// Find brokers where the storage utilization is d %
	// above the harmonic mean.
	t, _ := cmd.Flags().GetFloat64("storage-threshold")
	fmt.Println(brokers.BelowMean(t))
}
