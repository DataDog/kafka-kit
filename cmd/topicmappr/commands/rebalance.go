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
	rebalanceCmd.Flags().Bool("locality-scoped", true, "Disallow a relocation to traverse rack.id values among brokers")
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

	// Get the current partition map.
	pm, err := kafkazk.PartitionMapFromZK(Config.topics, zk)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Get a mapping of broker IDs to topics, partitions.
	mappings := pm.Mappings()

	// Get a broker map.
	brokers := kafkazk.BrokerMapFromPartitionMap(pm, brokerMeta, false)

	// Update the currentBrokers list with
	// the provided broker list.
	// TODO we should only take New brokers in a rebalance.
	bs := brokers.Update(Config.brokers, brokerMeta)
	fmt.Printf("%+v\n", bs)

	brokerList := brokers.List()

	// Find brokers where the storage utilization is d %
	// above the harmonic mean.
	t, _ := cmd.Flags().GetFloat64("storage-threshold")
	offloadTargets := brokers.BelowMean(t, brokers.HMean)

	// Storage the StorageFree harmonic mean.
	meanStorageFree := brokers.HMean()

	var div = 1073741824.00
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")

	fmt.Printf("HMean: %.2fGB\n", meanStorageFree/div)

	fmt.Printf("Brokers targeted for partition offloading: %v\n", offloadTargets)

	// Iterate over offload target brokers.
	for _, br := range offloadTargets {

		// Get the top 5 partitions for the target broker.
		topPartn, _ := mappings.LargestPartitions(br, 5, partitionMeta)

		fmt.Printf("\nBroker %d has a storage free of %.2fGB. Top partitions:\n",
			br, brokers[br].StorageFree/div)

		for _, p := range topPartn {
			pSize, _ := partitionMeta.Size(p)
			fmt.Printf("%stopic: %s, partition: %d, size: %.2fGB\n",
				indent, p.Topic, p.Partition, pSize/div)
		}

		targetLocality := brokers[br].Locality
		partnsToMove := map[int][]kafkazk.Partition{}

		// Plan partition movements.
		for _, p := range topPartn {
			pSize, _ := partitionMeta.Size(p)

			// Find a destination broker.
			var destinationTarget *kafkazk.Broker

			// Sort brokerList by storage.
			brokerList.SortByStorage()

			// Whether or not the destination broker have the same rack.id
			// as the target. If so, choose the lowest utilized broker in
			// same locality. If not, choose the lowest utilized broker.
			switch localityScoped {
			case true:
				for _, b := range brokers {
					if b.Locality == targetLocality {
						destinationTarget = b
						break
					}
				}
			default:
				destinationTarget = brokerList[0]
			}

			fmt.Printf("%sDestination %d free: %.2fGB\n",
				indent, destinationTarget.ID, destinationTarget.StorageFree/div)

			sourceFree := brokers[br].StorageFree + pSize
			destFree := destinationTarget.StorageFree - pSize

			fmt.Printf("%s%d estimated %.2fGB -> %.2fGB\n",
				indent, destinationTarget.ID, destinationTarget.StorageFree/div, destFree/div)

			if sourceFree <= meanStorageFree && destFree > meanStorageFree {
				partnsToMove[br] = append(partnsToMove[br], p)
			}
		}

		fmt.Printf("%sPartitions to move for %d: %v\n",
			indent, br, partnsToMove[br])

		// fmt.Printf("%sMoving partition %s:%d from %d -> %d\n",
		// 	indent, partitionToMove.Topic, partitionToMove.Partition, br, destinationTarget.ID)
		//
		// fmt.Printf("%sEstimated storage free: %d:%.2fGB, %d:%.2fGB\n",
		// 	indent, br, sourceFree/div, destinationTarget.ID, destFree/div)
	}
}
