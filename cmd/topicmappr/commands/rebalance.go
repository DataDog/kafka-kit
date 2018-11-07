package commands

import (
	"fmt"
	"math"
	"os"
	"sort"

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
	rebalanceCmd.Flags().Float64("tolerance", 0.10, "tolerance")
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
	brokersOrig := brokers.Copy()

	// Update the currentBrokers list with
	// the provided broker list.
	// TODO we should only take New brokers in a rebalance.
	bs := brokers.Update(Config.brokers, brokerMeta)
	fmt.Printf("%+v\n", bs)

	// Find brokers where the storage utilization is d %
	// above the harmonic mean.
	t, _ := cmd.Flags().GetFloat64("storage-threshold")
	offloadTargets := brokers.BelowMean(t, brokers.HMean)

	// Use the arithmetic mean for target
	// thresholds.
	meanStorageFree := brokers.Mean()

	var div = 1073741824.00
	tolerance, _ := cmd.Flags().GetFloat64("tolerance")
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")

	fmt.Printf("Mean: %.2fGB\n", meanStorageFree/div)

	fmt.Printf("Brokers targeted for partition offloading: %v\n", offloadTargets)

	// Iterate over offload target brokers.
	for _, sourceID := range offloadTargets {

		// Get the top 5 partitions for the target broker.
		topPartn, _ := mappings.LargestPartitions(sourceID, 5, partitionMeta)

		fmt.Printf("\nBroker %d has a storage free of %.2fGB. Top partitions:\n",
			sourceID, brokers[sourceID].StorageFree/div)

		for _, p := range topPartn {
			pSize, _ := partitionMeta.Size(p)
			fmt.Printf("%stopic: %s, partition: %d, size: %.2fGB\n",
				indent, p.Topic, p.Partition, pSize/div)
		}

		targetLocality := brokers[sourceID].Locality
		partnsToMove := map[int][]kafkazk.Partition{}

		// Plan partition movements.
		for _, p := range topPartn {
			// Get a storage sorted brokerList.
			brokerList := brokers.List()
			brokerList.SortByStorage()

			pSize, _ := partitionMeta.Size(p)

			// Find a destination broker.
			var dest *kafkazk.Broker

			// Whether or not the destination broker have the same rack.id
			// as the target. If so, choose the lowest utilized broker in
			// same locality. If not, choose the lowest utilized broker.
			switch localityScoped {
			case true:
				for _, b := range brokers {
					if b.Locality == targetLocality {
						dest = b
						break
					}
				}
			default:
				dest = brokerList[0]
			}

			sourceFree := brokers[sourceID].StorageFree + pSize
			destFree := dest.StorageFree - pSize

			// If the estimated storage change pushes neither the
			// target nor destination beyond the threshold distance
			// from the mean, we schedule the partition migration.
			if absDistance(sourceFree, meanStorageFree) <= tolerance && absDistance(destFree, meanStorageFree) <= tolerance {
				partnsToMove[sourceID] = append(partnsToMove[sourceID], p)
				// Update StorageFree values.
				brokers[sourceID].StorageFree = sourceFree
				brokers[dest.ID].StorageFree = destFree
			}
		}

		fmt.Printf("%sPartitions to move for %d: %v\n",
			indent, sourceID, partnsToMove[sourceID])

		// fmt.Printf("%sMoving partition %s:%d from %d -> %d\n",
		// 	indent, partitionToMove.Topic, partitionToMove.Partition, br, dest.ID)
		//
		// fmt.Printf("%sEstimated storage free: %d:%.2fGB, %d:%.2fGB\n",
		// 	indent, br, sourceFree/div, dest.ID, destFree/div)
	}

	// Get changes in storage utilization.
	storageDiffs := brokersOrig.StorageDiff(brokers)

	// Pop IDs into a slice for sorted ouptut.
	ids := []int{}
	for id := range storageDiffs {
		ids = append(ids, id)
	}

	sort.Ints(ids)

	fmt.Printf("\nBroker Storage Changes:\n")
	for _, id := range ids {
		// Skip the internal reserved ID.
		if id == 0 {
			continue
		}

		diff := storageDiffs[id]

		originalStorage := brokersOrig[id].StorageFree / div
		newStorage := brokers[id].StorageFree / div
		if diff[1] != 0.00 {
			fmt.Printf("%sBroker %d: %.2f -> %.2f (%+.2fGB, %.2f%%)\n",
				indent, id, originalStorage, newStorage, diff[0]/div, diff[1])
		}
	}
}

func absDistance(x, t float64) float64 {
	return math.Abs(t-x) / t
}
