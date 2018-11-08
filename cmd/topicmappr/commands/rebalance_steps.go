package commands

import (
	"fmt"
	"math"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

func absDistance(x, t float64) float64 {
	return math.Abs(t-x) / t
}

type relocation struct {
	partition   kafkazk.Partition
	destination int
}

type planRelocationsForBrokerParams struct {
	sourceID      int
	relos         map[int][]relocation
	mappings      kafkazk.Mappings
	brokers       kafkazk.BrokerMap
	partitionMeta kafkazk.PartitionMetaMap
	plan          relocationPlan
}

// relocationPlan is a mapping of topic,
// partition to a [2]int describing the
// source and destination broker to relocate
// a partition to and from.
type relocationPlan map[string]map[int][2]int

// add takes a kafkazk.Partition and a [2]int pair of
// source and destination broker IDs which the partition
// is scheduled to relocate from and to.
func (r relocationPlan) add(p kafkazk.Partition, ids [2]int) {
	if _, exist := r[p.Topic]; !exist {
		r[p.Topic] = make(map[int][2]int)
	}

	r[p.Topic][p.Partition] = ids
}

// isPlanned takes a kafkazk.Partition and returns whether
// a relocation is planned for the partition, along with
// the [2]int pair of source and destination broker IDs.
func (r relocationPlan) isPlanned(p kafkazk.Partition) (bool, [2]int) {
	var pair [2]int
	if _, exist := r[p.Topic]; !exist {
		return false, pair
	}

	if _, exist := r[p.Topic][p.Partition]; !exist {
		return false, pair
	} else {
		pair = r[p.Topic][p.Partition]
	}

	return true, pair
}

func planRelocationsForBroker(cmd *cobra.Command, params planRelocationsForBrokerParams) int {
	verbose, _ := cmd.Flags().GetBool("verbose")
	tolerance, _ := cmd.Flags().GetFloat64("tolerance")
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")

	relos := params.relos
	mappings := params.mappings
	brokers := params.brokers
	partitionMeta := params.partitionMeta
	plan := params.plan
	sourceID := params.sourceID

	// Use the arithmetic mean for target
	// thresholds.
	// TODO test what is best.
	meanStorageFree := brokers.Mean()

	// Get the top partitions for the target broker.
	topPartn, _ := mappings.LargestPartitions(sourceID, 10, partitionMeta)

	if verbose {
		fmt.Printf("\nBroker %d has a storage free of %.2fGB. Top partitions:\n",
			sourceID, brokers[sourceID].StorageFree/div)

		for _, p := range topPartn {
			pSize, _ := partitionMeta.Size(p)
			fmt.Printf("%s%s p%d: %.2fGB\n",
				indent, p.Topic, p.Partition, pSize/div)
		}
	}

	targetLocality := brokers[sourceID].Locality

	// Plan partition movements. Each time a partition is planned
	// to be moved, it's unmapped from the broker so that it's
	// not retried the next iteration.
	var reloCount int
	for _, p := range topPartn {
		// Get a storage sorted brokerList.
		brokerList := brokers.List()
		brokerList.SortByStorage()

		pSize, _ := partitionMeta.Size(p)

		// Find a destination broker.
		var dest *kafkazk.Broker

		// Whether or not the destination broker should have the same
		// rack.id as the target. If so, choose the lowest utilized broker
		// in same locality. If not, choose the lowest utilized broker.
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

		if verbose {
			fmt.Printf("%s-\n", indent)
			fmt.Printf("%sAttempting migration plan for %s p%d\n", indent, p.Topic, p.Partition)
			fmt.Printf("%sCandidate destination broker %d has a storage free of %.2fGB\n",
				indent, dest.ID, dest.StorageFree/div)
		}

		sourceFree := brokers[sourceID].StorageFree + pSize
		destFree := dest.StorageFree - pSize

		// Don't plan a migration that'd push a destination
		// beyond the point the source already is at + the
		// allowable tolerance.
		destinationTolerance := sourceFree * (1 - tolerance)
		if destFree < destinationTolerance {
			if verbose {
				fmt.Printf("%sCannot move partition to candidate: "+
					"expected storage free %.2fGB below tolerated threshold of %.2fGB\n",
					indent, destFree/div, destinationTolerance/div)

			}

			continue
		}

		// If the estimated storage change pushes neither the
		// target nor destination beyond the threshold distance
		// from the mean, plan the partition migration.
		if absDistance(sourceFree, meanStorageFree) <= tolerance && absDistance(destFree, meanStorageFree) <= tolerance {
			relos[sourceID] = append(relos[sourceID], relocation{partition: p, destination: dest.ID})
			reloCount++

			// Add to plan.
			plan.add(p, [2]int{sourceID, dest.ID})

			// Update StorageFree values.
			brokers[sourceID].StorageFree = sourceFree
			brokers[dest.ID].StorageFree = destFree

			// Remove the partition as being mapped
			// to the source broker.
			mappings.Remove(sourceID, p)

			if verbose {
				fmt.Printf("%sPlanning relocation to candidate\n", indent)
			}

			// Move on to the next broker.
			break
		}
	}

	return reloCount
}

func applyRelocationPlan(partitionMap *kafkazk.PartitionMap, plan relocationPlan) {
	// Traverse the partition list.
	for _, partn := range partitionMap.Partitions {
		// If a relocation is planned for the partition,
		// replace the source ID with the planned
		// destination ID.
		if planned, p := plan.isPlanned(partn); planned {
			for i, r := range partn.Replicas {
				if r == p[0] {
					partn.Replicas[i] = p[1]
				}
			}
		}
	}
}

func printPlannedRelocations(targets []int, relos map[int][]relocation, pmm kafkazk.PartitionMetaMap) {
	for _, id := range targets {
		fmt.Printf("\nBroker %d relocations planned:\n", id)

		if _, exist := relos[id]; !exist {
			fmt.Printf("%s[none]\n", indent)
			continue
		}

		for _, r := range relos[id] {
			pSize, _ := pmm.Size(r.partition)
			fmt.Printf("%s%s[%.2fGB] %s p%d -> %d\n",
				indent, indent, pSize/div, r.partition.Topic, r.partition.Partition, r.destination)
		}
	}
}
