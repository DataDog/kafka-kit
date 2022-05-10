package commands

import (
	"fmt"

	"github.com/DataDog/kafka-kit/v3/mapper"
)

// Relocation is a kafakzk.Partition to destination broker ID.
type relocation struct {
	partition   mapper.Partition
	destination int
}

// planRelocationsForBrokerParams are used to plan partition relocations from
// source brokers to destination brokers.
type planRelocationsForBrokerParams struct {
	relos                  map[int][]relocation
	mappings               mapper.Mappings
	brokers                mapper.BrokerMap
	partitionMeta          mapper.PartitionMetaMap
	plan                   relocationPlan
	topPartitionsLimit     int
	partitionSizeThreshold int
	offloadTargetsMap      map[int]struct{}
	tolerance              float64
	localityScoped         bool
	verbose                bool
	// These aren't specified by the user.
	pass     int
	sourceID int
}

// relocationPlan is a mapping of topic, partition to a [][2]int describing a
// series of source and destination brokers to relocate a partition to and from.
type relocationPlan map[string]map[int][][2]int

// add takes a mapper.Partition and a [2]int pair of source and destination
// broker IDs which the partition is scheduled to relocate from and to.
func (r relocationPlan) add(p mapper.Partition, ids [2]int) {
	if _, exist := r[p.Topic]; !exist {
		r[p.Topic] = make(map[int][][2]int)
	}

	r[p.Topic][p.Partition] = append(r[p.Topic][p.Partition], ids)
}

// isPlanned takes a mapper.Partition and returns whether a relocation is
// planned for the partition, along with the [][2]int list of source and
// destination broker ID pairs.
func (r relocationPlan) isPlanned(p mapper.Partition) ([][2]int, bool) {
	var pairs [][2]int

	if _, exist := r[p.Topic]; !exist {
		return pairs, false
	}

	if _, exist := r[p.Topic][p.Partition]; !exist {
		return pairs, false
	}

	return r[p.Topic][p.Partition], true
}

// TODO(jamie): ...wow
func planRelocationsForBroker(params planRelocationsForBrokerParams) int {
	relos := params.relos
	mappings := params.mappings
	brokers := params.brokers
	partitionMeta := params.partitionMeta
	plan := params.plan
	sourceID := params.sourceID
	topPartitionsLimit := params.topPartitionsLimit
	partitionSizeThreshold := float64(params.partitionSizeThreshold * 1 << 20)
	offloadTargetsMap := params.offloadTargetsMap
	tolerance := params.tolerance
	localityScoped := params.localityScoped
	verbose := params.verbose

	// Use the arithmetic mean for target
	// thresholds.
	meanStorageFree := brokers.Mean()

	// Get the top partitions for the target broker.
	topPartn, _ := mappings.LargestPartitions(sourceID, topPartitionsLimit, partitionMeta)

	// Filter out partitions below the targeted size threshold.
	for i, p := range topPartn {
		pSize, _ := partitionMeta.Size(p)
		if pSize < partitionSizeThreshold {
			topPartn = topPartn[:i]
			break
		}
	}

	if verbose {
		fmt.Printf("\n[pass %d with tolerance %.2f] Broker %d has a storage free of %.2fGB. Top partitions:\n",
			params.pass, tolerance, sourceID, brokers[sourceID].StorageFree/div)

		for _, p := range topPartn {
			pSize, _ := partitionMeta.Size(p)
			fmt.Printf("%s%s p%d: %.2fGB\n",
				indent, p.Topic, p.Partition, pSize/div)
		}
	}

	targetLocality := brokers[sourceID].Locality

	// Plan partition movements. Each time a partition is planned to be moved, it's
	// unmapped from the broker so that it's not retried the next iteration.
	var reloCount int
	for _, partn := range topPartn {
		// Get a storage sorted brokerList.
		brokerList := brokers.List()
		brokerList.SortByStorage()

		pSize, _ := partitionMeta.Size(partn)

		// Find a destination broker.
		var dest *mapper.Broker

		// Whether or not the destination broker should have the same rack.id as the
		// target. If so, choose the least utilized broker in same locality. If not,
		// choose the least utilized broker the satisfies placement constraints
		// considering the brokers in the replica list (excluding the sourceID broker
		// since it will be replaced).
		switch localityScoped {
		case true:
			for _, b := range brokerList {
				if b.Locality == targetLocality && b.ID != sourceID {
					// Don't select from offload targets.
					if _, t := offloadTargetsMap[b.ID]; t {
						continue
					}

					dest = b
					break
				}
			}
		case false:
			// Get constraints for all brokers in the partition replica set, excluding
			// the sourceID broker.
			replicaSet := mapper.BrokerList{}
			for _, id := range partn.Replicas {
				if id != sourceID {
					replicaSet = append(replicaSet, brokers[id])
				}
			}

			// Include brokers already scheduled to receive this partition.
			if pairs, planned := plan.isPlanned(partn); planned {
				for _, p := range pairs {
					replicaSet = append(replicaSet, brokers[p[1]])
				}
			}

			c := mapper.MergeConstraints(replicaSet)

			// Add all offload targets to the constraints. We're populating empty
			// Brokers using just the IDs so that the rack IDs aren't excluded.
			for id := range offloadTargetsMap {
				c.Add(&mapper.Broker{ID: id})
			}

			// Select the best candidate by storage.
			dest, _ = brokerList.BestCandidate(c, "storage", 0)
		}

		// If dest == nil, it's likely that the only available destination brokers
		// that don't break placement constraints are already taking a replica for
		// the partition. Continue to the next partition.
		if dest == nil {
			continue
		}

		if verbose {
			fmt.Printf("%s-\n", indent)
			fmt.Printf("%sAttempting migration plan for %s p%d\n", indent, partn.Topic, partn.Partition)
			fmt.Printf("%sCandidate destination broker %d has a storage free of %.2fGB\n",
				indent, dest.ID, dest.StorageFree/div)
		}

		sourceFree := brokers[sourceID].StorageFree + pSize
		destFree := dest.StorageFree - pSize

		// If the estimated storage change pushes either the target or destination
		// beyond the threshold distance from the mean, try the next partition.

		sLim := meanStorageFree * (1 + tolerance)
		if sourceFree > sLim {
			if verbose {
				fmt.Printf("%sCannot move partition from target: "+
					"expected storage free %.2fGB above tolerated threshold of %.2fGB\n",
					indent, sourceFree/div, sLim/div)
			}

			continue
		}

		dLim := meanStorageFree * (1 - tolerance)
		if destFree < dLim {
			if verbose {
				fmt.Printf("%sCannot move partition to candidate: "+
					"expected storage free %.2fGB below tolerated threshold of %.2fGB\n",
					indent, destFree/div, dLim/div)
			}

			continue
		}

		// Otherwise, schedule the relocation.

		relos[sourceID] = append(relos[sourceID], relocation{partition: partn, destination: dest.ID})
		reloCount++

		// Add to plan.
		plan.add(partn, [2]int{sourceID, dest.ID})

		// Update StorageFree values.
		brokers[sourceID].StorageFree = sourceFree
		brokers[dest.ID].StorageFree = destFree

		// Remove the partition as being mapped to the source broker.
		mappings.Remove(sourceID, partn)

		if verbose {
			fmt.Printf("%sPlanning relocation to candidate\n", indent)
		}

		// Break at the first placement.
		break
	}

	if verbose && reloCount == 0 {
		fmt.Printf("%s-\n", indent)
		fmt.Printf("%sNo suitable relocation destinations were found for any partitions "+
			"held by this broker. This is likely due to insufficient free candidates "+
			"in rack IDs that won't break placement constraints and/or suitable candidates "+
			"already being scheduled to take replicas of partitions held by this broker\n", indent)
	}

	return reloCount
}

func applyRelocationPlan(pm *mapper.PartitionMap, plan relocationPlan) {
	// Traverse the partition list.
	for _, partn := range pm.Partitions {
		// If a relocation is planned for the partition, replace the source ID with
		// the planned destination ID.
		if pairs, planned := plan.isPlanned(partn); planned {
			for i, r := range partn.Replicas {
				for _, relo := range pairs {
					if r == relo[0] {
						partn.Replicas[i] = relo[1]
					}
				}
			}
		}
	}
}

// Sort offload targets by size.
type offloadTargetsBySize struct {
	t  []int
	bm mapper.BrokerMap
}

// We work with storage free, so a sort by utilization descending requires an
// ascending sort.
func (o offloadTargetsBySize) Len() int      { return len(o.t) }
func (o offloadTargetsBySize) Swap(i, j int) { o.t[i], o.t[j] = o.t[j], o.t[i] }
func (o offloadTargetsBySize) Less(i, j int) bool {
	s1 := o.bm[o.t[i]].StorageFree
	s2 := o.bm[o.t[j]].StorageFree

	if s1 < s2 {
		return true
	}

	if s1 > s2 {
		return false
	}

	return o.t[i] < o.t[j]
}
