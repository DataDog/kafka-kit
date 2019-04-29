package commands

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/DataDog/kafka-kit/kafkazk"

	"github.com/spf13/cobra"
)

// Sort offload targets by size.
type offloadTargetsBySize struct {
	t  []int
	bm kafkazk.BrokerMap
}

// We work with storage free, so a sort by utilization
// descending requires an ascending sort.
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

type relocation struct {
	partition   kafkazk.Partition
	destination int
}

type planRelocationsForBrokerParams struct {
	sourceID               int
	relos                  map[int][]relocation
	mappings               kafkazk.Mappings
	brokers                kafkazk.BrokerMap
	partitionMeta          kafkazk.PartitionMetaMap
	plan                   relocationPlan
	pass                   int
	topPartitionsLimit     int
	partitionSizeThreshold int
	offloadTargetsMap      map[int]struct{}
	tolerance              float64
}

// relocationPlan is a mapping of topic,
// partition to a [][2]int describing a series of
// source and destination brokers to relocate
// a partition to and from.
type relocationPlan map[string]map[int][][2]int

// add takes a kafkazk.Partition and a [2]int pair of
// source and destination broker IDs which the partition
// is scheduled to relocate from and to.
func (r relocationPlan) add(p kafkazk.Partition, ids [2]int) {
	if _, exist := r[p.Topic]; !exist {
		r[p.Topic] = make(map[int][][2]int)
	}

	r[p.Topic][p.Partition] = append(r[p.Topic][p.Partition], ids)
}

// isPlanned takes a kafkazk.Partition and returns whether
// a relocation is planned for the partition, along with the
// [][2]int list of source and destination broker ID pairs.
func (r relocationPlan) isPlanned(p kafkazk.Partition) ([][2]int, bool) {
	var pairs [][2]int

	if _, exist := r[p.Topic]; !exist {
		return pairs, false
	}

	if _, exist := r[p.Topic][p.Partition]; !exist {
		return pairs, false
	}

	return r[p.Topic][p.Partition], true
}

func validateBrokersForRebalance(cmd *cobra.Command, brokers kafkazk.BrokerMap, bm kafkazk.BrokerMetaMap) []int {
	// No broker changes are permitted in rebalance
	// other than new broker additions.
	fmt.Println("\nValidating broker list:")

	// Update the current BrokerList with
	// the provided broker list.
	c, msgs := brokers.Update(Config.brokers, bm)
	for m := range msgs {
		fmt.Printf("%s%s\n", indent, m)
	}

	if c.Changes() {
		fmt.Printf("%s-\n", indent)
	}

	// Check if any referenced brokers are marked as having
	// missing/partial metrics data.
	ensureBrokerMetrics(cmd, brokers, bm)

	switch {
	case c.Missing > 0, c.OldMissing > 0, c.Replace > 0:
		fmt.Printf("%s[ERROR] rebalance only allows broker additions\n", indent)
		os.Exit(1)
	case c.New > 0:
		fmt.Printf("%s%d additional brokers added\n", indent, c.New)
		fmt.Printf("%s-\n", indent)
		fallthrough
	default:
		fmt.Printf("%sOK\n", indent)
	}

	st, _ := cmd.Flags().GetFloat64("storage-threshold")
	stg, _ := cmd.Flags().GetFloat64("storage-threshold-gb")

	var selectorMethod bytes.Buffer
	selectorMethod.WriteString("Brokers targeted for partition offloading ")

	var offloadTargets []int

	// Switch on the target selection method. If
	// a storage threshold in gigabytes is specified,
	// prefer this. Otherwise, use the percentage below
	// mean threshold.
	switch {
	case stg > 0.00:
		selectorMethod.WriteString(fmt.Sprintf("(< %.2fGB storage free)", stg))

		// Get all non-new brokers with a StorageFree
		// below the storage threshold in GB.
		f := func(b *kafkazk.Broker) bool {
			if !b.New && b.StorageFree < stg*div {
				return true
			}
			return false
		}

		matches := brokers.Filter(f)
		for _, b := range matches {
			offloadTargets = append(offloadTargets, b.ID)
		}

		sort.Ints(offloadTargets)
	default:
		selectorMethod.WriteString(fmt.Sprintf("(>= %.2f%% threshold below hmean)", st*100))

		// Find brokers where the storage free is t %
		// below the harmonic mean. Specifying 0 targets
		// all non-new brokers.
		switch st {
		case 0.00:
			f := func(b *kafkazk.Broker) bool { return !b.New }

			matches := brokers.Filter(f)
			for _, b := range matches {
				offloadTargets = append(offloadTargets, b.ID)
			}

			sort.Ints(offloadTargets)
		default:
			offloadTargets = brokers.BelowMean(st, brokers.HMean)
		}
	}

	fmt.Printf("\n%s:\n", selectorMethod.String())

	// Exit if no target brokers were found.
	if len(offloadTargets) == 0 {
		fmt.Printf("%s[none]\n", indent)
		os.Exit(0)
	} else {
		for _, id := range offloadTargets {
			fmt.Printf("%s%d\n", indent, id)
		}
	}

	return offloadTargets
}

func printRebalanceParams(cmd *cobra.Command, results []rebalanceResults, brokers kafkazk.BrokerMap, tol float64) {
	// Print rebalance parameters as a result of
	// input configurations and brokers found
	// to be beyond the storage threshold.
	fmt.Println("\nRebalance parameters:")

	pst, _ := cmd.Flags().GetInt("partition-size-threshold")
	mean, hMean := brokers.Mean(), brokers.HMean()

	fmt.Printf("%sIgnoring partitions smaller than %dMB\n", indent, pst)
	fmt.Printf("%sFree storage mean, harmonic mean: %.2fGB, %.2fGB\n",
		indent, mean/div, hMean/div)

	fmt.Printf("%sBroker free storage limits (with a %.2f%% tolerance from mean):\n",
		indent, tol*100)

	fmt.Printf("%s%sSources limited to <= %.2fGB\n", indent, indent, mean*(1+tol)/div)
	fmt.Printf("%s%sDestinations limited to >= %.2fGB\n", indent, indent, mean*(1-tol)/div)

	verbose, _ := cmd.Flags().GetBool("verbose")

	// Print the top 10 rebalance results
	// in verbose.
	if verbose {
		fmt.Printf("%s-\n%sTop 10 rebalance map results\n", indent, indent)
		for i := range results {
			fmt.Printf("%stolerance: %.2f -> range: %.2fGB\n",
				indent, results[i].tolerance, results[i].storageRange/div)
			if i == 10 {
				break
			}
		}
	}
}

func planRelocationsForBroker(cmd *cobra.Command, params planRelocationsForBrokerParams) int {
	verbose, _ := cmd.Flags().GetBool("verbose")
	localityScoped, _ := cmd.Flags().GetBool("locality-scoped")

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

	// Plan partition movements. Each time a partition is planned
	// to be moved, it's unmapped from the broker so that it's
	// not retried the next iteration.
	var reloCount int
	for _, partn := range topPartn {
		// Get a storage sorted brokerList.
		brokerList := brokers.List()
		brokerList.SortByStorage()

		pSize, _ := partitionMeta.Size(partn)

		// Find a destination broker.
		var dest *kafkazk.Broker

		// Whether or not the destination broker should have the same
		// rack.id as the target. If so, choose the least utilized broker
		// in same locality. If not, choose the least utilized broker
		// the satisfies placement constraints considering the brokers
		// in the replica list (excluding the sourceID broker since it
		// will be replaced).
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
			// Get constraints for all brokers in the
			// partition replica set, excluding the
			// sourceID broker.
			replicaSet := kafkazk.BrokerList{}
			for _, id := range partn.Replicas {
				if id != sourceID {
					replicaSet = append(replicaSet, brokers[id])
				}
			}

			// Include brokers already scheduled to
			// receive this partition.
			if pairs, planned := plan.isPlanned(partn); planned {
				for _, p := range pairs {
					replicaSet = append(replicaSet, brokers[p[1]])
				}
			}

			c := kafkazk.MergeConstraints(replicaSet)

			// Add all offload targets to the constraints.
			// We're populating empty Brokers using just
			// the IDs so that the rack IDs aren't excluded.
			for id := range offloadTargetsMap {
				c.Add(&kafkazk.Broker{ID: id})
			}

			// Select the best candidate by storage.
			dest, _ = brokerList.BestCandidate(c, "storage", 0)
		}

		// If dest == nil, it's likely that the only available
		// destination brokers that don't break placement constraints
		// are already taking a replica for the partition. Continue
		// to the next partition.
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

		// If the estimated storage change pushes either the
		// target or destination beyond the threshold distance
		// from the mean, try the next partition.

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

		// Remove the partition as being mapped
		// to the source broker.
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

func applyRelocationPlan(cmd *cobra.Command, pm *kafkazk.PartitionMap, plan relocationPlan) {
	// Traverse the partition list.
	for _, partn := range pm.Partitions {
		// If a relocation is planned for the partition,
		// replace the source ID with the planned
		// destination ID.
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

	// Optimize leaders.
	if t, _ := cmd.Flags().GetBool("optimize-leaders"); t {
		pm.SimpleLeaderOptimization()
	}
}

func printPlannedRelocations(targets []int, relos map[int][]relocation, pmm kafkazk.PartitionMetaMap) {
	var total float64

	for _, id := range targets {
		fmt.Printf("\nBroker %d relocations planned:\n", id)

		if _, exist := relos[id]; !exist {
			fmt.Printf("%s[none]\n", indent)
			continue
		}

		for _, r := range relos[id] {
			pSize, _ := pmm.Size(r.partition)
			total += pSize / div
			fmt.Printf("%s[%.2fGB] %s p%d -> %d\n",
				indent, pSize/div, r.partition.Topic, r.partition.Partition, r.destination)
		}
	}
	fmt.Printf("%s-\n", indent)
	fmt.Printf("%sTotal relocation volume: %.2fGB\n", indent, total)
}

func absDistance(x, t float64) float64 {
	return math.Abs(t-x) / t
}
