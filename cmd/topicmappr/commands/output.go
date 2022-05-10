package commands

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sort"

	"github.com/DataDog/kafka-kit/v3/mapper"

	"github.com/spf13/cobra"
)

type errors []error

func (e errors) Len() int           { return len(e) }
func (e errors) Less(i, j int) bool { return e[i].Error() < e[j].Error() }
func (e errors) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

// printTopics takes a partition map and prints out the names of all topics
// referenced in the map.
func printTopics(pm *mapper.PartitionMap) {
	topics := pm.Topics()

	fmt.Printf("\nTopics:\n")
	for _, t := range topics {
		fmt.Printf("%s%s\n", indent, t)
	}
}

func printExcludedTopics(p []string, e []string) {
	if len(p) > 0 {
		sort.Strings(p)
		fmt.Printf("\nTopics excluded due to pending deletion:\n")
		for _, t := range p {
			fmt.Printf("%s%s\n", indent, t)
		}
	}

	if len(e) > 0 {
		sort.Strings(e)
		fmt.Printf("\nTopics excluded:\n")
		for _, t := range e {
			fmt.Printf("%s%s\n", indent, t)
		}
	}
}

// printMapChanges takes the original input PartitionMap and the final output
// PartitionMap and prints what's changed.
func printMapChanges(pm1, pm2 *mapper.PartitionMap) {
	// Ensure the topic name and partition order match.
	for i := range pm1.Partitions {
		t1, t2 := pm1.Partitions[i].Topic, pm2.Partitions[i].Topic
		p1, p2 := pm1.Partitions[i].Partition, pm2.Partitions[i].Partition
		if t1 != t2 || p1 != p2 {
			fmt.Println("Unexpected partition map order")
			os.Exit(1)
		}
	}

	// Get a status string of what's changed.
	fmt.Println("\nPartition map changes:")
	for i := range pm1.Partitions {
		change := whatChanged(pm1.Partitions[i].Replicas,
			pm2.Partitions[i].Replicas)

		fmt.Printf("%s%s p%d: %v -> %v %s\n",
			indent,
			pm1.Partitions[i].Topic,
			pm1.Partitions[i].Partition,
			pm1.Partitions[i].Replicas,
			pm2.Partitions[i].Replicas,
			change)
	}
}

// printBrokerAssignmentStats prints before and after broker usage stats,
// such as leadership counts, total partitions owned, degree distribution,
// and changes in storage usage.
func printBrokerAssignmentStats(pm1, pm2 *mapper.PartitionMap, bm1, bm2 mapper.BrokerMap, storageBased bool, partitionSizeFactor float64) errors {
	var errs errors

	fmt.Println("\nBroker distribution:")

	// Get general info.
	dd1, dd2 := pm1.DegreeDistribution().Stats(), pm2.DegreeDistribution().Stats()
	fmt.Printf("%sdegree [min/max/avg]: %.0f/%.0f/%.2f -> %.0f/%.0f/%.2f\n",
		indent, dd1.Min, dd1.Max, dd1.Avg, dd2.Min, dd2.Max, dd2.Avg)

	fmt.Printf("%s-\n", indent)

	// Per-broker info.
	UseStats := pm2.UseStats().List()
	for _, use := range UseStats {
		fmt.Printf("%sBroker %d - leader: %d, follower: %d, total: %d\n",
			indent, use.ID, use.Leader, use.Follower, use.Leader+use.Follower)
	}

	// If we're using the storage placement strategy, write anticipated storage changes.
	if storageBased {
		fmt.Println("\nStorage free change estimations:")
		if partitionSizeFactor != 1.0 {
			fmt.Printf("%sPartition size factor of %.2f applied\n", indent, partitionSizeFactor)
		}

		// Get filtered BrokerMaps. For the 'before' broker statistics, we want
		// all brokers in the original BrokerMap that were also in the input PartitionMap.
		// For the 'after' broker statistics, we want brokers that were not marked
		// for replacement. We don't necessarily want to exclude brokers in the output
		// that aren't mapped in the output PartitionMap. It's possible that a broker is
		// not mapped to any of the input topics but is still holding data for other topics.
		// It's ideal to still include that broker's storage metrics since it was a provided
		// input and wasn't marked for replacement (generally, users are doing storage placements
		// particularly to balance out the storage of the input broker list).

		mb1, mb2 := bm1.Filter(pm1.BrokersIn()), bm2.Filter(mapper.NotReplacedBrokersFn)

		// Range before/after.
		r1, r2 := mb1.StorageRange(), mb2.StorageRange()
		fmt.Printf("%srange: %.2fGB -> %.2fGB\n", indent, r1/div, r2/div)
		if r2 > r1 {
			// Range increases are acceptable and common in scale up operations.
			errs = append(errs, fmt.Errorf("broker free storage range increased"))
		}

		// Range spread before/after.
		rs1, rs2 := mb1.StorageRangeSpread(), mb2.StorageRangeSpread()
		fmt.Printf("%srange spread: %.2f%% -> %.2f%%\n", indent, rs1, rs2)

		// Std dev before/after.
		sd1, sd2 := mb1.StorageStdDev(), mb2.StorageStdDev()
		fmt.Printf("%sstd. deviation: %.2fGB -> %.2fGB\n", indent, sd1/div, sd2/div)

		// Storage min/max before/after.
		min1, max1 := mb1.MinMax()
		min2, max2 := mb2.MinMax()
		fmt.Printf("%smin-max: %.2fGB, %.2fGB -> %.2fGB, %.2fGB\n",
			indent, min1/div, max1/div, min2/div, max2/div)

		fmt.Printf("%s-\n", indent)

		// Get changes in storage utilization.
		storageDiffs := bm1.StorageDiff(bm2)

		// Pop IDs into a slice for sorted ouptut.
		ids := []int{}
		for id := range storageDiffs {
			ids = append(ids, id)
		}

		sort.Ints(ids)

		for _, id := range ids {
			diff := storageDiffs[id]

			// Indicate if the broker
			// is a replacement.
			var replace string
			if bm2[id].Replace {
				replace = "*marked for replacement"
			}

			originalStorage := bm1[id].StorageFree / div
			newStorage := bm2[id].StorageFree / div

			fmt.Printf("%sBroker %d: %.2f -> %.2f (%+.2fGB, %.2f%%) %s\n",
				indent, id, originalStorage, newStorage, diff[0]/div, diff[1], replace)
		}
	}

	return errs
}

// skipReassignmentNoOps removes no-op partition map changes
// from the input and final output PartitionMap
func skipReassignmentNoOps(pm1, pm2 *mapper.PartitionMap) (*mapper.PartitionMap, *mapper.PartitionMap) {
	prunedInputPartitionMap := mapper.NewPartitionMap()
	prunedOutputPartitionMap := mapper.NewPartitionMap()
	for i := range pm1.Partitions {
		p1, p2 := pm1.Partitions[i], pm2.Partitions[i]
		if !p1.Equal(p2) {
			prunedInputPartitionMap.Partitions = append(prunedInputPartitionMap.Partitions, p1)
			prunedOutputPartitionMap.Partitions = append(prunedOutputPartitionMap.Partitions, p2)
		}
	}

	return prunedInputPartitionMap, prunedOutputPartitionMap
}

// writeMaps takes a PartitionMap and writes out files.
func writeMaps(outPath, outFile string, pms []*mapper.PartitionMap) {
	if len(pms) == 0 || len(pms[0].Partitions) == 0 {
		fmt.Println("\nNo partition reassignments, skipping map generation")
		return
	}

	phasedMaps := len(pms) > 1
	suffix := func(i int) string {
		if phasedMaps {
			return fmt.Sprintf("-phase%d", i)
		}
		return ""
	}

	// Break map up by topic.
	tm := map[string]*mapper.PartitionMap{}

	// For each map type, create per-topic maps.
	for i, m := range pms {
		// Populate each partition in the parent map keyed
		// by topic name and possible phase suffix.
		for _, p := range m.Partitions {
			mapName := fmt.Sprintf("%s%s", p.Topic, suffix(i))
			if tm[mapName] == nil {
				tm[mapName] = mapper.NewPartitionMap()
			}
			tm[mapName].Partitions = append(tm[mapName].Partitions, p)
		}
	}

	fmt.Println("\nNew partition maps:")

	// Write global map if set.
	if outFile != "" {
		for i, m := range pms {
			fullPath := fmt.Sprintf("%s%s%s", outPath, outFile, suffix(i))
			err := mapper.WriteMap(m, fullPath)
			if err != nil {
				fmt.Printf("%s%s", indent, err)
			} else {
				fmt.Printf("%s%s.json [combined map]\n", indent, fullPath)
			}
		}
	}

	// Write per-topic maps.
	for t := range tm {
		err := mapper.WriteMap(tm[t], outPath+t)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json\n", indent, outPath, t)
		}
	}
}

func printReassignmentParams(params reassignParams, results []reassignmentBundle, brokers mapper.BrokerMap, tol float64) {
	fmt.Printf("\nReassignment parameters:\n")

	mean, hMean := brokers.Mean(), brokers.HMean()

	fmt.Printf("%sIgnoring partitions smaller than %dMB\n", indent, params.partitionSizeThreshold)
	fmt.Printf("%sFree storage mean, harmonic mean: %.2fGB, %.2fGB\n",
		indent, mean/div, hMean/div)

	fmt.Printf("%sBroker free storage limits (with a %.2f%% tolerance from mean):\n",
		indent, tol*100)

	fmt.Printf("%s%sSources limited to <= %.2fGB\n", indent, indent, mean*(1+tol)/div)
	fmt.Printf("%s%sDestinations limited to >= %.2fGB\n", indent, indent, mean*(1-tol)/div)

	// Print the top 10 rebalance results in verbose.
	if params.verbose {
		fmt.Printf("%s-\nTop 10 reassignment map results\n", indent)
		for i, r := range results {
			fmt.Printf("%stolerance: %.2f -> range: %.2fGB, std. deviation: %.2fGB\n",
				indent, r.tolerance, r.storageRange/div, r.stdDev/div)
			if i == 10 {
				break
			}
		}
	}
}

func printPlannedRelocations(targets []int, relos map[int][]relocation, pmm mapper.PartitionMetaMap) {
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

// handleOverridableErrs handles errors that can be optionally ignored by the
// user (hence being referred to as 'WARN' in the CLI). If --ignore-warns is
// false (default), any errors passed here will cause an exit(1).
func handleOverridableErrs(cmd *cobra.Command, e errors) {
	fmt.Println("\nWARN:")
	if len(e) > 0 {
		sort.Sort(e)
		for _, err := range e {
			fmt.Printf("%s%s\n", indent, err)
		}
	} else {
		fmt.Printf("%s[none]\n", indent)
	}

	iw, _ := cmd.Flags().GetBool("ignore-warns")
	if !iw && len(e) > 0 {
		fmt.Printf("\n%sWarnings encountered, partition map not created. Override with --ignore-warns.\n", indent)
		os.Exit(1)
	}
}

// whatChanged takes a before and after broker replica set and returns a string
// describing what changed.
func whatChanged(s1 []int, s2 []int) string {
	var changes []string

	a, b := make([]int, len(s1)), make([]int, len(s2))
	copy(a, s1)
	copy(b, s2)

	var lchanged bool
	var echanged bool

	// Check if the len is different.
	switch {
	case len(a) > len(b):
		lchanged = true
		changes = append(changes, "decreased replication")
	case len(a) < len(b):
		lchanged = true
		changes = append(changes, "increased replication")
	}

	// If the len is the same, check elements.
	if !lchanged {
		for i := range a {
			if a[i] != b[i] {
				echanged = true
			}
		}
	}

	// Nothing changed.
	if !lchanged && !echanged {
		return "no-op"
	}

	// Determine what else changed.

	// Get smaller replica set len between old vs new, then cap both to this
	// len for comparison.
	slen := int(math.Min(float64(len(a)), float64(len(b))))

	a = a[:slen]
	b = b[:slen]

	echanged = false
	for i := range a {
		if a[i] != b[i] {
			echanged = true
		}
	}

	sort.Ints(a)
	sort.Ints(b)

	samePostSort := true
	for i := range a {
		if a[i] != b[i] {
			samePostSort = false
		}
	}

	// If the broker lists changed but are the same after sorting, we've just
	// changed the preferred leader.
	if echanged && samePostSort {
		changes = append(changes, "preferred leader")
	}

	// If the broker lists changed and aren't the same after sorting, we've
	// replaced a broker.
	if echanged && !samePostSort {
		changes = append(changes, "replaced broker")
	}

	// Construct change string.
	var buf bytes.Buffer
	for i, c := range changes {
		buf.WriteString(c)
		if i < len(changes)-1 {
			buf.WriteString(", ")
		}
	}

	return buf.String()
}
