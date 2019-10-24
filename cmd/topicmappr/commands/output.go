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

type errors []error

func (e errors) Len() int           { return len(e) }
func (e errors) Less(i, j int) bool { return e[i].Error() < e[j].Error() }
func (e errors) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }

// printTopics takes a partition map and prints out
// the names of all topics referenced in the map.
func printTopics(pm *kafkazk.PartitionMap) {
	topics := map[string]struct{}{}
	for _, p := range pm.Partitions {
		topics[p.Topic] = struct{}{}
	}

	fmt.Printf("\nTopics:\n")
	for t := range topics {
		fmt.Printf("%s%s\n", indent, t)
	}
}

func printExcludedTopics(p []string) {
	if len(p) > 0 {
		fmt.Printf("\nTopics excluded due to pending deletion:\n")
		for _, t := range p {
			fmt.Printf("%s%s\n", indent, t)
		}
	}
}

// printMapChanges takes the original input PartitionMap
// and the final output PartitionMap and prints what's changed.
func printMapChanges(pm1, pm2 *kafkazk.PartitionMap) {
	// Ensure the topic name and partition
	// order match.
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
func printBrokerAssignmentStats(cmd *cobra.Command, pm1, pm2 *kafkazk.PartitionMap, bm1, bm2 kafkazk.BrokerMap) errors {
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

	// If we're using the storage placement strategy,
	// write anticipated storage changes.
	psf, _ := cmd.Flags().GetFloat64("partition-size-factor")

	if cmd.Use == "rebalance" || cmd.Flag("placement").Value.String() == "storage" {
		fmt.Println("\nStorage free change estimations:")
		if psf != 1.0 && cmd.Use != "rebalance" {
			fmt.Printf("%sPartition size factor of %.2f applied\n", indent, psf)
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

		// Filter function for brokers where the Replaced
		// field is false.
		nonReplaced := func(b *kafkazk.Broker) bool {
			if b.Replace {
				return false
			}
			return true
		}

		// Get all IDs in PartitionMap.
		mappedIDs := map[int]struct{}{}
		for _, partn := range pm1.Partitions {
			for _, id := range partn.Replicas {
				mappedIDs[id] = struct{}{}
			}
		}

		// Filter function for brokers that are in the
		// partition map.
		mapped := func(b *kafkazk.Broker) bool {
			if _, exist := mappedIDs[b.ID]; exist {
				return true
			}
			return false
		}

		mb1, mb2 := bm1.Filter(mapped), bm2.Filter(nonReplaced)

		// Range before/after.
		r1, r2 := mb1.StorageRange(), mb2.StorageRange()
		fmt.Printf("%srange: %.2fGB -> %.2fGB\n", indent, r1/div, r2/div)
		if r2 > r1 {
			errs = append(errs, fmt.Errorf("broker free storage range increased"))
		}

		// Range spread before/after.
		rs1, rs2 := mb1.StorageRangeSpread(), mb2.StorageRangeSpread()
		fmt.Printf("%srange spread: %.2f%% -> %.2f%%\n", indent, rs1, rs2)

		// Std dev before/after.
		sd1, sd2 := mb1.StorageStdDev(), mb2.StorageStdDev()
		fmt.Printf("%sstd. deviation: %.2fGB -> %.2fGB\n", indent, sd1/div, sd2/div)

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

			// Skip reporting non-changes when using rebalance.
			// if cmd.Use == "rebalance" && diff[1] == 0.00 {
			// 	continue
			// }

			fmt.Printf("%sBroker %d: %.2f -> %.2f (%+.2fGB, %.2f%%) %s\n",
				indent, id, originalStorage, newStorage, diff[0]/div, diff[1], replace)
		}
	}

	return errs
}

// skipReassignmentNoOps removes no-op partition map changes
// from the input and final output PartitionMap
func skipReassignmentNoOps(pm1, pm2 *kafkazk.PartitionMap) (*kafkazk.PartitionMap, *kafkazk.PartitionMap) {
	prunedInputPartitionMap := kafkazk.NewPartitionMap()
	prunedOutputPartitionMap := kafkazk.NewPartitionMap()
	for i := range pm1.Partitions {
		p1, p2 := pm1.Partitions[i], pm2.Partitions[i]
		if !p1.Equal(p2) {
			prunedInputPartitionMap.Partitions = append(prunedInputPartitionMap.Partitions, p1)
			prunedOutputPartitionMap.Partitions = append(prunedOutputPartitionMap.Partitions, p2)
		}
	}

	return prunedInputPartitionMap, prunedOutputPartitionMap
}

// writeMaps takes a PartitionMap and writes out
// files.
func writeMaps(cmd *cobra.Command, pm *kafkazk.PartitionMap) {
	if len(pm.Partitions) == 0 {
		fmt.Println("\nNo partition reassignments, skipping map generation")
		return
	}

	op := cmd.Flag("out-path").Value.String()
	of := cmd.Flag("out-file").Value.String()

	// Map per topic.
	tm := map[string]*kafkazk.PartitionMap{}
	for _, p := range pm.Partitions {
		if tm[p.Topic] == nil {
			tm[p.Topic] = kafkazk.NewPartitionMap()
		}
		tm[p.Topic].Partitions = append(tm[p.Topic].Partitions, p)
	}

	fmt.Println("\nNew partition maps:")
	// Global map if set.
	if of != "" {
		err := kafkazk.WriteMap(pm, op+of)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json [combined map]\n", indent, op, of)
		}
	}

	for t := range tm {
		err := kafkazk.WriteMap(tm[t], op+t)
		if err != nil {
			fmt.Printf("%s%s", indent, err)
		} else {
			fmt.Printf("%s%s%s.json\n", indent, op, t)
		}
	}
}

// handleOverridableErrs handles errors that can be optionally ignored
// by the user (hence being referred to as 'WARN' in the
// CLI). If --ignore-warns is false (default), any errors passed
// here will cause an exit(1).
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

// whatChanged takes a before and after broker replica set
// and returns a string describing what changed.
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

	// If the len is the same,
	// check elements.
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

	// Get smaller replica set len between
	// old vs new, then cap both to this len for
	// comparison.
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

	// If the broker lists changed but
	// are the same after sorting,
	// we've just changed the preferred
	// leader.
	if echanged && samePostSort {
		changes = append(changes, "preferred leader")
	}

	// If the broker lists changed and
	// aren't the same after sorting, we've
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
