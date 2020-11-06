package commands

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/DataDog/kafka-kit/v3/kafkazk"

	"github.com/spf13/cobra"
)

// Sort offload targets by size.
type offloadTargetsBySize struct {
	t  []int
	bm kafkazk.BrokerMap
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

func validateBrokersForRebalance(cmd *cobra.Command, brokers kafkazk.BrokerMap, bm kafkazk.BrokerMetaMap) []int {
	// No broker changes are permitted in rebalance other than new broker additions.
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

	// Check if any referenced brokers are marked as having missing/partial metrics data.
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

	// Switch on the target selection method. If a storage threshold in gigabytes
	// is specified, prefer this. Otherwise, use the percentage below mean threshold.
	switch {
	case stg > 0.00:
		selectorMethod.WriteString(fmt.Sprintf("(< %.2fGB storage free)", stg))

		// Get all non-new brokers with a StorageFree below the storage threshold in GB.
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

		// Find brokers where the storage free is t % below the harmonic mean.
		// Specifying 0 targets all non-new brokers.
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
	// Print rebalance parameters as a result of input configurations and brokers
	// found to be beyond the storage threshold.
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

	// Print the top 10 rebalance results in verbose.
	if verbose {
		fmt.Printf("%s-\n%sTop 10 rebalance map results\n", indent, indent)
		for i, r := range results {
			fmt.Printf("%stolerance: %.2f -> range: %.2fGB, std. deviation: %.2fGB\n",
				indent, r.tolerance, r.storageRange/div, r.stdDev/div)
			if i == 10 {
				break
			}
		}
	}
}
