package commands

import (
	"bytes"
	"fmt"
	"os"
	"sort"

	"github.com/DataDog/kafka-kit/v3/kafkazk"

	"github.com/spf13/cobra"
)

func validateBrokersForScale(cmd *cobra.Command, brokers kafkazk.BrokerMap, bm kafkazk.BrokerMetaMap) []int {
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
		fmt.Printf("%s[ERROR] scale only allows broker additions\n", indent)
		os.Exit(1)
	case c.New > 0:
		fmt.Printf("%s%d additional brokers added\n", indent, c.New)
		fmt.Printf("%s-\n", indent)
		fmt.Printf("%sOK\n", indent)
	default:
		fmt.Printf("%s[ERROR] scale requires additional brokers\n", indent)
		os.Exit(1)
	}

	var selectorMethod bytes.Buffer
	selectorMethod.WriteString("Brokers targeted for partition offloading ")

	var offloadTargets []int

	// Offload targets are all non-new brokers.
	f := func(b *kafkazk.Broker) bool { return !b.New }

	matches := brokers.Filter(f)
	for _, b := range matches {
		offloadTargets = append(offloadTargets, b.ID)
	}

	sort.Ints(offloadTargets)

	// Exit if we've hit insufficient broker counts.
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

func printScaleParams(cmd *cobra.Command, results []rebalanceResults, brokers kafkazk.BrokerMap, tol float64) {
	// Print rebalance parameters as a result of input configurations and brokers
	// found to be beyond the storage threshold.
	fmt.Println("\nScale parameters:")

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
		fmt.Printf("%s-\n%sTop 10 scale map results\n", indent, indent)
		for i, r := range results {
			fmt.Printf("%stolerance: %.2f -> range: %.2fGB, std. deviation: %.2fGB\n",
				indent, r.tolerance, r.storageRange/div, r.stdDev/div)
			if i == 10 {
				break
			}
		}
	}
}
