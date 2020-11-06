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
