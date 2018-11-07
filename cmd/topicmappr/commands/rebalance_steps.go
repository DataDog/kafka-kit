package commands

import (
	"fmt"
	"math"

	"github.com/DataDog/kafka-kit/kafkazk"
)

func absDistance(x, t float64) float64 {
	return math.Abs(t-x) / t
}

type relocation struct {
	partition   kafkazk.Partition
	destination int
}

// relocationPlan is a mapping of topic,
// partition to a [2]int describing the
// source and destination broker to relocate
// a partition to and from.
type relocationPlan map[string]map[int][2]int

func (r relocationPlan) add(p kafkazk.Partition, ids [2]int) {
	if _, exist := r[p.Topic]; !exist {
		r[p.Topic] = make(map[int][2]int)
	}

	r[p.Topic][p.Partition] = ids
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
