package kafkazk

import (
	"fmt"
)

// Mappings is a mapping of broker IDs
// to currently held partition as a partitionList.
type Mappings map[int]map[string]partitionList

// NewMappings returns a new Mappings.
func NewMappings() Mappings {
	return map[int]map[string]partitionList{}
}

// Mappings returns a Mappings from a *PartitionMap.
func (pm *PartitionMap) Mappings() Mappings {
	m := NewMappings()

	for _, p := range pm.Partitions {
		for _, id := range p.Replicas {
			// Create if not exists.
			if _, exist := m[id]; !exist {
				m[id] = map[string]partitionList{}
			}

			// Add the partition to the list.
			m[id][p.Topic] = append(m[id][p.Topic], p)
		}
	}

	return m
}

// LargestPartitions takes a broker ID and PartitionMetaMap and
// returns a partitionList with the top k partitions by size for
// the provided broker ID.
func (m Mappings) LargestPartitions(id int, k int, pm PartitionMetaMap) (partitionList, error) {
	var allBySize partitionList

	if _, exist := m[id]; !exist {
		return nil, fmt.Errorf("no mapping for ID %d", id)
	}

	for topic := range m[id] {
		allBySize = append(allBySize, m[id][topic]...)
	}

	allBySize.SortBySize(pm)

	if k > len(allBySize) {
		k = len(allBySize)
	}

	pl := make(partitionList, k)
	copy(pl, allBySize)

	return pl, nil
}
