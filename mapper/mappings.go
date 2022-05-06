package mapper

import (
	"fmt"
)

// Mappings is a mapping of broker IDs to currently held
// partition as a PartitionList.
type Mappings map[int]map[string]PartitionList

// NewMappings returns a new Mappings.
func NewMappings() Mappings {
	return map[int]map[string]PartitionList{}
}

// NoMappingForBroker error.
type NoMappingForBroker struct {
	id int
}

func (e NoMappingForBroker) Error() string {
	return fmt.Sprintf("No mapping for ID %d", e.id)
}

// NoMappingForTopic error.
type NoMappingForTopic struct {
	id    int
	topic string
}

func (e NoMappingForTopic) Error() string {
	return fmt.Sprintf("No mapping for ID %d topic %s", e.id, e.topic)
}

// Mappings returns a Mappings from a *PartitionMap.
func (pm *PartitionMap) Mappings() Mappings {
	m := NewMappings()

	for _, p := range pm.Partitions {
		for _, id := range p.Replicas {
			// Create if not exists.
			if _, exist := m[id]; !exist {
				m[id] = map[string]PartitionList{}
			}

			// Add the partition to the list.
			m[id][p.Topic] = append(m[id][p.Topic], p)
		}
	}

	return m
}

// LargestPartitions takes a broker ID and PartitionMetaMap and
// returns a PartitionList with the top k partitions by size for
// the provided broker ID.
func (m Mappings) LargestPartitions(id int, k int, pm PartitionMetaMap) (PartitionList, error) {
	var allBySize PartitionList

	if _, exist := m[id]; !exist {
		return nil, NoMappingForBroker{id: id}
	}

	for topic := range m[id] {
		allBySize = append(allBySize, m[id][topic]...)
	}

	allBySize.SortBySize(pm)

	if k > len(allBySize) {
		k = len(allBySize)
	}

	pl := make(PartitionList, k)
	copy(pl, allBySize)

	return pl, nil
}

// Remove takes a broker ID and partition and
// removes the mapping association.
func (m Mappings) Remove(id int, p Partition) error {
	if _, exist := m[id]; !exist {
		return NoMappingForBroker{id: id}
	}

	if _, exist := m[id][p.Topic]; !exist {
		return NoMappingForTopic{id: id, topic: p.Topic}
	}

	var newPl PartitionList
	for _, partn := range m[id][p.Topic] {
		if !partn.Equal(p) {
			newPl = append(newPl, partn)
		} else {
		}
	}

	m[id][p.Topic] = newPl

	return nil
}
