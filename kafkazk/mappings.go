package kafkazk

// Mappings is a mapping of broker IDs
// to currently held partition as a partitionList.
type Mappings map[int]map[string]partitionList

// NewMappings returns a new Mappings.
func NewMappings() Mappings {
	return map[int]map[string]partitionList{}
}

// // Add takes a broker ID and Partition and
// // adds the association to the Mapping.
// func (m Mapping) Add(id int, p Partition) {
// }

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
