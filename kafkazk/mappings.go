package kafkazk

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
