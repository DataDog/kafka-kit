package kafkazk

import (
	"sort"
)

// DegreeDistribution holds broker to
// broker relationships.
type DegreeDistribution struct {
	// Relationships is a an adjacency list
	// where an edge between brokers is defined as
	// a common occupancy in at least one replica set.
	// For instance, given the replica set [1001,1002,1003],
	// ID 1002 has a relationship with 1001 and 1003.
	Relationships map[int]map[int]interface{}
}

// NewDegreeDistribution returns a new DegreeDistribution.
func NewDegreeDistribution() DegreeDistribution {
	return DegreeDistribution{
		Relationships: make(map[int]map[int]interface{}),
	}
}

// Add takes a []int of broker IDs representing a
// replica set and updates the adjacency lists for
// each broker in the set.
func (dd DegreeDistribution) Add(nodes []int) {
	for _, node := range nodes {
		if _, exists := dd.Relationships[node]; !exists {
			dd.Relationships[node] = make(map[int]interface{})
		}

		for _, neighbor := range nodes {
			if node != neighbor {
				dd.Relationships[node][neighbor] = nil
			}
		}
	}
}

// Count takes a node ID and returns the
// degree distribution.
func (dd DegreeDistribution) Count(n int) int {
	c, exists := dd.Relationships[n]
	if !exists {
		return 0
	}

	return len(c)
}

// DegreeDistributionStats holds general
// statistical information describing the
// DegreeDistribution counts.
type DegreeDistributionStats struct {
	Min float64
	Max float64
	Avg float64
}

// Stats returns a DegreeDistributionStats.
func (dd DegreeDistribution) Stats() DegreeDistributionStats {
	vals := []int{}

	for node := range dd.Relationships {
		vals = append(vals, dd.Count(node))
	}

	sort.Ints(vals)
	var s int
	for _, v := range vals {
		s += v
	}

	return DegreeDistributionStats{
		Min: float64(vals[0]),
		Max: float64(vals[len(vals)-1]),
		Avg: float64(s) / float64(len(vals)),
	}
}

// DegreeDistribution returns the DegreeDistribution
// for the PartitionMap.
func (pm *PartitionMap) DegreeDistribution() DegreeDistribution {
	d := NewDegreeDistribution()

	for _, partn := range pm.Partitions {
		d.Add(partn.Replicas)
	}

	return d
}
