package kafkazk

import (
	"math"
	"sort"
)

// DegreeDistribution counts broker to broker relationships.
type DegreeDistribution struct {
	// Relationships is a an adjacency list
	// where an edge between brokers is defined as
	// a common occupancy in at least one replica set.
	// For instance, given the replica set [1001,1002,1003],
	// ID 1002 has a relationship with 1001 and 1003.
	Relationships map[int]map[int]struct{}
}

// NewDegreeDistribution returns a new DegreeDistribution.
func NewDegreeDistribution() DegreeDistribution {
	return DegreeDistribution{
		Relationships: make(map[int]map[int]struct{}),
	}
}

// Add takes a []int of broker IDs representing a
// replica set and updates the adjacency lists for
// each broker in the set.
func (dd DegreeDistribution) Add(nodes []int) {
	for _, node := range nodes {
		if _, exists := dd.Relationships[node]; !exists {
			dd.Relationships[node] = make(map[int]struct{})
		}

		for _, neighbor := range nodes {
			if node != neighbor {
				dd.Relationships[node][neighbor] = struct{}{}
			}
		}
	}
}

// Count takes a node ID and returns the degree distribution.
func (dd DegreeDistribution) Count(n int) int {
	c, exists := dd.Relationships[n]
	if !exists {
		return 0
	}

	return len(c)
}

// DegreeDistributionStats holds general statistical
// information describing the DegreeDistribution counts.
type DegreeDistributionStats struct {
	Min float64
	Max float64
	Avg float64
}

// Stats returns a DegreeDistributionStats.
func (dd DegreeDistribution) Stats() DegreeDistributionStats {
	dds := DegreeDistributionStats{}
	if len(dd.Relationships) == 0 {
		return dds
	}

	vals := []int{}

	for node := range dd.Relationships {
		vals = append(vals, dd.Count(node))
	}

	sort.Ints(vals)
	var s int
	for _, v := range vals {
		s += v
	}

	dds.Min = float64(vals[0])
	dds.Max = float64(vals[len(vals)-1])
	dds.Avg = float64(s) / float64(len(vals))

	return dds
}

// DegreeDistribution returns the DegreeDistribution for the PartitionMap.
func (pm *PartitionMap) DegreeDistribution() DegreeDistribution {
	d := NewDegreeDistribution()

	for _, partn := range pm.Partitions {
		d.Add(partn.Replicas)
	}

	return d
}

// StorageDiff takes two BrokerMaps and returns a per broker ID
// diff in storage as a [2]float64: [absolute, percentage] diff.
func (b BrokerMap) StorageDiff(b2 BrokerMap) map[int][2]float64 {
	d := map[int][2]float64{}

	for bid := range b {
		if bid == StubBrokerID {
			continue
		}

		if _, exist := b2[bid]; !exist {
			continue
		}

		diff := b2[bid].StorageFree - b[bid].StorageFree
		p := diff / b[bid].StorageFree * 100
		d[bid] = [2]float64{diff, p}
	}

	return d
}

// StorageRangeSpread returns the range spread
// of free storage for all brokers in the BrokerMap.
func (b BrokerMap) StorageRangeSpread() float64 {
	l, h := b.minMax()
	// Return range spread.
	return (h - l) / l * 100
}

// StorageRange returns the range of free
// storage for all brokers in the BrokerMap.
func (b BrokerMap) StorageRange() float64 {
	l, h := b.minMax()
	// Return range.
	return h - l
}

func (b BrokerMap) minMax() (float64, float64) {
	// Get the high/low StorageFree values.
	h, l := 0.00, math.MaxFloat64

	for id := range b {
		if id == StubBrokerID {
			continue
		}

		v := b[id].StorageFree

		// Update the high/low.
		if v > h {
			h = v
		}

		if v < l {
			l = v
		}
	}

	return l, h
}

// StorageStdDev returns the standard deviation
// of free storage for all brokers in the BrokerMap.
func (b BrokerMap) StorageStdDev() float64 {
	var m float64
	var t float64
	var s float64
	var l float64

	for id := range b {
		if id == StubBrokerID {
			continue
		}
		l++
		t += b[id].StorageFree
	}

	m = t / l

	for id := range b {
		if id == StubBrokerID {
			continue
		}
		s += math.Pow(m-b[id].StorageFree, 2)
	}

	msq := s / l

	return math.Sqrt(msq)
}

// HMean returns the harmonic mean of broker storage free.
func (b BrokerMap) HMean() float64 {
	var t float64
	var c float64

	for _, br := range b {
		if br.ID != StubBrokerID && br.StorageFree > 0 {
			c++
			t += (1.00 / br.StorageFree)
		}
	}

	return c / t
}

// Mean returns the arithmetic mean of broker storage free.
func (b BrokerMap) Mean() float64 {
	var t float64
	var c float64

	for _, br := range b {
		if br.ID != StubBrokerID && br.StorageFree > 0 {
			c++
			t += br.StorageFree
		}
	}

	return t / c
}

// AboveMean returns a sorted []int of broker IDs that are above the mean
// by d percent (0.00 < d). The mean type is provided as a function f.
func (b BrokerMap) AboveMean(d float64, f func() float64) []int {
	m := f()
	var ids []int

	if d <= 0.00 {
		return ids
	}

	for _, br := range b {
		if br.ID == StubBrokerID {
			continue
		}

		if (br.StorageFree-m)/m > d {
			ids = append(ids, br.ID)
		}
	}

	sort.Ints(ids)

	return ids
}

// BelowMean returns a sorted []int of broker IDs that are below the mean
// by d percent (0.00 < d). The mean type is provided as a function f.
func (b BrokerMap) BelowMean(d float64, f func() float64) []int {
	m := f()
	var ids []int

	if d <= 0.00 {
		return ids
	}

	for _, br := range b {
		if br.ID == StubBrokerID {
			continue
		}

		if (m-br.StorageFree)/m > d {
			ids = append(ids, br.ID)
		}
	}

	sort.Ints(ids)

	return ids
}
