package kafkazk

import (
	"fmt"
	"math"
	"sort"
	"testing"
)

func TestDegreeDistribution(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString3("test_topic"))
	dd := pm.DegreeDistribution()

	expected := map[int][]int{
		1001: []int{1002, 1003, 1004},
		1002: []int{1001, 1003, 1004},
		1003: []int{1001, 1004, 1002},
		1004: []int{1001, 1002, 1003, 1005},
		1005: []int{1004},
	}

	// Check that expected relationships exist.
	for id := range expected {
		for _, r := range expected[id] {
			if _, exists := dd.Relationships[id]; !exists {
				t.Errorf("[%d] Expected %d in relationships: %v", id, r, dd.Relationships[id])
			}
		}
	}

	// Check that no unexpected relationships exist.
	for id := range dd.Relationships {
		for r := range dd.Relationships[id] {
			var found bool
			for _, e := range expected[id] {
				if r == e {
					found = true
				}
			}
			if !found {
				t.Errorf("[%d] Unexpected %d in relationships: %v", id, r, expected[id])
			}
		}
	}

	// Test Count method.
	expectedCounts := map[int]int{
		1001: 3,
		1002: 3,
		1003: 3,
		1004: 4,
		1005: 1,
	}

	for id, count := range expectedCounts {
		c := dd.Count(id)
		if count != c {
			t.Errorf("[%d] Expected count %d, got %d", id, count, c)
		}
	}

	// Test Stats method.
	stats := dd.Stats()

	if stats.Min != 1.00 {
		t.Errorf("Expected min val %f, got %f", 1.00, stats.Min)
	}
	if stats.Max != 4.00 {
		t.Errorf("Expected max val %f, got %f", 4.00, stats.Max)
	}
	if stats.Avg != 2.80 {
		t.Errorf("Expected avg val %f, got %f", 2.80, stats.Avg)
	}
}

func TestBrokerMapStorageDiff(t *testing.T) {
	bm1 := newStubBrokerMap()
	bm2 := newStubBrokerMap()

	bm2[1001].StorageFree = 200.00
	bm2[1002].StorageFree = 100.00

	diff := bm1.StorageDiff(bm2)

	expected := map[int][2]float64{
		1001: [2]float64{100.00, 100.00},
		1002: [2]float64{-100, -50.00},
	}

	for bid, v := range expected {
		if v[0] != diff[bid][0] {
			t.Errorf("Expected diff value of %f, got %f\n", v[0], diff[bid][0])
		}

		if v[1] != diff[bid][1] {
			t.Errorf("Expected diff percent of %f, got %f\n", v[1], diff[bid][1])
		}
	}
}

func TestBrokerMapStorageRangeSpread(t *testing.T) {
	bm := newStubBrokerMap()
	rs := bm.StorageRangeSpread()

	if rs != 300.00 {
		t.Errorf("Expected storage range spread 300, got %f", rs)
	}
}

func TestBrokerMapStorageRange(t *testing.T) {
	bm := newStubBrokerMap()
	r := bm.StorageRange()

	if r != 300 {
		t.Errorf("Expected storage range of 300, got %f", r)
	}
}

func TestBrokerMapStorageStdDev(t *testing.T) {
	bm := newStubBrokerMap()

	sd := math.Round(bm.StorageStdDev()/0.001) * 0.001

	if sd != 111.803000 {
		t.Errorf("Expected storage standard deviation 111.803000, got %f", sd)
	}
}

func TestBrokerListSort(t *testing.T) {
	b := newStubBrokerMap()
	bl := b.Filter(func(b *Broker) bool { return true }).List()

	// Test sort brokersByStorage.
	sort.Sort(brokersByStorage(bl))

	expected := []int{1004, 1003, 1002, 1001}

	for i, br := range bl {
		if br.ID != expected[i] {
			t.Errorf("Expected broker %d, got %d", expected[i], br.ID)
		}
	}
	// Test sort brokersByCount.
	sort.Sort(brokersByCount(bl))

	expected = []int{1003, 1004, 1001, 1002}

	for i, br := range bl {
		if br.ID != expected[i] {
			t.Errorf("Expected broker %d, got %d", expected[i], br.ID)
		}
	}
}

func TestHMean(t *testing.T) {
	bm := newStubBrokerMap2()

	m := fmt.Sprintf("%.4f", bm.HMean())
	if m != "247.0588" {
		t.Errorf("Expected harmonic mean of 247.0588, got %s", m)
	}
}

func TestMean(t *testing.T) {
	bm := newStubBrokerMap2()

	m := fmt.Sprintf("%.4f", bm.Mean())
	if m != "314.2857" {
		t.Errorf("Expected harmonic mean of 314.2857, got %s", m)
	}
}

func TestAboveMean(t *testing.T) {
	bm := newStubBrokerMap2()

	// With HMean.
	tests := map[float64][]int{
		0.20: []int{1003, 1004, 1005, 1006, 1007},
		0.60: []int{1004, 1005, 1006, 1007},
		0.80: []int{},
	}

	for d, expected := range tests {
		if results := bm.AboveMean(d, bm.HMean); !sameIDs(results, expected) {
			t.Errorf("Expected %v, got %v for distance %.2f", expected, results, d)
		}
	}

	// With Mean.
	tests = map[float64][]int{
		0.20: []int{1004, 1005, 1006, 1007},
		0.60: []int{},
	}

	for d, expected := range tests {
		if results := bm.AboveMean(d, bm.Mean); !sameIDs(results, expected) {
			t.Errorf("Expected %v, got %v for distance %.2f", expected, results, d)
		}
	}
}

func TestBelowMean(t *testing.T) {
	bm := newStubBrokerMap2()

	// With HMean.
	tests := map[float64][]int{
		0.10: []int{1001, 1002},
		0.20: []int{1001},
	}

	for d, expected := range tests {
		if results := bm.BelowMean(d, bm.HMean); !sameIDs(results, expected) {
			t.Errorf("Expected %v, got %v for distance %.2f", expected, results, d)
		}
	}

	// With Mean
	tests = map[float64][]int{
		0.10: []int{1001, 1002},
		0.20: []int{1001, 1002},
		0.60: []int{1001},
	}

	for d, expected := range tests {
		if results := bm.BelowMean(d, bm.Mean); !sameIDs(results, expected) {
			t.Errorf("Expected %v, got %v for distance %.2f", expected, results, d)
		}
	}
}

func sameIDs(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}

	for n := range a {
		if a[n] != b[n] {
			return false
		}
	}

	return true
}
