package kafkazk

import (
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
