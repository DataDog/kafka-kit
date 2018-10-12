package kafkazk

import (
	"sort"
	"testing"
)

func TestConstraintsMatch(t *testing.T) {
	bm := newMockBrokerMap()

	ref := bm[1001]
	delete(bm, 1001)

	m := map[*Broker]struct{}{}
	for broker := range bm {
		m[bm[broker]] = struct{}{}
	}

	b, err := constraintsMatch(ref, m)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if b.ID != 1004 {
		t.Errorf("Expected broker ID 1004, got %d", b.ID)
	}

	//  Check that 1004 has been
	// removed from the map.
	if _, exists := m[b]; exists {
		t.Error("Broker 1004 unexpectedly exists in map")
	}

	// Expects error.
	ref = bm[1002]
	delete(bm, 1002)

	m = map[*Broker]struct{}{}
	for broker := range bm {
		m[bm[broker]] = struct{}{}
	}

	_, err = constraintsMatch(ref, m)
	expected := "Insufficient free brokers for locality b"
	if err.Error() != expected {
		t.Errorf("Unexpected error '%s': %s", expected, err)
	}

}

func TestSubstitutionAffinities(t *testing.T) {
	z := &Mock{}
	pm, _ := z.GetPartitionMap("test_topic")

	bm := newMockBrokerMap()
	bm[1001].Replace = true

	// Should error because no
	// broker is available marked as new.
	_, err := bm.SubstitutionAffinities(pm)
	if err == nil {
		t.Error("Expected error")
	}

	// Should still fail since
	// 1002 doesn't satisfy
	// constraints as an affinity.
	bm[1002].New = true
	_, err = bm.SubstitutionAffinities(pm)
	if err == nil {
		t.Error("Expected error")
	}

	bm[1004].New = true
	sa, err := bm.SubstitutionAffinities(pm)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	if sa[1001].ID != 1004 {
		t.Error("Expected substitution affinity 1001->1004")
	}

	// Should fail. We have two
	// replacements but only 1
	// new broker available.
	bm[1002].Replace = true
	bm[1002].New = false

	_, err = bm.SubstitutionAffinities(pm)
	expected := "Insufficient number of new brokers"
	if err.Error() != expected {
		t.Errorf("Expected error '%s', got %s", expected, err)
	}
}

func TestSubstitutionAffinitiesInferred(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString2("test_topic"))

	bm := newMockBrokerMap()
	bm[1001] = &Broker{
		ID:      1001,
		Replace: true,
		Missing: true,
	}

	sort.Sort(pm.Partitions)
	pm.Partitions[2].Replicas = []int{1003, 1001}

	// Our mock data starts as follows:

	// 1001, Locality: missing, unknown
	// 1002, Locality: "b"
	// 1003, Locality: "c"
	// 1004, Locality: "a"

	// x indicates former 1001 positions.

	// {"topic":"test_topic","partition":0,"replicas":[x,1002]}
	// {"topic":"test_topic","partition":1,"replicas":[1002,x]}
	// {"topic":"test_topic","partition":2,"replicas":[1003,1001]}
	// {"topic":"test_topic","partition":3,"replicas":[1004,1003]}
	// {"topic":"test_topic","partition":4,"replicas":[1004,1003]}
	// {"topic":"test_topic","partition":5,"replicas":[1004,1003]}
	// {"topic":"test_topic","partition":6,"replicas":[1004,1003]}

	// Neither locality b nor c can be sourced for substitutions.

	// Should fail.
	bm[1005] = &Broker{
		ID:       1005,
		Locality: "b",
		New:      true,
	}

	_, err := bm.SubstitutionAffinities(pm)
	if err == nil {
		t.Errorf("Expected error, ID 1005 is not a suitable replacement")
	}

	// Also should fail.
	bm[1006] = &Broker{
		ID:       1006,
		Locality: "c",
		New:      true,
	}

	_, err = bm.SubstitutionAffinities(pm)
	if err == nil {
		t.Errorf("Expected error, ID 1005 is not a suitable replacement")
	}

	// Should succeed.
	bm[1007] = &Broker{
		ID:       1007,
		Locality: "a",
		New:      true,
	}

	sa, err := bm.SubstitutionAffinities(pm)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}
	if sa[1001].ID != 1007 {
		t.Error("Expected substitution affinity 1001->1007")
	}

}
