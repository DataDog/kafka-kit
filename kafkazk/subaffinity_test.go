package kafkazk

import (
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
		t.Errorf("Expected error")
	}

	// Should still fail since
	// 1002 doesn't satisfy
	// constraints as an affinity.
	bm[1002].New = true
	_, err = bm.SubstitutionAffinities(pm)
	if err == nil {
		t.Errorf("Expected error")
	}

	bm[1004].New = true
	sa, err := bm.SubstitutionAffinities(pm)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}

	if sa[1001].ID != 1004 {
		t.Errorf("Expected substitution affinity 1001->1004")
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
