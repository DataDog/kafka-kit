package kafkazk

import (
	"testing"
)

func TestSelectBrokerByCount(t *testing.T) {
	localities := []string{"a", "b", "c"}
	bl := BrokerList{}

	for i := 0; i < 4; i++ {
		b := &Broker{
			ID:       1000 + i,
			Locality: localities[i%3],
			Used:     i,
		}

		bl = append(bl, b)
	}

	c := NewConstraints()
	// Removes ID 1000 as a candidate.
	c.id[1000] = true
	// Removes any brokers with locality
	// "b" as candidates.
	c.locality["b"] = true

	p := ConstraintsParams{
		SelectorMethod: "count",
	}

	b, _ := c.SelectBroker(bl, p)
	// 1002 should be the first available.
	if b.ID != 1002 {
		t.Errorf("Expected candidate with ID 1002, got %d", b.ID)
	}

	b, _ = c.SelectBroker(bl, p)
	// 1003 should be next available.
	if b.ID != 1003 {
		t.Errorf("Expected candidate with ID 1003, got %d", b.ID)
	}

	_, err := c.SelectBroker(bl, p)
	if err == nil {
		t.Error("Expected exhausted candidate list")
	}
}

func TestSelectBrokerByStorage(t *testing.T) {
	localities := []string{"a", "b", "c"}
	bl := BrokerList{}

	for i := 0; i < 4; i++ {
		b := &Broker{
			ID:          1000 + i,
			Locality:    localities[i%3],
			Used:        i,
			StorageFree: float64(1000 * i),
		}

		bl = append(bl, b)
	}

	c := NewConstraints()
	// Removes any brokers with locality
	// "b" as candidates.
	c.locality["c"] = true

	p := ConstraintsParams{
		SelectorMethod: "storage",
		RequestSize:    1000.00,
	}

	b, _ := c.SelectBroker(bl, p)

	// 1003 should be the first available.
	if b.ID != 1003 {
		t.Errorf("Expected candidate with ID 1003, got %d", b.ID)
	}

	// Ensure that the request size was deducted
	// from the broker storage.
	if b.StorageFree != 2000.00 {
		t.Errorf("Expected StorageFree of 2000.00, got %2.f", b.StorageFree)
	}

	b, _ = c.SelectBroker(bl, p)
	// 1003 should be next available.
	if b.ID != 1001 {
		t.Errorf("Expected candidate with ID 1001, got %d", b.ID)
	}

	if b.StorageFree != 0.00 {
		t.Errorf("Expected StorageFree of 0.00, got %2.f", b.StorageFree)
	}

	_, err := c.SelectBroker(bl, p)
	if err == nil {
		t.Error("Expected exhausted candidate list")
	}
}

func TestBestCandidateByCount(t *testing.T) {
	localities := []string{"a", "b", "c"}
	bl := BrokerList{}

	for i := 0; i < 4; i++ {
		b := &Broker{
			ID:       1000 + i,
			Locality: localities[i%3],
			Used:     i,
		}

		bl = append(bl, b)
	}

	c := NewConstraints()
	// Removes ID 1000 as a candidate.
	c.id[1000] = true
	// Removes any brokers with locality
	// "b" as candidates.
	c.locality["b"] = true

	b, _ := bl.BestCandidate(c, "count", 1)
	// 1002 should be the first available.
	if b.ID != 1002 {
		t.Errorf("Expected candidate with ID 1002, got %d", b.ID)
	}

	b, _ = bl.BestCandidate(c, "count", 1)
	// 1003 should be next available.
	if b.ID != 1003 {
		t.Errorf("Expected candidate with ID 1003, got %d", b.ID)
	}

	_, err := bl.BestCandidate(c, "count", 1)
	if err == nil {
		t.Error("Expected exhausted candidate list")
	}
}

func TestBestCandidateByStorage(t *testing.T) {
	localities := []string{"a", "b", "c"}
	bl := BrokerList{}

	for i := 0; i < 4; i++ {
		b := &Broker{
			ID:          1000 + i,
			Locality:    localities[i%3],
			Used:        i,
			StorageFree: float64(1000 * i),
		}

		bl = append(bl, b)
	}

	c := NewConstraints()
	// Removes any brokers with locality
	// "b" as candidates.
	c.locality["c"] = true
	// Sets request size.
	c.requestSize = 1000.00

	b, _ := bl.BestCandidate(c, "storage", 1)

	// 1003 should be the first available.
	if b.ID != 1003 {
		t.Errorf("Expected candidate with ID 1003, got %d", b.ID)
	}

	// Ensure that the request size was deducted
	// from the broker storage.
	if b.StorageFree != 2000.00 {
		t.Errorf("Expected StorageFree of 2000.00, got %2.f", b.StorageFree)
	}

	b, _ = bl.BestCandidate(c, "storage", 1)
	// 1003 should be next available.
	if b.ID != 1001 {
		t.Errorf("Expected candidate with ID 1001, got %d", b.ID)
	}

	if b.StorageFree != 0.00 {
		t.Errorf("Expected StorageFree of 0.00, got %2.f", b.StorageFree)
	}

	_, err := bl.BestCandidate(c, "storage", 1)
	if err == nil {
		t.Error("Expected exhausted candidate list")
	}
}

func TestConstraintsAdd(t *testing.T) {
	b1 := &Broker{ID: 1000, Locality: "a"}
	b2 := &Broker{ID: 1001, Locality: "b"}

	bl := BrokerList{b1}
	c := MergeConstraints(bl)
	c.Add(b2)

	bl = append(bl, b2)

	for _, b := range bl {
		if _, exists := c.id[b.ID]; !exists {
			t.Errorf("Expected ID %d to exist", b.ID)
		}
		if _, exists := c.locality[b.Locality]; !exists {
			t.Errorf("Expected ID %d to exist", b.ID)
		}
	}
}

func TestConstraintsPasses(t *testing.T) {
	c := NewConstraints()
	c.locality["a"] = true
	c.id[1000] = true

	// Fail on ID and/or locality.
	b1 := &Broker{ID: 1000, Locality: "a"}
	// Fail on locality only.
	b2 := &Broker{ID: 1001, Locality: "a"}
	// Fail on ID only.
	b3 := &Broker{ID: 1000, Locality: "b"}
	// Pass.
	b4 := &Broker{ID: 1001, Locality: "c"}

	bl := BrokerList{b1, b2, b3, b4}

	expected := []bool{false, false, false, true}

	for i, b := range bl {
		if p := c.passes(b); p != expected[i] {
			t.Errorf("Expected broker b%d return constraint check with %v", i, expected[i])
		}
	}
}

func TestConstraintsPassesWithParams(t *testing.T) {
	c := NewConstraints()
	c.locality["a"] = true
	c.id[1000] = true

	// Fail on ID and/or locality.
	b1 := &Broker{ID: 1000, Locality: "a"}
	// Fail on locality only.
	b2 := &Broker{ID: 1001, Locality: "a"}
	// Fail on ID only.
	b3 := &Broker{ID: 1000, Locality: "b"}
	// Pass.
	b4 := &Broker{ID: 1001, Locality: "c"}

	bl := BrokerList{b1, b2, b3, b4}

	expected := []bool{false, false, false, true}

	p := ConstraintsParams{
		MinUniqueRackIDs: 0,
	}

	// Basic tests.

	for i, b := range bl {
		if r := c.passesWithParams(b, p); r != expected[i] {
			t.Errorf("Expected broker b%d return constraint check with %v", i, expected[i])
		}
	}

	// MinUniqueRackIDs tests.

	p.MinUniqueRackIDs = 1

	// b2 should now pass.
	if b := c.passesWithParams(b2, p); b != true {
		t.Errorf("Expected broker b2 to pass with MinUniqueRackIDs set to 1")
	}

	p.MinUniqueRackIDs = 2

	// b2 should now fail again.
	if b := c.passesWithParams(b2, p); b != false {
		t.Errorf("Expected broker b2 to fail with MinUniqueRackIDs set to 2")
	}

	// b4 should still pass.
	if b := c.passesWithParams(b4, p); b != true {
		t.Errorf("Expected broker b4 to pass constraints")
	}

	// Storage tests.

	p.RequestSize = 500
	b4.StorageFree = 600

	if b := c.passesWithParams(b4, p); b != true {
		t.Errorf("Expected broker b4 to pass constraints")
	}

	p.RequestSize = 650

	// b4 should now fail due to insufficient storage.
	if b := c.passesWithParams(b4, p); b != false {
		t.Errorf("Expected broker b4 to fail constraints")
	}
}

func TestMergeConstraints(t *testing.T) {
	localities := []string{"a", "b", "c"}
	bl := BrokerList{}

	for i := 0; i < 5; i++ {
		b := &Broker{
			ID:       1000 + i,
			Locality: localities[i%3],
		}

		// Brokers marked for replacement
		// don't get merged in.
		if i == 4 {
			b.Replace = true
		}

		bl = append(bl, b)
	}

	c := NewConstraints()
	c.MergeConstraints(bl)

	// Check expected.
	for i := 1000; i < 1004; i++ {
		if _, exists := c.id[i]; !exists {
			t.Errorf("Expected ID %d to exist", i)
		}
	}

	for _, l := range localities {
		if _, exists := c.locality[l]; !exists {
			t.Errorf("Expected locality %s to exist", l)
		}
	}

	// Check excluded.
	if _, exists := c.id[1004]; exists {
		t.Error("ID 1004 shouldn't exist in the Constraints")
	}
}

// TODO deprecate.
func TestMergeConstraintsX(t *testing.T) {
	localities := []string{"a", "b", "c"}
	bl := BrokerList{}

	for i := 0; i < 5; i++ {
		b := &Broker{
			ID:       1000 + i,
			Locality: localities[i%3],
		}

		// Brokers marked for replacement
		// don't get merged in.
		if i == 4 {
			b.Replace = true
		}

		bl = append(bl, b)
	}

	c := MergeConstraints(bl)

	// Check expected.
	for i := 1000; i < 1004; i++ {
		if _, exists := c.id[i]; !exists {
			t.Errorf("Expected ID %d to exist", i)
		}
	}

	for _, l := range localities {
		if _, exists := c.locality[l]; !exists {
			t.Errorf("Expected locality %s to exist", l)
		}
	}

	// Check excluded.
	if _, exists := c.id[1004]; exists {
		t.Error("ID 1004 shouldn't exist in the Constraints")
	}
}
