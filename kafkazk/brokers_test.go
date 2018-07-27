package kafkazk

import (
	"testing"
)

func TestBrokerMapFromPartitionMap(t *testing.T) {
	zk := &Mock{}
	bmm, _ := zk.GetAllBrokerMeta(false)
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	forceRebuild := false

	brokers := BrokerMapFromPartitionMap(pm, bmm, forceRebuild)
	expected := newMockBrokerMap()

	for id, b := range brokers {
		switch {
		case b.ID != expected[id].ID:
			t.Errorf("Expected id %d, got %d for broker %d",
				expected[id].ID, b.ID, id)
		case b.Locality != expected[id].Locality:
			t.Errorf("Expected locality %s, got %s for broker %d",
				expected[id].Locality, b.Locality, id)
		case b.Used != expected[id].Used:
			t.Errorf("Expected used %d, got %d for broker %d",
				expected[id].Used, b.Used, id)
		case b.Replace != expected[id].Replace:
			t.Errorf("Expected replace %t, got %t for broker %d",
				expected[id].Replace, b.Replace, id)
		}
	}
}

func TestUpdate(t *testing.T) {
	zk := &Mock{}
	bmm, _ := zk.GetAllBrokerMeta(false)
	bm := newMockBrokerMap()
	// 1001 isn't in the list, should
	// add to the Missing count.
	delete(bmm, 1001)
	// 1002 will be in the list but
	// missing, should add to the
	// OldMissing count.
	delete(bmm, 1002)

	// 1006 doesn't exist in the meta map.
	// This should also add to the missing.
	stat := bm.Update([]int{1002, 1003, 1005, 1006}, bmm)

	if stat.New != 1 {
		t.Errorf("Expected New count of 1, got %d", stat.New)
	}
	if stat.Missing != 2 {
		t.Errorf("Expected Missing count of 2, got %d", stat.Missing)
	}
	if stat.OldMissing != 1 {
		t.Errorf("Expected OldMissing count of 1, got %d", stat.OldMissing)
	}
	if stat.Replace != 2 {
		t.Errorf("Expected Replace count of 2, got %d", stat.Replace)
	}

	// TODO assert that all Broker fields
	// are also what we expected.
}

// func TestSubStorageAll(t *testing.T) {} // TODO

func TestFilteredList(t *testing.T) {
	bm := newMockBrokerMap()
	bm[1003].Replace = true

	nl := bm.filteredList()
	expected := map[int]struct{}{
		1001: struct{}{},
		1002: struct{}{},
		1004: struct{}{},
	}

	for _, b := range nl {
		if _, exist := expected[b.ID]; !exist {
			t.Errorf("Broker ID %d shouldn't exist", b.ID)
		}
	}
}

func TestBrokerMapCopy(t *testing.T) {
	bm1 := newMockBrokerMap()
	bm2 := bm1.Copy()

	if len(bm1) != len(bm2) {
		t.Errorf("Unexpected length inequality")
	}

	for b := range bm1 {
		switch {
		case bm1[b].ID != bm2[b].ID:
			t.Errorf("id field mismatch")
		case bm1[b].Locality != bm2[b].Locality:
			t.Errorf("locality field mismatch")
		case bm1[b].Used != bm2[b].Used:
			t.Errorf("used field mismatch")
		case bm1[b].Replace != bm2[b].Replace:
			t.Errorf("replace field mismatch")
		case bm1[b].StorageFree != bm2[b].StorageFree:
			t.Errorf("StorageFree field mismatch")
		}
	}
}

func TestSortPseudoShuffle(t *testing.T) {
	bl := newMockBrokerMap2().filteredList()

	// Test with seed val of 1.
	expected := []int{1001, 1002, 1005, 1004, 1007, 1003, 1006}
	bl.SortPseudoShuffle(1)

	for i, b := range bl {
		if b.ID != expected[i] {
			t.Errorf("Expected broker %d, got %d", expected[i], b.ID)
		}
	}

	// Test with seed val of 3.
	expected = []int{1001, 1005, 1002, 1004, 1003, 1006, 1007}
	bl.SortPseudoShuffle(3)

	for i, b := range bl {
		if b.ID != expected[i] {
			t.Errorf("Expected broker %d, got %d", expected[i], b.ID)
		}
	}
}

func TestBrokerStringToSlice(t *testing.T) {
	bs := BrokerStringToSlice("1001,1002,1003,1003")
	expected := []int{1001, 1002, 1003}

	if len(bs) != 3 {
		t.Errorf("Expected slice len of 3, got %d", len(bs))
	}

	for i, b := range bs {
		if b != expected[i] {
			t.Errorf("Expected ID %d, got %d", expected[i], b)
		}
	}
}

func newMockBrokerMap() BrokerMap {
	return BrokerMap{
		0:    &Broker{ID: 0, Replace: true},
		1001: &Broker{ID: 1001, Locality: "a", Used: 3, Replace: false, StorageFree: 100.00},
		1002: &Broker{ID: 1002, Locality: "b", Used: 3, Replace: false, StorageFree: 200.00},
		1003: &Broker{ID: 1003, Locality: "c", Used: 2, Replace: false, StorageFree: 300.00},
		1004: &Broker{ID: 1004, Locality: "a", Used: 2, Replace: false, StorageFree: 400.00},
	}
}

func newMockBrokerMap2() BrokerMap {
	return BrokerMap{
		0:    &Broker{ID: 0, Replace: true},
		1001: &Broker{ID: 1001, Locality: "a", Used: 2, Replace: false, StorageFree: 100.00},
		1002: &Broker{ID: 1002, Locality: "b", Used: 2, Replace: false, StorageFree: 200.00},
		1003: &Broker{ID: 1003, Locality: "c", Used: 3, Replace: false, StorageFree: 300.00},
		1004: &Broker{ID: 1004, Locality: "a", Used: 2, Replace: false, StorageFree: 400.00},
		1005: &Broker{ID: 1005, Locality: "b", Used: 2, Replace: false, StorageFree: 400.00},
		1006: &Broker{ID: 1006, Locality: "c", Used: 3, Replace: false, StorageFree: 400.00},
		1007: &Broker{ID: 1007, Locality: "a", Used: 3, Replace: false, StorageFree: 400.00},
	}
}
