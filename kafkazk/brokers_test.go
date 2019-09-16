package kafkazk

import (
	"testing"
)

func TestChanges(t *testing.T) {
	b := BrokerStatus{}

	if b.Changes() {
		t.Error("Expected return 'false'")
	}

	b.New = 1
	if !b.Changes() {
		t.Error("Expected return 'true'")
	}
	b.New = 0

	b.Missing = 1
	if !b.Changes() {
		t.Error("Expected return 'true'")
	}
	b.Missing = 0

	b.OldMissing = 1
	if !b.Changes() {
		t.Error("Expected return 'true'")
	}
	b.OldMissing = 0

	b.Replace = 1
	if !b.Changes() {
		t.Error("Expected return 'true'")
	}
}

func TestSortBrokerListByCount(t *testing.T) {
	b := newMockBrokerMap2()
	bl := b.Filter(func(b *Broker) bool { return true }).List()

	bl.SortByCount()

	var blIDs []int
	for _, br := range bl {
		blIDs = append(blIDs, br.ID)
	}

	expected := []int{1001, 1002, 1004, 1005, 1003, 1006, 1007}

	for i, br := range bl {
		if br.ID != expected[i] {
			t.Fatalf("Expected %v, got %v", expected, blIDs)
		}
	}
}

func TestSortBrokerListByStorage(t *testing.T) {
	b := newMockBrokerMap2()
	bl := b.Filter(func(b *Broker) bool { return true }).List()

	bl.SortByStorage()

	var blIDs []int
	for _, br := range bl {
		blIDs = append(blIDs, br.ID)
	}

	expected := []int{1004, 1005, 1006, 1007, 1003, 1002, 1001}

	for i, br := range bl {
		if br.ID != expected[i] {
			t.Fatalf("Expected %v, got %v", expected, blIDs)
		}
	}
}

func TestSortBrokerListByID(t *testing.T) {
	b := newMockBrokerMap2()
	bl := b.Filter(func(b *Broker) bool { return true }).List()

	bl.SortByID()

	var blIDs []int
	for _, br := range bl {
		blIDs = append(blIDs, br.ID)
	}

	expected := []int{1001, 1002, 1003, 1004, 1005, 1006, 1007}

	for i, br := range bl {
		if br.ID != expected[i] {
			t.Fatalf("Expected %v, got %v", expected, blIDs)
		}
	}
}

func TestSortPseudoShuffle(t *testing.T) {
	b := newMockBrokerMap2()
	bl := b.Filter(func(b *Broker) bool { return true }).List()

	// Test with seed val of 1.
	expected := []int{1001, 1002, 1005, 1004, 1007, 1003, 1006}
	bl.SortPseudoShuffle(1)

	for i, br := range bl {
		if br.ID != expected[i] {
			t.Errorf("Expected broker %d, got %d", expected[i], br.ID)
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
	stat, _ := bm.Update([]int{1002, 1003, 1005, 1006}, bmm)

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

	// Ensure all broker IDs are in the map.
	for _, id := range []int{StubBrokerID, 1001, 1002, 1003, 1004, 1005} {
		if _, ok := bm[id]; !ok {
			t.Errorf("Expected presence of ID %d", id)
		}
	}

	// Test that brokers have appropriately
	// updated fields.

	if !bm[1001].Missing {
		t.Error("Expected ID 1001 Missing == true")
	}
	if !bm[1001].Replace {
		t.Error("Expected ID 1001 Replace == true")
	}

	if !bm[1002].Missing {
		t.Error("Expected ID 1002 Missing == true")
	}
	if !bm[1002].Replace {
		t.Error("Expected ID 1002 Replace == true")
	}

	if bm[1003].Missing || bm[1003].Replace || bm[1003].New {
		t.Error("Unexpected fields set for ID 1003")
	}

	if bm[1004].Missing {
		t.Error("Expected ID 1004 Missing != true")
	}
	if !bm[1004].Replace {
		t.Error("Expected ID 1004 Replace == true")
	}

	if bm[1005].Missing || bm[1005].Replace {
		t.Error("Unexpected fields set for ID 1005")
	}
	if !bm[1005].New {
		t.Error("Expected ID 1005 New == true")
	}

	if _, exists := bm[1006]; exists {
		t.Error("ID 1006 unexpectedly exists in BrokerMap")
	}
}

func TestUpdateIncludeExisting(t *testing.T) {
	zk := &Mock{}
	bmm, _ := zk.GetAllBrokerMeta(false)
	bm := newMockBrokerMap()

	bm.Update([]int{-1}, bmm)

	// Ensure all broker IDs are in the map.
	for _, id := range []int{StubBrokerID, 1001, 1002, 1003, 1004} {
		if _, ok := bm[id]; !ok {
			t.Errorf("Expected presence of ID %d", id)
		}
	}
}

func TestSubStorageAll(t *testing.T) {
	bm := newMockBrokerMap()
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pmm := NewPartitionMetaMap()

	pmm["test_topic"] = map[int]*PartitionMeta{
		0: &PartitionMeta{Size: 30},
		1: &PartitionMeta{Size: 35},
		2: &PartitionMeta{Size: 60},
		3: &PartitionMeta{Size: 45},
	}

	allBrokers := func(b *Broker) bool { return true }
	err := bm.SubStorage(pm, pmm, allBrokers)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	expected := map[int]float64{
		1001: 225,
		1002: 310,
		1003: 405,
		1004: 505,
	}

	for _, b := range bm {
		if b.StorageFree != expected[b.ID] {
			t.Errorf("Expected '%f' StorageFree for ID %d, got '%f'",
				expected[b.ID], b.ID, b.StorageFree)
		}
	}
}

func TestSubStorageReplacements(t *testing.T) {
	bm := newMockBrokerMap()
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pmm := NewPartitionMetaMap()

	pmm["test_topic"] = map[int]*PartitionMeta{
		0: &PartitionMeta{Size: 30},
		1: &PartitionMeta{Size: 35},
		2: &PartitionMeta{Size: 60},
		3: &PartitionMeta{Size: 45},
	}

	bm[1003].Replace = true

	replacedBrokers := func(b *Broker) bool { return b.Replace }
	err := bm.SubStorage(pm, pmm, replacedBrokers)
	if err != nil {
		t.Errorf("Unexpected error: %s", err)
	}

	// Only 1003 should be affected.
	expected := map[int]float64{
		1001: 100,
		1002: 200,
		1003: 405,
		1004: 400,
	}

	for _, b := range bm {
		if b.StorageFree != expected[b.ID] {
			t.Errorf("Expected '%f' StorageFree for ID %d, got '%f'",
				expected[b.ID], b.ID, b.StorageFree)
		}
	}
}

func TestMapFilter(t *testing.T) {
	bm1 := newMockBrokerMap2()
	f := func(b *Broker) bool {
		if b.Locality == "a" {
			return true
		}
		return false
	}

	bm2 := bm1.Filter(f)

	if len(bm2) != 3 {
		t.Errorf("Expected BrokerMap len of 3, got %d", len(bm2))
	}

	for _, id := range []int{1001, 1004, 1007} {
		if _, exist := bm2[id]; !exist {
			t.Errorf("Expected ID %d in BrokerMap", id)
		}
	}
}

func TestListFilter(t *testing.T) {
	bl1 := newMockBrokerMap2().List()
	f := func(b *Broker) bool {
		if b.Locality == "a" {
			return true
		}
		return false
	}

	bl2 := bl1.Filter(f)

	if len(bl2) != 3 {
		t.Errorf("Expected BrokerList len of 3, got %d", len(bl2))
	}

	bm := BrokerMap{}
	for _, b := range bl2 {
		bm[b.ID] = nil
	}

	for _, id := range []int{1001, 1004, 1007} {
		if _, exist := bm[id]; !exist {
			t.Errorf("Expected ID %d in BrokerList", id)
		}
	}
}

func TestBrokerMapFromPartitionMap(t *testing.T) {
	zk := &Mock{}
	bmm, _ := zk.GetAllBrokerMeta(false)
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	forceRebuild := false

	// Include an offline broker / value -1.
	pm.Partitions = append(pm.Partitions, Partition{Topic: "test_topic", Partition: 4, Replicas: []int{-1}})

	brokers := BrokerMapFromPartitionMap(pm, bmm, forceRebuild)
	expected := newMockBrokerMap()

	for id, b := range brokers {
		_, exist := expected[id]
		switch {
		case !exist:
			t.Errorf("Unexpected id %d", id)
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

func TestBrokerMapCopy(t *testing.T) {
	bm1 := newMockBrokerMap()
	bm2 := bm1.Copy()

	if len(bm1) != len(bm2) {
		t.Error("Unexpected length inequality")
	}

	for b := range bm1 {
		switch {
		case bm1[b].ID != bm2[b].ID:
			t.Error("id field mismatch")
		case bm1[b].Locality != bm2[b].Locality:
			t.Error("locality field mismatch")
		case bm1[b].Used != bm2[b].Used:
			t.Error("used field mismatch")
		case bm1[b].Replace != bm2[b].Replace:
			t.Error("replace field mismatch")
		case bm1[b].StorageFree != bm2[b].StorageFree:
			t.Error("StorageFree field mismatch")
		}
	}
}

func TestBrokerCopy(t *testing.T) {
	bm := newMockBrokerMap()
	b1 := bm[1001]
	b2 := b1.Copy()

	switch {
	case b1.ID != b2.ID:
		t.Error("ID field mistmatch")
	case b1.Locality != b2.Locality:
		t.Error("Locality field mistmatch")
	case b1.Used != b2.Used:
		t.Error("Used field mistmatch")
	case b1.StorageFree != b2.StorageFree:
		t.Error("StorageFree field mistmatch")
	case b1.Replace != b2.Replace:
		t.Error("Replace field mistmatch")
	case b1.Missing != b2.Missing:
		t.Error("Missing field mistmatch")
	case b1.New != b2.New:
		t.Error("New field mistmatch")
	}
}

func newMockBrokerMap() BrokerMap {
	return BrokerMap{
		StubBrokerID: &Broker{ID: StubBrokerID, Replace: true},
		1001:         &Broker{ID: 1001, Locality: "a", Used: 3, Replace: false, StorageFree: 100.00},
		1002:         &Broker{ID: 1002, Locality: "b", Used: 3, Replace: false, StorageFree: 200.00},
		1003:         &Broker{ID: 1003, Locality: "c", Used: 2, Replace: false, StorageFree: 300.00},
		1004:         &Broker{ID: 1004, Locality: "a", Used: 2, Replace: false, StorageFree: 400.00},
	}
}

func newMockBrokerMap2() BrokerMap {
	return BrokerMap{
		StubBrokerID: &Broker{ID: StubBrokerID, Replace: true},
		1001:         &Broker{ID: 1001, Locality: "a", Used: 2, Replace: false, StorageFree: 100.00},
		1002:         &Broker{ID: 1002, Locality: "b", Used: 2, Replace: false, StorageFree: 200.00},
		1003:         &Broker{ID: 1003, Locality: "c", Used: 3, Replace: false, StorageFree: 300.00},
		1004:         &Broker{ID: 1004, Locality: "a", Used: 2, Replace: false, StorageFree: 400.00},
		1005:         &Broker{ID: 1005, Locality: "b", Used: 2, Replace: false, StorageFree: 400.00},
		1006:         &Broker{ID: 1006, Locality: "c", Used: 3, Replace: false, StorageFree: 400.00},
		1007:         &Broker{ID: 1007, Locality: "a", Used: 3, Replace: false, StorageFree: 400.00},
	}
}
