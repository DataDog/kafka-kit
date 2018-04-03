package kafkazk

import (
	"sort"
	"testing"
)

func TestBrokerMapFromTopicMap(t *testing.T) {
	zk := &Mock{}
	bmm, _ := zk.GetAllBrokerMeta(false)
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	forceRebuild := false

	brokers := BrokerMapFromTopicMap(pm, bmm, forceRebuild)
	expected := newMockBrokerMap()

	for id, b := range brokers {
		switch {
		case b.id != expected[id].id:
			t.Errorf("Expected id %d, got %d for broker %d",
				expected[id].id, b.id, id)
		case b.locality != expected[id].locality:
			t.Errorf("Expected locality %s, got %s for broker %d",
				expected[id].locality, b.locality, id)
		case b.used != expected[id].used:
			t.Errorf("Expected used %d, got %d for broker %d",
				expected[id].used, b.used, id)
		case b.replace != expected[id].replace:
			t.Errorf("Expected replace %b, got %b for broker %d",
				expected[id].replace, b.replace, id)
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

}

// func TestSubStorageAll(t *testing.T) {} // TODO

func TestFilteredList(t *testing.T) {
	bm := newMockBrokerMap()
	bm[1003].replace = true

	nl := bm.filteredList()
	expected := map[int]interface{}{
		1001: nil,
		1002: nil,
		1004: nil,
	}

	for _, b := range nl {
		if _, exist := expected[b.id]; !exist {
			t.Errorf("Broker ID %d shouldn't exist", b.id)
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
		case bm1[b].id != bm2[b].id:
			t.Errorf("id field mismatch")
		case bm1[b].locality != bm2[b].locality:
			t.Errorf("locality field mismatch")
		case bm1[b].used != bm2[b].used:
			t.Errorf("used field mismatch")
		case bm1[b].replace != bm2[b].replace:
			t.Errorf("replace field mismatch")
		case bm1[b].StorageFree != bm2[b].StorageFree:
			t.Errorf("StorageFree field mismatch")
		}
	}
}

func TestBrokerMapStorageDiff(t *testing.T) {
	bm1 := newMockBrokerMap()
	bm2 := newMockBrokerMap()

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

func TestBrokerListSort(t *testing.T) {
	bl := newMockBrokerMap().filteredList()

	// Test sort brokersByStorage.
	sort.Sort(brokersByStorage(bl))

	expected := []int{1004, 1003, 1002, 1001}

	for i, b := range bl {
		if b.id != expected[i] {
			t.Errorf("Expected broker %d, got %d", expected[i], b.id)
		}
	}
	// Test sort brokersByCount.
	sort.Sort(brokersByCount(bl))

	expected = []int{1003, 1004, 1001, 1002}

	for i, b := range bl {
		if b.id != expected[i] {
			t.Errorf("Expected broker %d, got %d", expected[i], b.id)
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
		0:    &broker{id: 0, replace: true},
		1001: &broker{id: 1001, locality: "a", used: 3, replace: false, StorageFree: 100.00},
		1002: &broker{id: 1002, locality: "b", used: 3, replace: false, StorageFree: 200.00},
		1003: &broker{id: 1003, locality: "c", used: 2, replace: false, StorageFree: 300.00},
		1004: &broker{id: 1004, locality: "a", used: 2, replace: false, StorageFree: 400.00},
	}
}
