package kafkazk

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
)

func testGetMapString(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004,1001]},
    {"topic":"%s","partition":3,"replicas":[1004,1003,1002]}]}`, n, n, n, n)
}

func testGetMapString2(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004]},
    {"topic":"%s","partition":3,"replicas":[1004,1003]},
		{"topic":"%s","partition":4,"replicas":[1004,1003]},
		{"topic":"%s","partition":5,"replicas":[1004,1003]},
		{"topic":"%s","partition":6,"replicas":[1004,1003]}]}`, n, n, n, n, n, n, n)
}

func testGetMapString3(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004,1001]},
    {"topic":"%s","partition":3,"replicas":[1004,1003,1002]},
		{"topic":"%s","partition":3,"replicas":[1004,1005]}]}`, n, n, n, n, n)
}

// func TestSize(t *testing.T) {} XXX Do.

func TestSortBySize(t *testing.T) {
	z := &Mock{}

	partitionMap, _ := z.GetPartitionMap("test_topic")
	partitionMetaMap, _ := z.GetAllPartitionMeta()

	s := partitionsBySize{
		pl: partitionMap.Partitions,
		pm: partitionMetaMap,
	}

	sort.Sort(partitionsBySize(s))

	expected := []int{3, 2, 1, 0}
	for i, p := range partitionMap.Partitions {
		if p.Partition != expected[i] {
			t.Errorf("Expected partition %d, got %d", expected[i], p.Partition)
		}
	}
}

func TestEqual(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pm2, _ := PartitionMapFromString(testGetMapString("test_topic"))

	if same, _ := pm.equal(pm2); !same {
		t.Error("Unexpected inequality")
	}

	// After modifying the partitions list,
	// we expect inequality.
	pm.Partitions = pm.Partitions[:2]
	if same, _ := pm.equal(pm2); same {
		t.Errorf("Unexpected equality")
	}
}

func TestPartitionMapCopy(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pm2 := pm.Copy()

	if same, _ := pm.equal(pm2); !same {
		t.Error("Unexpected inequality")
	}

	// After modifying the partitions list,
	// we expect inequality.
	pm.Partitions = pm.Partitions[:2]
	if same, _ := pm.equal(pm2); same {
		t.Errorf("Unexpected equality")
	}
}

func TestPartitionMapFromString(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	zk := &Mock{}
	pm2, _ := zk.GetPartitionMap("test_topic")

	// We expect equality here.
	if same, _ := pm.equal(pm2); !same {
		t.Errorf("Unexpected inequality")
	}
}

func TestPartitionMapFromZK(t *testing.T) {
	zk := &Mock{}

	r := []*regexp.Regexp{}
	r = append(r, regexp.MustCompile("/^null$/"))
	pm, err := PartitionMapFromZK(r, zk)

	// This should fail because we're passing
	// a regex that the mock call to GetTopics()
	// from PartitionMapFromZK doesn't have
	// any matches.
	if pm != nil || err.Error() != "No topics found matching: [/^null$/]" {
		t.Errorf("Expected topic lookup failure")
	}

	r = r[:0]
	r = append(r, regexp.MustCompile("test"))

	// This is going to match both "test_topic"
	// and "test_topic2" from the mock.
	pm, _ = PartitionMapFromZK(r, zk)

	// Build a merged map of these for
	// equality testing.
	pm2 := NewPartitionMap()
	for _, t := range []string{"test_topic", "test_topic2"} {
		pmap, _ := PartitionMapFromString(testGetMapString(t))
		pm2.Partitions = append(pm2.Partitions, pmap.Partitions...)
	}

	sort.Sort(pm.Partitions)
	sort.Sort(pm2.Partitions)

	// Compare.
	if same, err := pm.equal(pm2); !same {
		t.Errorf("Unexpected inequality: %s", err)
	}

}

func TestSetReplication(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))

	pm.SetReplication(3)
	// All partitions should now have 3 replicas.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 3 {
			t.Errorf("Expected 3 replicas, got %d", len(r.Replicas))
		}
	}

	pm.SetReplication(2)
	// All partitions should now have 3 replicas.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 2 {
			t.Errorf("Expected 2 replicas, got %d", len(r.Replicas))
		}
	}

	pm.SetReplication(0)
	// Setting to 0 is a no-op.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 2 {
			t.Errorf("Expected 2 replicas, got %d", len(r.Replicas))
		}
	}
}

func TestStrip(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))

	spm := pm.Strip()

	for _, p := range spm.Partitions {
		for _, b := range p.Replicas {
			if b != 0 {
				t.Errorf("Unexpected non-stub broker ID %d", b)
			}
		}
	}
}

func TestUseStats(t *testing.T) {
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))

	s := pm.UseStats()

	expected := map[int][2]int{
		1001: [2]int{1, 2},
		1002: [2]int{1, 2},
		1003: [2]int{1, 1},
		1004: [2]int{1, 1},
	}

	for _, b := range s {
		if b.Leader != expected[b.ID][0] {
			t.Errorf("Expected leader count %d for %d, got %d",
				expected[b.ID][0], b.ID, b.Leader)
		}

		if b.Follower != expected[b.ID][1] {
			t.Errorf("Expected follower count %d for %d, got %d",
				expected[b.ID][1], b.ID, b.Follower)
		}
	}
}

func TestRebuild(t *testing.T) {
	forceRebuild := true
	withMetrics := false

	zk := &Mock{}
	bm, _ := zk.GetAllBrokerMeta(withMetrics)
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pmm := NewPartitionMetaMap()
	brokers := BrokerMapFromTopicMap(pm, bm, forceRebuild)

	out, errs := pm.Rebuild(brokers, pmm, "distribution", "count")
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	// This rebuild should be a no-op since
	// all brokers already in the map were provided,
	// none marked as replace.
	if same, _ := pm.equal(out); !same {
		t.Error("Expected no-op, topic map changed")
	}

	// Mark 1004 for replacement.
	brokers[1004].Replace = true

	// Rebuild.
	out, errs = pm.Rebuild(brokers, pmm, "distribution", "count")
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	// Expected map after a replacement rebuild.
	expected, _ := PartitionMapFromString(testGetMapString("test_topic"))
	expected.Partitions[2].Replicas = []int{1003, 1002, 1001}
	expected.Partitions[3].Replicas = []int{1001, 1003, 1002}

	if same, err := out.equal(expected); !same {
		t.Errorf("Unexpected inequality after broker replacement: %s", err)
	}

	// Test a rebuild with a change in
	// replication factor.
	pm.SetReplication(2)
	expected.SetReplication(2)

	out, _ = pm.Rebuild(brokers, pmm, "distribution", "count")

	if same, err := out.equal(expected); !same {
		t.Errorf("Unexpected inequality after replication factor change -> rebuild: %s", err)
	}

	// Test a force rebuild.
	forceRebuild = true
	pm, _ = PartitionMapFromString(testGetMapString2("test_topic"))
	pmStripped := pm.Strip()
	brokers = BrokerMapFromTopicMap(pm, bm, forceRebuild)

	out, _ = pmStripped.Rebuild(brokers, pmm, "distribution", "count")
	fmt.Printf("%v\n", out)
	expected = pm.Copy()
	expected.Partitions[0].Replicas = []int{1001, 1003}
	expected.Partitions[1].Replicas = []int{1002, 1004}
	expected.Partitions[2].Replicas = []int{1003, 1001}
	expected.Partitions[3].Replicas = []int{1004, 1002}
	expected.Partitions[4].Replicas = []int{1001, 1002}
	expected.Partitions[5].Replicas = []int{1002, 1004}
	expected.Partitions[6].Replicas = []int{1003, 1001}

	same, err := out.equal(expected)
	if !same {
		t.Errorf("Unexpected inequality after force rebuild: %s", err)
	}
}

func TestRebuildByStorage(t *testing.T) {
	forceRebuild := true
	withMetrics := true

	zk := &Mock{}
	bm, _ := zk.GetAllBrokerMeta(withMetrics)
	pm, _ := PartitionMapFromString(testGetMapString("test_topic"))
	pmm, _ := zk.GetAllPartitionMeta()

	pm.SetReplication(2)
	pmStripped := pm.Strip()

	brokers := BrokerMapFromTopicMap(pm, bm, forceRebuild)
	_ = brokers.SubStorageAll(pm, pmm)

	out, errs := pmStripped.Rebuild(brokers, pmm, "distribution", "storage")
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	// TODO
	_ = out
	/*
		for _, b := range brokers {
			fmt.Printf("%d %f\n", b.ID, b.StorageFree)
		}*/
}
