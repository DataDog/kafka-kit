package main

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

func TestEqual(t *testing.T) {
	pm, _ := partitionMapFromString(testGetMapString("test_topic"))
	pm2, _ := partitionMapFromString(testGetMapString("test_topic"))

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

func TestCopy(t *testing.T) {
	pm, _ := partitionMapFromString(testGetMapString("test_topic"))
	pm2 := pm.copy()

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
	pm, _ := partitionMapFromString(testGetMapString("test_topic"))
	zk := &zkmock{}
	pm2, _ := zk.getPartitionMap("test_topic")

	// We expect equality here.
	if same, _ := pm.equal(pm2); !same {
		t.Errorf("Unexpected inequality")
	}
}

func TestPartitionMapFromZK(t *testing.T) {
	zk := &zkmock{}

	r := []*regexp.Regexp{}
	r = append(r, regexp.MustCompile("/^null$/"))
	pm, err := partitionMapFromZK(r, zk)

	// This should fail because we're passing
	// a regex that the mock call to getTopics()
	// from partitionMapFromZK doesn't have
	// any matches.
	if pm != nil || err.Error() != "No topics found matching: " {
		t.Errorf("Expected topic lookup failure")
	}

	r = r[:0]
	r = append(r, regexp.MustCompile("test"))

	// This is going to match both "test_topic"
	// and "test_topic2" from the mock.
	pm, _ = partitionMapFromZK(r, zk)

	// Build a merged map of these for
	// equality testing.
	pm2 := newPartitionMap()
	for _, t := range []string{"test_topic", "test_topic2"} {
		pmap, _ := partitionMapFromString(testGetMapString(t))
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
	pm, _ := partitionMapFromString(testGetMapString("test_topic"))

	pm.setReplication(3)
	// All partitions should now have 3 replicas.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 3 {
			t.Errorf("Expected 3 replicas, got %d", len(r.Replicas))
		}
	}

	pm.setReplication(2)
	// All partitions should now have 3 replicas.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 2 {
			t.Errorf("Expected 2 replicas, got %d", len(r.Replicas))
		}
	}

	pm.setReplication(0)
	// Setting to 0 is a no-op.
	for _, r := range pm.Partitions {
		if len(r.Replicas) != 2 {
			t.Errorf("Expected 2 replicas, got %d", len(r.Replicas))
		}
	}
}

func TestStrip(t *testing.T) {
	pm, _ := partitionMapFromString(testGetMapString("test_topic"))

	spm := pm.strip()

	for _, p := range spm.Partitions {
		for _, b := range p.Replicas {
			if b != 0 {
				t.Errorf("Unexpected non-stub broker ID %d", b)
			}
		}
	}
}

func TestUseStats(t *testing.T) {
	pm, _ := partitionMapFromString(testGetMapString("test_topic"))

	s := pm.useStats()

	expected := map[int][2]int{
		1001: [2]int{1, 2},
		1002: [2]int{1, 2},
		1003: [2]int{1, 1},
		1004: [2]int{1, 1},
	}

	for b, bs := range s {
		if bs.leader != expected[b][0] {
			t.Errorf("Expected leader count %d for %d, got %d",
				expected[b][0], b, bs.leader)
		}

		if bs.follower != expected[b][1] {
			t.Errorf("Expected follower count %d for %d, got %d",
				expected[b][1], b, bs.follower)
		}
	}
}

func TestRebuild(t *testing.T) {
	zk := &zkmock{}
	bm, _ := zk.getAllBrokerMeta()
	pm, _ := partitionMapFromString(testGetMapString("test_topic"))
	forceRebuild := false

	brokers := brokerMapFromTopicMap(pm, bm, forceRebuild)
	out, errs := pm.rebuild(brokers)
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
	brokers[1004].replace = true
	out, errs = pm.rebuild(brokers)
	if errs != nil {
		t.Errorf("Unexpected error(s): %s", errs)
	}

	// Expected map after a replacement rebuild.
	expected, _ := partitionMapFromString(testGetMapString("test_topic"))
	expected.Partitions[2].Replicas = []int{1003, 1002, 1001}
	expected.Partitions[3].Replicas = []int{1001, 1003, 1002}

	if same, _ := out.equal(expected); !same {
		t.Error("Unexpected inequality after broker replacement")
	}

	// Test a rebuild with a change in
	// replication factor.
	pm.setReplication(2)
	expected.setReplication(2)

	out, _ = pm.rebuild(brokers)

	if same, _ := out.equal(expected); !same {
		t.Error("Unexpected inequality after replication factor change -> rebuild")
	}

	// Test a force rebuild.
	pmStripped := pm.strip()
	out, _ = pmStripped.rebuild(brokers)

	same, _ := pm.equal(out)
	if same {
		t.Error("Unexpected inequality after force rebuild")
	}
}
