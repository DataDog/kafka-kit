package main

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
)

// zkmock implements a mock zkhandler.
type zkmock struct{}

func (z *zkmock) getReassignments() reassignments {
	r := reassignments{
		"test_topic": map[int][]int{
			2: []int{1003, 1004},
			3: []int{1004, 1003},
		},
	}
	return r
}

func (z *zkmock) getTopics(ts []*regexp.Regexp) ([]string, error) {
	t := []string{"test_topic", "test_topic2"}

	match := map[string]bool{}
	// Get all topics that match all
	// provided topic regexps.
	for _, topicRe := range ts {
		for _, topic := range t {
			if topicRe.MatchString(topic) {
				match[topic] = true
			}
		}
	}

	// Add matches to a slice.
	matched := []string{}
	for topic := range match {
		matched = append(matched, topic)
	}

	return matched, nil
}

func (z *zkmock) getAllBrokerMeta() (brokerMetaMap, error) {
	b := brokerMetaMap{
		1001: &BrokerMeta{Rack: "a"},
		1002: &BrokerMeta{Rack: "b"},
		1003: &BrokerMeta{Rack: "c"},
		1004: &BrokerMeta{Rack: "a"},
	}

	return b, nil
}

func (z *zkmock) getPartitionMap(t string) (*partitionMap, error) {
	p := &partitionMap{
		Version: 1,
		Partitions: partitionList{
			Partition{Topic: t, Partition: 0, Replicas: []int{1001, 1002}},
			Partition{Topic: t, Partition: 1, Replicas: []int{1002, 1001}},
			Partition{Topic: t, Partition: 2, Replicas: []int{1003, 1004, 1001}},
			Partition{Topic: t, Partition: 3, Replicas: []int{1004, 1003, 1002}},
		},
	}

	return p, nil
}

func getMapString(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004,1001]},
    {"topic":"%s","partition":3,"replicas":[1004,1003,1002]}]}`, n, n, n, n)
}

// func TestCopy(t *testing.T) {}

// func TestRebuild(t *testing.T) {}

func TestPartitionMapFromString(t *testing.T) {
	pm, _ := partitionMapFromString(getMapString("test_topic"))
	zk := &zkmock{}
	pmap, _ := zk.getPartitionMap("test_topic")

	// We expect equality here.
	if same, _ := pm.equal(pmap); !same {
		t.Errorf("Unexpected inequality")
	}

	// After modifying the partitions list,
	// we expect inequality.
	pm.Partitions = pm.Partitions[:2]
	if same, _ := pm.equal(pmap); same {
		t.Errorf("Unexpected equality")
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
		pmap, _ := partitionMapFromString(getMapString(t))
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
  pm, _ := partitionMapFromString(getMapString("test_topic"))

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
// func TestStrip(t *testing.T) {}
// func TestUseStats(t *testing.T) {}
