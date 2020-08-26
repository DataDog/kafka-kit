package commands

import (
	"regexp"
	"sort"
	"testing"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

func TestRemoveTopics(t *testing.T) {
	zk := kafkazk.NewZooKeeperMock()
	pm1, _ := zk.GetPartitionMap("test")
	pm2, _ := zk.GetPartitionMap("test2")
	pm3, _ := zk.GetPartitionMap("test3")

	pm := mergePartitionMaps(pm1, pm2, pm3)

	// Remove all topics with a trailing digit.
	toRemove := []*regexp.Regexp{
		// Junk pattern.
		regexp.MustCompile(`asdf`),
		// Pattern we care about.
		regexp.MustCompile(`\d$`),
	}

	removed := removeTopics(pm, toRemove)

	// Check the removed output.
	if len(removed) != 2 {
		t.Fatalf("Expected removed length of 2 got %d", len(removed))
	}

	sort.Strings(removed)
	expectedRemove := []string{"test2", "test3"}
	for i, r := range removed {
		if r != expectedRemove[i] {
			t.Errorf("Expected removed topic '%s', got '%s'", expectedRemove[i], r)
		}
	}

	// Test remaining in PartitionMap.
	topics := pm.Topics()

	if len(topics) != 1 {
		t.Fatalf("Expected remaining topics length of 1, got %d", len(topics))
	}

	if topics[0] != "test" {
		t.Fatalf("Expected remaining topic 'test', got '%s'", topics[0])
	}
}

func mergePartitionMaps(pms ...*kafkazk.PartitionMap) *kafkazk.PartitionMap {
	pm := kafkazk.NewPartitionMap()

	for _, p := range pms {
		pm.Partitions = append(pm.Partitions, p.Partitions...)
	}

	return pm
}
