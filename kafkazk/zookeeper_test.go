package kafkazk

import (
	"fmt"
	"regexp"
	"testing"

	"github.com/DataDog/kafka-kit/v3/mapper"
)

func TestPartitionMapFromZK(t *testing.T) {
	zk := NewZooKeeperStub()

	r := []*regexp.Regexp{}
	r = append(r, regexp.MustCompile("/^null$/"))
	pm, err := PartitionMapFromZK(r, zk)

	// This should fail because we're passing a regex that the stub call to
	// GetTopics() from PartitionMapFromZK doesn't have any matches.
	if pm != nil || err.Error() != "No topics found matching: [/^null$/]" {
		t.Error("Expected topic lookup failure")
	}

	r = r[:0]
	r = append(r, regexp.MustCompile("test"))

	// This is going to match both "test_topic" and "test_topic2" from the stub.
	pm, _ = PartitionMapFromZK(r, zk)

	// Build a merged map of these for equality testing.
	pm2 := mapper.NewPartitionMap()
	for _, t := range []string{"test_topic", "test_topic2"} {
		pmap, _ := mapper.PartitionMapFromString(testGetMapString(t))
		pm2.Partitions = append(pm2.Partitions, pmap.Partitions...)
	}

	// Compare.
	if same, err := pm.Equal(pm2); !same {
		t.Errorf("Unexpected inequality: %s", err)
	}

}

func testGetMapString(n string) string {
	return fmt.Sprintf(`{"version":1,"partitions":[
    {"topic":"%s","partition":0,"replicas":[1001,1002]},
    {"topic":"%s","partition":1,"replicas":[1002,1001]},
    {"topic":"%s","partition":2,"replicas":[1003,1004,1001]},
    {"topic":"%s","partition":3,"replicas":[1004,1003,1002]}]}`, n, n, n, n)
}
