package commands

import (
	"encoding/json"
	"fmt"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"io/ioutil"
	"testing"
)

func TestBasicChunkedDownscale(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = chunked(&finalMap, &inMap, []int{10000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008}, 3)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10003, 10004, 10005})
	validateMapDoesNotContainBrokers(t, chunks[1], []int{10003, 10004, 10005, 10006, 10007, 10008})
}

func TestIgnoresNoop(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("nine_brokers.json")
	var chunks = chunked(&finalMap, &inMap, []int{10000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008}, 3)

	if len(chunks) != 0 {
		t.Errorf("Noop should result in no chunks generated.")
	}
}

func TestBasicChunkedDownscaleStepSizeOne(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = chunked(&finalMap, &inMap, []int{10000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008}, 1)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10003})
	validateMapDoesNotContainBrokers(t, chunks[1], []int{10003, 10004})
	validateMapDoesNotContainBrokers(t, chunks[2], []int{10003, 10004, 10005})
	validateMapDoesNotContainBrokers(t, chunks[3], []int{10003, 10004, 10005, 10006})
	validateMapDoesNotContainBrokers(t, chunks[4], []int{10003, 10004, 10005, 10006, 10007})
	validateMapDoesNotContainBrokers(t, chunks[5], []int{10003, 10004, 10005, 10006, 10007, 10008})
}

func TestUnevenBrokersForStepSize(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = chunked(&finalMap, &inMap, []int{10000, 10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008}, 4)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10003})
	validateMapDoesNotContainBrokers(t, chunks[1], []int{10003, 10004, 10005, 10006, 10007})
	validateMapDoesNotContainBrokers(t, chunks[2], []int{10003, 10004, 10005, 10006, 10007, 10008})
}

func validateFinalChunk(t *testing.T, chunks []*kafkazk.PartitionMap, finalMap kafkazk.PartitionMap) {
	if equals, _ := chunks[len(chunks)-1].Equal(&finalMap); !equals {
		t.Errorf("Final chunk should be equal to the desired map.")
	}
}

func validateMapDoesNotContainBrokers(t *testing.T, m *kafkazk.PartitionMap, brokers []int) {
	for _, p := range m.Partitions {
		for _, r := range p.Replicas {
			for b := range brokers {
				if r == b {
					t.Errorf("Failed to remove broker %d", r)
				}
			}
		}
	}
}

func readTestPartitionMap(filename string) kafkazk.PartitionMap {
	file, _ := ioutil.ReadFile(fmt.Sprintf("../test_resources/%s", filename))
	data := kafkazk.PartitionMap{}
	_ = json.Unmarshal(file, &data)
	return data
}
