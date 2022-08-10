package commands

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/DataDog/kafka-kit/v4/mapper"
)

func TestBasicChunkedDownscale(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = getPartitionMapChunks(&finalMap, &inMap, mapper.BrokerList{
		&mapper.Broker{ID: mapper.StubBrokerID},
		&mapper.Broker{ID: 10000},
		&mapper.Broker{ID: 10001},
		&mapper.Broker{ID: 10002},
		&mapper.Broker{ID: 10003},
		&mapper.Broker{ID: 10004},
		&mapper.Broker{ID: 10005},
		&mapper.Broker{ID: 10006},
		&mapper.Broker{ID: 10007},
		&mapper.Broker{ID: 10008}}, 3)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10008, 10007, 10006})
	validateMapDoesNotContainBrokers(t, chunks[1], []int{10003, 10004, 10005, 10006, 10007, 10008})
	if len(chunks) != 2 {
		t.Errorf("Removing 6 brokers in 2 chunks of 3, expected to return 2 map chunks.")
	}
}

func TestIgnoresNoop(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("nine_brokers.json")
	var chunks = getPartitionMapChunks(&finalMap, &inMap, mapper.BrokerList{
		&mapper.Broker{ID: mapper.StubBrokerID},
		&mapper.Broker{ID: 10000},
		&mapper.Broker{ID: 10001},
		&mapper.Broker{ID: 10002},
		&mapper.Broker{ID: 10003},
		&mapper.Broker{ID: 10004},
		&mapper.Broker{ID: 10005},
		&mapper.Broker{ID: 10006},
		&mapper.Broker{ID: 10007},
		&mapper.Broker{ID: 10008}}, 3)

	if len(chunks) != 0 {
		t.Errorf("Noop should result in no chunks generated.")
	}
}

func TestBasicChunkedDownscaleStepSizeOne(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = getPartitionMapChunks(&finalMap, &inMap, mapper.BrokerList{
		&mapper.Broker{ID: mapper.StubBrokerID},
		&mapper.Broker{ID: 10000},
		&mapper.Broker{ID: 10001},
		&mapper.Broker{ID: 10002},
		&mapper.Broker{ID: 10003},
		&mapper.Broker{ID: 10004},
		&mapper.Broker{ID: 10005},
		&mapper.Broker{ID: 10006},
		&mapper.Broker{ID: 10007},
		&mapper.Broker{ID: 10008}}, 1)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10008})
	validateMapDoesNotContainBrokers(t, chunks[1], []int{10008, 10007})
	validateMapDoesNotContainBrokers(t, chunks[2], []int{10008, 10007, 10006})
	validateMapDoesNotContainBrokers(t, chunks[3], []int{10008, 10007, 10006, 10005})
	validateMapDoesNotContainBrokers(t, chunks[4], []int{10008, 10007, 10006, 10005, 10004})
	validateMapDoesNotContainBrokers(t, chunks[5], []int{10008, 10007, 10006, 10005, 10004, 10003})
	if len(chunks) != 6 {
		t.Errorf("Removing 6 brokers in 6 chunks of 1, expected to return 1 map chunk per broker removed.")
	}
}

func TestChunksGeneratedWithConsistentOrdering(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = getPartitionMapChunks(&finalMap, &inMap, mapper.BrokerList{
		&mapper.Broker{ID: mapper.StubBrokerID},
		&mapper.Broker{ID: 10001},
		&mapper.Broker{ID: 10000},
		&mapper.Broker{ID: 10006},
		&mapper.Broker{ID: 10005},
		&mapper.Broker{ID: 10002},
		&mapper.Broker{ID: 10007},
		&mapper.Broker{ID: 10004},
		&mapper.Broker{ID: 10003},
		&mapper.Broker{ID: 10008}}, 1)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10008})
	validateMapDoesNotContainBrokers(t, chunks[1], []int{10008, 10007})
	validateMapDoesNotContainBrokers(t, chunks[2], []int{10008, 10007, 10006})
	validateMapDoesNotContainBrokers(t, chunks[3], []int{10008, 10007, 10006, 10005})
	validateMapDoesNotContainBrokers(t, chunks[4], []int{10008, 10007, 10006, 10005, 10004})
	validateMapDoesNotContainBrokers(t, chunks[5], []int{10008, 10007, 10006, 10005, 10004, 10003})
	if len(chunks) != 6 {
		t.Errorf("Removing 6 brokers in 6 chunks of 1, expected to return 1 map chunk per broker removed.")
	}
}

func TestUnevenBrokersForStepSize(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = getPartitionMapChunks(&finalMap, &inMap, mapper.BrokerList{
		&mapper.Broker{ID: mapper.StubBrokerID},
		&mapper.Broker{ID: 10000},
		&mapper.Broker{ID: 10001},
		&mapper.Broker{ID: 10002},
		&mapper.Broker{ID: 10003},
		&mapper.Broker{ID: 10004},
		&mapper.Broker{ID: 10005},
		&mapper.Broker{ID: 10006},
		&mapper.Broker{ID: 10007},
		&mapper.Broker{ID: 10008}}, 4)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10005, 10006, 10007, 10008})
	validateMapDoesNotContainBrokers(t, chunks[1], []int{10003, 10004, 10005, 10006, 10007, 10008})
	if len(chunks) != 2 {
		t.Errorf("Removing 6 brokers in 2 chunks of 4, expected to return only two map chunks.")
	}
}

func TestChunkSizeBiggerThanAvailableBrokers(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = getPartitionMapChunks(&finalMap, &inMap, mapper.BrokerList{
		&mapper.Broker{ID: mapper.StubBrokerID},
		&mapper.Broker{ID: 10000},
		&mapper.Broker{ID: 10001},
		&mapper.Broker{ID: 10002},
		&mapper.Broker{ID: 10003},
		&mapper.Broker{ID: 10004},
		&mapper.Broker{ID: 10005},
		&mapper.Broker{ID: 10006},
		&mapper.Broker{ID: 10007},
		&mapper.Broker{ID: 10008}}, 12)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10003, 10004, 10005, 10006, 10007, 10008})
	if len(chunks) != 1 {
		t.Errorf("Chunk size is bigger than available brokers, expected to return only the final map.")
	}
}

func validateFinalChunk(t *testing.T, chunks []*mapper.PartitionMap, finalMap mapper.PartitionMap) {
	if equals, _ := chunks[len(chunks)-1].Equal(&finalMap); !equals {
		t.Errorf("Final chunk should be equal to the desired map.")
	}
}

func validateMapDoesNotContainBrokers(t *testing.T, m *mapper.PartitionMap, brokers []int) {
	for _, p := range m.Partitions {
		for _, r := range p.Replicas {
			for _, b := range brokers {
				if r == b {
					t.Errorf("Failed to remove broker %d", r)
				}
			}
		}
	}
}

func readTestPartitionMap(filename string) mapper.PartitionMap {
	file, _ := ioutil.ReadFile(fmt.Sprintf("../test_resources/%s", filename))
	data := mapper.PartitionMap{}
	_ = json.Unmarshal(file, &data)
	return data
}
