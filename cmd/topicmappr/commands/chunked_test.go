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
	var chunks = getPartitionMapChunks(&finalMap, &inMap, kafkazk.BrokerList{
		&kafkazk.Broker{ID: 10000},
		&kafkazk.Broker{ID: 10001},
		&kafkazk.Broker{ID: 10002},
		&kafkazk.Broker{ID: 10003},
		&kafkazk.Broker{ID: 10004},
		&kafkazk.Broker{ID: 10005},
		&kafkazk.Broker{ID: 10006},
		&kafkazk.Broker{ID: 10007},
		&kafkazk.Broker{ID: 10008}}, 3)

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
	var chunks = getPartitionMapChunks(&finalMap, &inMap, kafkazk.BrokerList{
		&kafkazk.Broker{ID: 10000},
		&kafkazk.Broker{ID: 10001},
		&kafkazk.Broker{ID: 10002},
		&kafkazk.Broker{ID: 10003},
		&kafkazk.Broker{ID: 10004},
		&kafkazk.Broker{ID: 10005},
		&kafkazk.Broker{ID: 10006},
		&kafkazk.Broker{ID: 10007},
		&kafkazk.Broker{ID: 10008}}, 3)

	if len(chunks) != 0 {
		t.Errorf("Noop should result in no chunks generated.")
	}
}

func TestBasicChunkedDownscaleStepSizeOne(t *testing.T) {
	var inMap = readTestPartitionMap("nine_brokers.json")
	var finalMap = readTestPartitionMap("three_brokers.json")
	var chunks = getPartitionMapChunks(&finalMap, &inMap, kafkazk.BrokerList{
		&kafkazk.Broker{ID: 10000},
		&kafkazk.Broker{ID: 10001},
		&kafkazk.Broker{ID: 10002},
		&kafkazk.Broker{ID: 10003},
		&kafkazk.Broker{ID: 10004},
		&kafkazk.Broker{ID: 10005},
		&kafkazk.Broker{ID: 10006},
		&kafkazk.Broker{ID: 10007},
		&kafkazk.Broker{ID: 10008}}, 1)

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
	var chunks = getPartitionMapChunks(&finalMap, &inMap, kafkazk.BrokerList{
		&kafkazk.Broker{ID: 10001},
		&kafkazk.Broker{ID: 10000},
		&kafkazk.Broker{ID: 10006},
		&kafkazk.Broker{ID: 10005},
		&kafkazk.Broker{ID: 10002},
		&kafkazk.Broker{ID: 10007},
		&kafkazk.Broker{ID: 10004},
		&kafkazk.Broker{ID: 10003},
		&kafkazk.Broker{ID: 10008}}, 1)

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
	var chunks = getPartitionMapChunks(&finalMap, &inMap, kafkazk.BrokerList{
		&kafkazk.Broker{ID: 10000},
		&kafkazk.Broker{ID: 10001},
		&kafkazk.Broker{ID: 10002},
		&kafkazk.Broker{ID: 10003},
		&kafkazk.Broker{ID: 10004},
		&kafkazk.Broker{ID: 10005},
		&kafkazk.Broker{ID: 10006},
		&kafkazk.Broker{ID: 10007},
		&kafkazk.Broker{ID: 10008}}, 4)

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
	var chunks = getPartitionMapChunks(&finalMap, &inMap, kafkazk.BrokerList{
		&kafkazk.Broker{ID: 10000},
		&kafkazk.Broker{ID: 10001},
		&kafkazk.Broker{ID: 10002},
		&kafkazk.Broker{ID: 10003},
		&kafkazk.Broker{ID: 10004},
		&kafkazk.Broker{ID: 10005},
		&kafkazk.Broker{ID: 10006},
		&kafkazk.Broker{ID: 10007},
		&kafkazk.Broker{ID: 10008}}, 12)

	validateFinalChunk(t, chunks, finalMap)
	validateMapDoesNotContainBrokers(t, chunks[0], []int{10003, 10004, 10005, 10006, 10007, 10008})
	if len(chunks) != 1 {
		t.Errorf("Chunk size is bigger than available brokers, expected to return only the final map.")
	}
}

func validateFinalChunk(t *testing.T, chunks []*kafkazk.PartitionMap, finalMap kafkazk.PartitionMap) {
	if equals, _ := chunks[len(chunks)-1].Equal(&finalMap); !equals {
		t.Errorf("Final chunk should be equal to the desired map.")
	}
}

func validateMapDoesNotContainBrokers(t *testing.T, m *kafkazk.PartitionMap, brokers []int) {
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

func readTestPartitionMap(filename string) kafkazk.PartitionMap {
	file, _ := ioutil.ReadFile(fmt.Sprintf("../test_resources/%s", filename))
	data := kafkazk.PartitionMap{}
	_ = json.Unmarshal(file, &data)
	return data
}
