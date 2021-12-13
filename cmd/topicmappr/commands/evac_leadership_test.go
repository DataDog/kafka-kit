package commands

import (
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"testing"
)

var topic = "testTopic"
var pMapIn = kafkazk.PartitionMap{
	Version: 1,
	Partitions: kafkazk.PartitionList{
		kafkazk.Partition{
			Topic:     topic,
			Partition: 0,
			Replicas: []int{
				10001,
				10002,
				10003},
		},
		kafkazk.Partition{
			Topic:     topic,
			Partition: 1,
			Replicas: []int{
				10002,
				10001,
				10003,
			},
		},
		kafkazk.Partition{
			Topic:     topic,
			Partition: 0,
			Replicas: []int{
				10003,
				10002,
				10001,
			},
		},
	},
}

func TestRemoveProblemBroker(t *testing.T) {
	problemBrokerId := 10001
	problemBrokers := kafkazk.BrokerMap{}
	problemBrokers[10001] = &kafkazk.Broker{}

	pMapOut := EvacLeadership(pMapIn, problemBrokers)

	for _, partition := range pMapOut.Partitions {
		if partition.Replicas[0] == problemBrokerId {
			t.Errorf("Expected Broker ID 10001 to be evacuated from leadership")
		}
	}
}

func TestEvacTwoProblemBrokers(t *testing.T) {
	problemBrokers := kafkazk.BrokerMap{}
	problemBrokers[10001] = &kafkazk.Broker{}
	problemBrokers[10002] = &kafkazk.Broker{}

	pMapOut := EvacLeadership(pMapIn, problemBrokers)

	for _, partition := range pMapOut.Partitions {
		if partition.Replicas[0] == 10001 || partition.Replicas[0] == 10002 {
			t.Errorf("Expected Broker ID 10001 to be evacuated from leadership")
		}
	}
}

// TODO: This test currently fails because the error case calls os.Exit(1). Better way to test, or better error handling.
//func TestEvacAllBrokersForPartitionFails(t *testing.T) {
//	problemBrokers := kafkazk.BrokerMap{}
//	problemBrokers[10001] = &kafkazk.Broker{}
//	problemBrokers[10002] = &kafkazk.Broker{}
//	problemBrokers[10003] = &kafkazk.Broker{}
//
//	EvacLeadership(pMapIn, problemBrokers)
//
//	t.Errorf("EvacLeadership should have errored out at this point.")
//}
