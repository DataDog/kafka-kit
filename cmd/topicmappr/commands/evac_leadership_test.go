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
			Partition: 3,
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

	pMapOut := EvacLeadership(pMapIn, []int{problemBrokerId}, []string{topic})

	for _, partition := range pMapOut.Partitions {
		if partition.Replicas[0] == problemBrokerId {
			t.Errorf("Expected Broker ID 10001 to be evacuated from leadership")
		}
	}
}

func TestEvacTwoProblemBrokers(t *testing.T) {
	problemBrokers := []int{10001, 10002}

	pMapOut := EvacLeadership(pMapIn, problemBrokers, []string{topic})

	for _, partition := range pMapOut.Partitions {
		if partition.Replicas[0] == problemBrokers[0] || partition.Replicas[0] == problemBrokers[1] {
			t.Errorf("Expected Broker ID 10001 and 10002 to be evacuated from leadership")
		}
	}
}

func TestNoMatchingTopicToEvac(t *testing.T) {
	pMapOut := EvacLeadership(pMapIn, []int{10001}, []string{"some other topic"})

	for i, partition := range pMapOut.Partitions {
		for j, broker := range partition.Replicas {
			if broker != pMapIn.Partitions[i].Replicas[j] {
				t.Errorf("Expected no changes in leadership because no matching topic was passed in.")
			}
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
