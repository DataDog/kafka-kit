package main

import (
	"testing"
)

var (
  // Map as a json encoded string.
  mapString string = `{"version":1,"partitions":[{"topic":"test_topic","partition":0,"replicas":[1001,1002]},{"topic":"test_topic","partition":1,"replicas":[1002,1001]},{"topic":"test_topic","partition":2,"replicas":[1003,1004,1001]},{"topic":"test_topic","partition":3,"replicas":[1004,1003,1002]}]}`
  // Same map as a *partitionMap.
  pmap *partitionMap = &partitionMap{
    Version: 1,
    Partitions: partitionList{
      Partition{Topic: "test_topic",Partition:0,Replicas:[]int{1001,1002}},
      Partition{Topic: "test_topic",Partition:1,Replicas:[]int{1002,1001}},
      Partition{Topic: "test_topic",Partition:2,Replicas:[]int{1003,1004,1001}},
      Partition{Topic: "test_topic",Partition:3,Replicas:[]int{1004,1003,1002}},
    },
  }
)

// func TestRebuild(t *testing.T) {}

func TestPartitionMapFromString(t *testing.T) {
  pm, _ := partitionMapFromString(mapString)

  if !pm.equal(pmap)  {
    t.Errorf("Unexpected inequality")
  }

  pm.Partitions = pm.Partitions[:2]
  if pm.equal(pmap)  {
    t.Errorf("Unexpected equality")
  }
}

// func TestPartitionMapFromZK(t *testing.T) {}
// func TestSetReplication(t *testing.T) {}
// func TestCopy(t *testing.T) {}
// func TestStrip(t *testing.T) {}
// func TestWriteMap(t *testing.T) {}
// func TestUseStats(t *testing.T) {}
