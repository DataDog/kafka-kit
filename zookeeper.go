package main

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
)

var (
	zk store.Store
)

func init() {
	zookeeper.Register()
}

type BrokerMeta struct {
	Rack string `json:"rack"`
}

// brokerMetaMap is a map of broker IDs
// to BrokerMeta metadata fetched from
// ZooKeeper. Currently, just the rack
// field is retrieved.
type brokerMetaMap map[int]*BrokerMeta

// topicState is used for unmarshing
// ZooKeeper json data from a topic:
// e.g. `get /brokers/topics/some-topic`.
type topicState struct {
	Partitions map[string][]int `json:"partitions"`
}

type zkConfig struct {
	ConnectString string
	Prefix        string
}

func initZK(zc *zkConfig) error {
	var err error
	zk, err = libkv.NewStore(
		store.ZK,
		[]string{zc.ConnectString},
		&store.Config{
			ConnectionTimeout: 10 * time.Second,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func getAllBrokerMeta(zc *zkConfig) (brokerMetaMap, error) {
	var path string
	if zc.Prefix != "" {
		path = fmt.Sprintf("%s/brokers/ids", zc.Prefix)
	} else {
		path = "brokers/ids"
	}

	// Get all brokers.
	entries, err := zk.List(path)
	if err != nil {
		return nil, err
	}

	bmm := brokerMetaMap{}

	// Map each broker.
	for _, pair := range entries {
		bm := &BrokerMeta{}
		// In case we encounter non-ints
		// (broker IDs) for whatever reason,
		// just continue.
		bid, err := strconv.Atoi(pair.Key)
		if err != nil {
			continue
		}

		// Same with unmarshalling json meta.
		err = json.Unmarshal(pair.Value, bm)
		if err != nil {
			continue
		}

		bmm[bid] = bm
	}

	return bmm, nil
}

func partitionMapFromZk(zc *zkConfig, t string) (*partitionMap, error) {
	var path string
	if zc.Prefix != "" {
		path = fmt.Sprintf("%s/brokers/topics/%s", zc.Prefix, t)
	} else {
		path = fmt.Sprintf("brokers/topics/%s", t)
	}

	// Fetch topic data from ZK.
	ts := &topicState{}
	m, err := zk.Get(path)
	switch err {
	case store.ErrKeyNotFound:
		return nil, fmt.Errorf("Topic %s not found in ZooKeeper\n", t)
	case nil:
		break
	default:
		return nil, err
	}

	err = json.Unmarshal(m.Value, ts)
	if err != nil {
		return nil, err
	}

	// Map topicState to a
	// partitionMap.
	pm := newPartitionMap()
	pl := partitionList{}

	for partition, replicas := range ts.Partitions {
		i, _ := strconv.Atoi(partition)
		pl = append(pl, Partition{
			Topic:     t,
			Partition: i,
			Replicas:  replicas,
		})
	}
	pm.Partitions = pl

	return pm, nil
}
