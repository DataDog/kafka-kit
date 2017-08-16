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

func init() {
	zookeeper.Register()
}

type BrokerMeta struct {
	Rack string `json:"rack"`
}

type brokerMetaMap map[int]*BrokerMeta

type zkConfig struct {
	ConnectString string
	Prefix        string
}

func getAllBrokerMeta(zc *zkConfig) (brokerMetaMap, error) {
	kv, err := libkv.NewStore(
		store.ZK,
		[]string{zc.ConnectString},
		&store.Config{
			ConnectionTimeout: 10 * time.Second,
		},
	)
	if err != nil {
		return nil, err
	}

	var path string
	if zc.Prefix != "" {
		path = fmt.Sprintf("%s/brokers/ids", zc.Prefix)
	} else {
		path = "brokers/ids"
	}

	entries, err := kv.List(path)
	if err != nil {
		return nil, err
	}

	bmm := brokerMetaMap{}

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
