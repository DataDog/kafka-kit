package server

import (
	"testing"
)

func TestSetTags(t *testing.T) {
	store, _ := NewZKTagStorage(ZKTagStorageConfig{Prefix: testConfig.Prefix})

	ts := TagSet{
		"key":  "value",
		"key2": "value2",
	}

	o := KafkaObject{
		Kind: "broker",
		ID:   "1002",
	}

	_ = store.SetTags(o, ts)
}
