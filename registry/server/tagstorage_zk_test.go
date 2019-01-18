package server

import (
	"testing"
)

func TestSetTags(t *testing.T) {
	store, _ := NewZKTagStorage(ZKTagStorageConfig{Prefix: testConfig.Prefix})

	ts := TagSet{
		"key": "value",
	}

	o := KafkaObject{
		Kind: "broker",
		ID:   "1002",
	}

	_ = store.SetTags(o, ts)
}
