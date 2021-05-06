package server

import (
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"testing"
)

func TestMarkStaleTags(t *testing.T) {
	bt := TagSet{"foo": "bar"}
	broker := KafkaObject{Type: "broker", ID: "1002"}

	tt := TagSet{"bing": "baz"}
	topic := KafkaObject{Type: "topic", ID: "test_topic"}

	// This broker is not in our zk stub
	nbt := TagSet{"stale": "tag"}
	noBroker := KafkaObject{Type: "broker", ID: "not found"}

	th := testTagHandler()
	th.Store.SetTags(topic, tt)
	th.Store.SetTags(broker, bt)
	th.Store.SetTags(noBroker, nbt)

	zk := kafkazk.NewZooKeeperStub()

	s := Server{Tags: th, ZK: zk}

	s.MarkForDeletion()

	nbtags, _ := th.Store.GetTags(noBroker)
	if _, exists := nbtags[TagMarkTimeKey]; !exists {
		t.Errorf("Expected tags for broker %s to be marked for cleanup.", noBroker)
	}

	ttags, _ := th.Store.GetTags(topic)
	if _, exists := ttags[TagMarkTimeKey]; exists {
		t.Errorf("Expected tags for topic %s to not be marked for cleanup.", topic)
	}

	btags, _ := th.Store.GetTags(broker)
	if _, exists := btags[TagMarkTimeKey]; exists {
		t.Errorf("Expected tags for broker %s to not be marked for cleanup.", broker)
	}
}
