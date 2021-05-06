package server

import (
	"fmt"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"testing"
	"time"
)

func TestMarkStaleTags(t *testing.T) {
	// GIVEN
	bt := TagSet{"foo": "bar"}
	broker := KafkaObject{Type: "broker", ID: "1002"}

	tt := TagSet{"bing": "baz"}
	topic := KafkaObject{Type: "topic", ID: "test_topic"}

	// This broker is not in our zk stub
	nbt := TagSet{"stale": "tag"}
	noBroker := KafkaObject{Type: "broker", ID: "not found"}

	zk := kafkazk.NewZooKeeperStub()
	th := testTagHandler()
	s := Server{Tags: th, ZK: zk}

	// WHEN
	th.Store.SetTags(topic, tt)
	th.Store.SetTags(broker, bt)
	th.Store.SetTags(noBroker, nbt)
	s.MarkForDeletion(time.Now)

	// THEN
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

func TestDeleteStaleTags(t *testing.T) {
	markTime := time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)
	sweepTime := markTime.Add(15 * time.Minute)

	bt := TagSet{"foo": "bar", TagMarkTimeKey: fmt.Sprint(markTime.Unix())}
	broker := KafkaObject{Type: "broker", ID: "not found"}

	th := testTagHandler()
	zk := kafkazk.NewZooKeeperStub()
	s := Server{Tags: th, ZK: zk}

	th.Store.SetTags(broker, bt)

	s.DeleteStaleTags(func() time.Time { return sweepTime }, Config{TagAllowedStalenessMinutes: 10})

	btags, _ := th.Store.GetTags(broker)
	if len(btags) > 0 {
		t.Errorf("Expected marked tags for broker %s to be deleted.", broker)
	}
}