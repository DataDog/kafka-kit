package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

func TestMarkStaleTags(t *testing.T) {
	// GIVEN
	bt := TagSet{"foo": "bar"}
	broker := KafkaObject{Type: "broker", ID: "1002"}

	tt := TagSet{"bing": "baz"}
	topic := KafkaObject{Type: "topic", ID: "test_topic"}

	// This broker is not in our zk stub
	nbt := TagSet{"stale": "tag"}
	noBroker := KafkaObject{Type: "broker", ID: "34"}

	zk := kafkazk.NewZooKeeperStub()
	th := testTagHandler()

	s := testServer()
	s.Tags = th
	s.ZK = zk

	// WHEN
	th.Store.SetTags(topic, tt)
	th.Store.SetTags(broker, bt)
	th.Store.SetTags(noBroker, nbt)
	s.MarkForDeletion(context.Background(), time.Now)

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
	//GIVEN
	markTime := time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)
	sweepTime := markTime.Add(15 * time.Minute)

	bt := TagSet{"foo": "bar", TagMarkTimeKey: fmt.Sprint(markTime.Unix())}
	broker := KafkaObject{Type: "broker", ID: "not found"}

	zk := kafkazk.NewZooKeeperStub()
	th := testTagHandler()

	s := testServer()
	s.Tags = th
	s.ZK = zk

	//WHEN
	th.Store.SetTags(broker, bt)
	s.DeleteStaleTags(context.Background(), func() time.Time { return sweepTime }, Config{TagAllowedStalenessMinutes: 10})

	//THEN
	btags, _ := th.Store.GetTags(broker)
	if len(btags) > 0 {
		t.Errorf("Expected marked tags for broker %s to be deleted.", broker)
	}
}

func TestUnmarkedTagsAreSafe(t *testing.T) {
	//GIVEN
	markTime := time.Date(2020, 1, 2, 3, 0, 0, 0, time.UTC)
	sweepTime := markTime.Add(15 * time.Minute)

	bt := TagSet{"foo": "bar"}
	broker := KafkaObject{Type: "broker", ID: "not found"}

	zk := kafkazk.NewZooKeeperStub()
	th := testTagHandler()

	s := testServer()
	s.Tags = th
	s.ZK = zk

	//WHEN
	th.Store.SetTags(broker, bt)
	s.DeleteStaleTags(context.Background(), func() time.Time { return sweepTime }, Config{TagAllowedStalenessMinutes: 10})

	//THEN
	btags, _ := th.Store.GetTags(broker)
	if len(btags) != 1 {
		t.Errorf("Expected marked tags for broker %s to be safe.", broker)
	}
}

func TestKafkaObjectComesBack(t *testing.T) {
	// GIVEN
	bt := TagSet{"foo": "bar", TagMarkTimeKey: "12345"} // pretend this broker was marked for tag deletion previously
	broker := KafkaObject{Type: "broker", ID: "1002"}   // but this broker now exists in our stub, so the marker will be removed.

	tt := TagSet{"bing": "baz", TagMarkTimeKey: "12345"} // same for a topic.
	topic := KafkaObject{Type: "topic", ID: "test_topic"}

	zk := kafkazk.NewZooKeeperStub()
	th := testTagHandler()

	s := testServer()
	s.Tags = th
	s.ZK = zk

	// WHEN
	th.Store.SetTags(broker, bt)
	th.Store.SetTags(topic, tt)
	s.MarkForDeletion(context.Background(), time.Now)

	// THEN
	btags, _ := th.Store.GetTags(broker)
	if _, exists := btags[TagMarkTimeKey]; exists {
		t.Errorf("Expected mark for broker %s to be removed.", broker)
	}

	ttags, _ := th.Store.GetTags(topic)
	if _, exists := ttags[TagMarkTimeKey]; exists {
		t.Errorf("Expected mark for topic %s to be removed.", topic)
	}
}
