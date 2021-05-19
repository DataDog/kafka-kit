// +build integration

package server

import (
	"fmt"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

var (
	zkaddr   = "zookeeper:2181"
	zkprefix = "registry_test"

	store *ZKTagStorage
)

func TestSetup(t *testing.T) {
	overrideZKAddr := os.Getenv("TEST_ZK_ADDR")
	if overrideZKAddr != "" {
		zkaddr = overrideZKAddr
	}

	// Init a kafkazk.Handler.
	zk, err := kafkazk.NewHandler(&kafkazk.Config{Connect: zkaddr})
	if err != nil {
		t.Fatalf("Error initializing ZooKeeper client: %s", err)
	}

	// Init a ZKTagStorage.
	store, err = NewZKTagStorage(ZKTagStorageConfig{Prefix: zkprefix})
	if err != nil {
		t.Error(err)
	}

	store.ZK = zk

	if err := store.Init(); err != nil {
		t.Fatal(err)
	}

	// Load reserved fields.
	store.LoadReservedFields(GetReservedFields())
}

func TestSetTags(t *testing.T) {
	testTagSets := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
	}

	testObjects := map[int]KafkaObject{
		0: KafkaObject{Type: "broker", ID: "1002"},
		1: KafkaObject{Type: "topic", ID: "test"},
	}

	expected := map[int]string{
		0: `{"key":"value","key2":"value2"}`,
		1: `{"key":"value"}`,
	}

	for k := range testTagSets {
		// Set tags.
		err := store.SetTags(testObjects[k], testTagSets[k])
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}

		// Fetch tags, compare value.
		tpath := fmt.Sprintf("/%s/%s/%s",
			zkprefix, testObjects[k].Type, testObjects[k].ID)

		result, _, err := store.ZK.Get(tpath)
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}

		if string(result) != expected[k] {
			t.Errorf("[test %d] Expected tag string value '%s', got '%s'",
				k, expected[k], result)
		}
	}
}

func TestTagSetFailures(t *testing.T) {
	// Test invalid KafkaObject Type.
	o := KafkaObject{Type: "test", ID: "1002"}

	err := store.SetTags(o, TagSet{"key": "value"})
	if err != ErrInvalidKafkaObjectType {
		t.Error("Expected ErrInvalidKafkaObjectType error")
	}

	// Test nil TagSet.
	o = KafkaObject{Type: "broker", ID: "1002"}

	err = store.SetTags(o, nil)
	if err != ErrNilTagSet {
		t.Error("Expected ErrNilTagSet error")
	}

	// Test empty TagSet.
	o = KafkaObject{Type: "broker", ID: "1002"}

	err = store.SetTags(o, TagSet{})
	if err != ErrNilTagSet {
		t.Error("Expected ErrNilTagSet error")
	}

	// Test reserved tag.
	if len(store.ReservedFields) == 0 {
		t.Error("ReservedFields len should be non-zero")
	}

	for k := range store.ReservedFields {
		o.Type = k
		for f := range store.ReservedFields[k] {
			err = store.SetTags(o, TagSet{f: "value"})
			switch err.(type) {
			case ErrReservedTag:
				continue
			default:
				t.Errorf("Expected ErrReservedTag error for type %s tag %s", k, f)
			}
		}
	}
}

func TestGetTags(t *testing.T) {
	testTagSets := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
	}

	testObjects := map[int]KafkaObject{
		0: KafkaObject{Type: "broker", ID: "1002"},
		1: KafkaObject{Type: "topic", ID: "test"},
	}

	expected := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
	}

	for k := range testTagSets {
		// Set tags.
		err := store.SetTags(testObjects[k], testTagSets[k])
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}

		// Fetch tags, compare value.
		tags, err := store.GetTags(testObjects[k])
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}

		if !tags.Equal(expected[k]) {
			t.Errorf("[test %d] Expected TagSet '%v', got '%v'",
				k, expected[k], tags)
		}
	}
}

func TestGetTagsFailures(t *testing.T) {
	// Test invalid object.
	_, err := store.GetTags(KafkaObject{Type: "fail"})
	if err != ErrInvalidKafkaObjectType {
		t.Error("Expected ErrInvalidKafkaObjectType error")
	}

	// Test non-existent object.
	_, err = store.GetTags(KafkaObject{Type: "broker", ID: "000"})
	if err != ErrKafkaObjectDoesNotExist {
		t.Error("Expected ErrKafkaObjectDoesNotExist error")
	}
}

func TestGetAllTags(t *testing.T) {
	testTagSets := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
	}

	testObjects := map[int]KafkaObject{
		0: KafkaObject{Type: "broker", ID: "1002"},
		1: KafkaObject{Type: "topic", ID: "test"},
	}

	expected := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
	}

	for k := range testTagSets {
		// Set tags.
		err := store.SetTags(testObjects[k], testTagSets[k])
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}
	}

	// Fetch tags, compare value.
	tags, _ := store.GetAllTags()

	for k := range testTagSets {
		obj := testObjects[k]
		expectedTags := expected[k]
		if reflect.DeepEqual(tags[obj], expectedTags) {
			t.Errorf("[test %d] Expected TagSet '%v', got '%v'",
				k, expected[k], tags)
		}
	}
}

func TestDeleteTags(t *testing.T) {
	testTagSets := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2", "key3": "value3"},
		1: TagSet{"key": "value"},
	}

	testObjects := map[int]KafkaObject{
		0: KafkaObject{Type: "broker", ID: "1002"},
		1: KafkaObject{Type: "topic", ID: "test"},
	}

	testTagDeletes := map[int]Tags{
		0: Tags{"key", "key2"},
		1: Tags{"key2"},
	}

	expected := map[int]TagSet{
		0: TagSet{"key3": "value3"},
		1: TagSet{"key": "value"},
	}

	for k := range testTagSets {
		// Set tags.
		err := store.SetTags(testObjects[k], testTagSets[k])
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}

		// Delete tags.
		err = store.DeleteTags(testObjects[k], testTagDeletes[k])
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}

		// Fetch tags, compare value.
		tags, err := store.GetTags(testObjects[k])
		if err != nil {
			t.Errorf("[test %d] %s", k, err)
		}

		if !tags.Equal(expected[k]) {
			t.Errorf("[test %d] Expected TagSet '%v', got '%v'",
				k, expected[k], tags)
		}
	}
}

func TestDeleteTagsFailures(t *testing.T) {
	// Test invalid object.
	err := store.DeleteTags(KafkaObject{Type: "fail"}, Tags{"k"})
	if err != ErrInvalidKafkaObjectType {
		t.Error("Expected ErrInvalidKafkaObjectType error")
	}

	// Test non-existent object.
	err = store.DeleteTags(KafkaObject{Type: "broker", ID: "000"}, Tags{"k"})
	if err != ErrKafkaObjectDoesNotExist {
		t.Error("Expected ErrKafkaObjectDoesNotExist error")
	}

	// Test empty Tags.
	err = store.DeleteTags(KafkaObject{Type: "broker", ID: "000"}, Tags{})
	if err != ErrNilTags {
		t.Error("Expected ErrNilTags error")
	}

	err = store.DeleteTags(KafkaObject{Type: "broker", ID: "000"}, nil)
	if err != ErrNilTags {
		t.Error("Expected ErrNilTags error")
	}
}

// TestTearDown does any tear down cleanup.
func TestTearDown(t *testing.T) {
	paths := allChildren("/" + testConfig.Prefix)
	sort.Sort(sort.Reverse(byLength(paths)))

	for _, p := range paths {
		err := store.ZK.Delete(p)
		if err != nil {
			t.Log(p)
			t.Error(err)
		}
	}

	store.ZK.Close()
}
