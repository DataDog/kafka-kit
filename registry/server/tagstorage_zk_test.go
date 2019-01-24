package server

import (
	"fmt"
	"sort"
	"testing"

	"github.com/DataDog/kafka-kit/kafkazk"
)

const (
	zkaddr   = "localhost:2181"
	zkprefix = "registry_test"
)

var (
	store *ZKTagStorage
	// To track znodes created.
	paths = []string{
		zkprefix,
		zkprefix + "/broker",
		zkprefix + "/topic",
	}
)

func TestSetup(t *testing.T) {
	if testing.Short() {
		t.Skip()
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
	if testing.Short() {
		t.Skip()
	}

	testTagSets := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
		// An empty TagSet should be a no-op;
		// expected results 1 & 2 should be the same.
		2: TagSet{},
	}

	testObjects := map[int]KafkaObject{
		0: KafkaObject{Type: "broker", ID: "1002"},
		1: KafkaObject{Type: "topic", ID: "test"},
		2: KafkaObject{Type: "topic", ID: "test"},
	}

	expected := map[int]string{
		0: `{"key":"value","key2":"value2"}`,
		1: `{"key":"value"}`,
		2: `{"key":"value"}`,
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

		result, err := store.ZK.Get(tpath)
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
	if testing.Short() {
		t.Skip()
	}

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
	if testing.Short() {
		t.Skip()
	}

	testTagSets := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
		2: TagSet{},
	}

	testObjects := map[int]KafkaObject{
		0: KafkaObject{Type: "broker", ID: "1002"},
		1: KafkaObject{Type: "topic", ID: "test"},
		2: KafkaObject{Type: "topic", ID: "test"},
	}

	expected := map[int]TagSet{
		0: TagSet{"key": "value", "key2": "value2"},
		1: TagSet{"key": "value"},
		2: TagSet{"key": "value"},
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
	if testing.Short() {
		t.Skip()
	}

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

// Sort by string length.

type byLen []string

func (a byLen) Len() int           { return len(a) }
func (a byLen) Less(i, j int) bool { return len(a[i]) > len(a[j]) }
func (a byLen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// TestTearDown does any tear down cleanup.
func TestTearDown(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	// We sort the paths by descending
	// length. This ensures that we're always
	// deleting children first.
	sort.Sort(byLen(paths))

	// Remove test data.

	// Children first.
	for _, p := range []string{"broker", "topic"} {
		path := fmt.Sprintf("/%s/%s", zkprefix, p)
		children, err := store.ZK.Children(path)
		if err != nil {
			t.Error(err)
		}

		for _, c := range children {
			_ = store.ZK.Delete(fmt.Sprintf("%s/%s", path, c))
		}
	}

	for _, p := range paths {
		// The "/" addition is required because we're using
		// the zkprefix var for both Kafka prefixes and the
		// ZKTagStorage prefix configuration, which doesn't
		// take a leading /.
		if err := store.ZK.Delete("/" + p); err != nil {
			t.Error(err)
		}
	}

	store.ZK.Close()
}
