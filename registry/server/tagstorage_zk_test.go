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

	store, err = NewZKTagStorage(ZKTagStorageConfig{Prefix: zkprefix})
	if err != nil {
		t.Error(err)
	}

	store.ZK = zk

	if err := store.Init(); err != nil {
		t.Fatal(err)
	}
}

func TestSetTags(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	ts := TagSet{
		"key":  "value",
		"key2": "value2",
	}

	o := KafkaObject{
		Type: "broker",
		ID:   "1002",
	}

	err := store.SetTags(o, ts)
	if err != nil {
		t.Error(err)
	}

	// Test invalid KafkaObject Type.
	o = KafkaObject{
		Type: "test",
		ID:   "1002",
	}

	err = store.SetTags(o, ts)
	if err != ErrInvalidKafkaObjectType {
		t.Error("Expected ErrInvalidKafkaObjectType error")
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
