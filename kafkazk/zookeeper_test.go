package kafkazk

import (
	"fmt"
	"sort"
	"testing"
	"time"

	zkclient "github.com/samuel/go-zookeeper/zk"
)

const (
	zkaddr   = "localhost:2181"
	zkprefix = "kafka"
)

var (
	zkc *zkclient.Conn
	zki ZK
)

// TestSetup is used for long tests that
// rely on a blank ZooKeeper server listening
// on localhost:2181. A direct ZooKeeper client
// is initialized to write test data into ZooKeeper
// that a ZK interface implementation may be
// tested against. Any ZK to be tested should
// also be instantiated here.
// A usable setup can be done with the official
// ZooKeeper docker image:
// - $ docker pull zookeeper
// - $ docker run --rm -d -p 2181:2181 zookeeper
// While the long tests perform a teardown, it's
// preferable to run the container with --rm and just
// using starting a new one for each test run. The removal
// logic in TestTearDown is quite rudimentary. If any steps fail,
// subsequent test runs will likely produce errors.
func TestSetup(t *testing.T) {
	if !testing.Short() {
		// Dial and test direct client.
		var err error
		zkc, _, err = zkclient.Connect([]string{zkaddr}, time.Second, zkclient.WithLogInfo(false))
		if err != nil {
			t.Errorf("Error initializing ZooKeeper client: %s", err.Error())
		}

		_, _, _ = zkc.Get("/")
		if s := zkc.State(); s != 100|101 {
			t.Errorf("ZooKeeper client not in a connected state (state=%d)", s)
		}

		// Populate test data.
		zkc.Create("/"+zkprefix, []byte{}, 0, zkclient.WorldACL(31))

		// Init a ZooKeeper based ZK.
		zki, err = NewZK(&ZKConfig{
			Connect: zkaddr,
			Prefix:  zkprefix,
		})
		if err != nil {
			t.Errorf("Error initializing ZooKeeper client: %s", err.Error())
		}

	} else {
		t.Skip("Skipping long test setup")
	}
}

// This is tested in TestSetup.
// func TestNewZK(t *testing.T) {}
// func TestClose(t *testing.T) {}

func TestCreateSetGet(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	err := zki.Create("/test", "")
	if err != nil {
		t.Error(err)
	}

	err = zki.Set("/test", "test data")
	if err != nil {
		t.Error(err)
	}

	v, err := zki.Get("/test")
	if err != nil {
		t.Error(err)
	}

	if string(v) != "test data" {
		t.Errorf("Expected string 'test data', got '%s'", v)
	}

}

func TestCreateSequential(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	var err error
	for i := 0; i < 3; i++ {
		err = zki.CreateSequential("/test/seq", "")
		if err != nil {
			t.Error(err)
		}
	}

	c, _, err := zkc.Children("/test")
	if err != nil {
		t.Error(err)
	}

	sort.Strings(c)

	if len(c) != 3 {
		t.Errorf("Expected 3 znodes to be found, got %d", len(c))
	}

	expected := []string{
		"seq0000000000",
		"seq0000000001",
		"seq0000000002",
	}

	for i, z := range c {
		if z != expected[i] {
			t.Errorf("Expected znode '%s', got '%s'", expected[i], z)
		}
	}
}

func TestExists(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	e, err := zki.Exists("/test")
	if err != nil {
		t.Error(err)
	}

	if !e {
		t.Error("Expected path '/test' to exist")
	}
}

// func TestGetReassignments(t *testing.T) {}
// func TestGetTopics(t *testing.T) {}
// func TestGetTopicConfig(t *testing.T) {}
// func TestGetAllBrokerMeta(t *testing.T) {}
// func TestGetTopicState(t *testing.T) {}
// func TestGetPartitionMap(t *testing.T) {}
// func TestUpdateKafkaConfig(t *testing.T) {}

// TestTearDown does any tear down cleanup.
func TestTearDown(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	errors := []error{}

	// Test data to be removed.
	for _, p := range []string{
		"/test/seq0000000000",
		"/test/seq0000000001",
		"/test/seq0000000002",
		"/test",
	} {
		_, s, err := zkc.Get(p)
		if err != nil {
			errors = append(errors, err)
		} else {
			err = zkc.Delete(p, s.Version)
			if err != nil {
				errors = append(errors, err)
			}
		}
	}

	for _, e := range errors {
		fmt.Println(e.Error())
	}

	if len(errors) > 0 {
		t.Fail()
	}

}