package kafkazk

import (
	"fmt"
	"regexp"
	"sort"
	"testing"
	"time"

	zkclient "github.com/samuel/go-zookeeper/zk"
)

const (
	zkaddr   = "localhost:2181"
	zkprefix = "/kafka"
)

var (
	zkc *zkclient.Conn
	zki ZK
	// Create paths.
	paths = []string{
		zkprefix,
		zkprefix + "/brokers",
		zkprefix + "/brokers/ids",
		zkprefix + "/brokers/topics",
		zkprefix + "/admin",
		zkprefix + "/admin/reassign_partitions",
		zkprefix + "/config",
		zkprefix + "/config/topics",
	}
)

// Sort by string length.

type byLen []string

func (a byLen) Len() int {
	return len(a)
}

func (a byLen) Less(i, j int) bool {
	return len(a[i]) > len(a[j])
}

func (a byLen) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

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
		// Init a direct client.
		var err error
		zkc, _, err = zkclient.Connect([]string{zkaddr}, time.Second, zkclient.WithLogInfo(false))
		if err != nil {
			t.Errorf("Error initializing ZooKeeper client: %s", err.Error())
		}

		_, _, _ = zkc.Get("/")
		if s := zkc.State(); s != 100|101 {
			t.Errorf("ZooKeeper client not in a connected state (state=%d)", s)
		}

		// Init a ZooKeeper based ZK.
		var configPrefix string
		if len(zkprefix) > 0 {
			configPrefix = zkprefix[1:]
		} else {
			configPrefix = ""
		}

		zki, err = NewZK(&ZKConfig{
			Connect: zkaddr,
			Prefix:  configPrefix,
		})
		if err != nil {
			t.Errorf("Error initializing ZooKeeper client: %s", err.Error())
		}

		/*****************
		  Populate test data
		  *****************/

		// Create paths.
		for _, p := range paths {
			_, err := zkc.Create(p, []byte{}, 0, zkclient.WorldACL(31))
			if err != nil {
				t.Error(err)
			}
		}

		// Create topics.
		data := []byte(`{"version":1,"partitions":{"0":[1001,1002],"1":[1002,1001],"2":[1003,1004],"3":[1004,1003]}}`)
		for i := 0; i < 5; i++ {
			topic := fmt.Sprintf("%s/brokers/topics/topic%d", zkprefix, i)
			paths = append(paths, topic)
			_, err := zkc.Create(topic, data, 0, zkclient.WorldACL(31))
			if err != nil {
				t.Error(err)
			}
		}

		// Create reassignments data.
		data = []byte(`{"version":1,"partitions":[{"topic":"topic0","partition":0,"replicas":[1003,1004]}]}`)
		_, err = zkc.Set(zkprefix+"/admin/reassign_partitions", data, -1)
		if err != nil {
			t.Error(err)
		}

		// Create topic config.
		data = []byte(`{"version":1,"config":{"retention.ms":"129600000"}}`)
		paths = append(paths, zkprefix+"/config/topics/topic0")
		_, err = zkc.Create(zkprefix+"/config/topics/topic0", data, 0, zkclient.WorldACL(31))
		if err != nil {
			t.Error(err)
		}

		//{"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":["PLAINTEXT://172.21.52.195:9092"],"rack":"us-east-1a","jmx_port":9999,"host":"172.21.52.195","timestamp":"1519911707766","port":9092,"version":4}
		// Create brokers.
		rack := []string{"a", "b", "c"}
		for i := 0; i < 10; i++ {
			// Create data.
			data := fmt.Sprintf(`{"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":["PLAINTEXT://10.0.1.%d:9092"],"rack":"%s","jmx_port":9999,"host":"10.0.1.%d","timestamp":"%d","port":9092,"version":4}`,
				100+i, rack[i%3], 100+i, time.Now().Unix())
			p := fmt.Sprintf("%s/brokers/ids/%d", zkprefix, 1001+i)

			paths = append(paths, p)

			// Add.
			_, err = zkc.Create(p, []byte(data), 0, zkclient.WorldACL(31))
			if err != nil {
				t.Error(err)
			}
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
	paths = append(paths, "/test")
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
		paths = append(paths, "/test/"+expected[i])
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

func TestGetReassignments(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	re := zki.GetReassignments()

	if len(re) != 1 {
		t.Errorf("Expected 1 reassignment, got %d", len(re))
	}

	if _, exist := re["topic0"]; !exist {
		t.Error("Expected 'topic0' in reassignments")
	}

	replicas, exist := re["topic0"][0]
	if !exist {
		t.Error("Expected topic0 partition 0 in reassignments")
	}

	sort.Ints(replicas)

	expected := []int{1003, 1004}
	for i, r := range replicas {
		if r != expected[i] {
			t.Errorf("Expected replica '%d', got '%d'", expected[i], r)
		}
	}
}

func TestGetTopics(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	rs := []*regexp.Regexp{
		regexp.MustCompile("topic[0-2]"),
	}

	ts, err := zki.GetTopics(rs)
	if err != nil {
		t.Error(err)
	}

	sort.Strings(ts)

	expected := []string{"topic0", "topic1", "topic2"}

	if len(ts) != 3 {
		t.Errorf("Expected topic list len of 3, got %d", len(ts))
	}

	for i, n := range ts {
		if n != expected[i] {
			t.Errorf("Expected topic '%s', got '%s'", n, expected[i])
		}
	}
}

func TestGetTopicConfig(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	c, err := zki.GetTopicConfig("topic0")
	if err != nil {
		t.Error(err)
	}

	if c == nil {
		t.Error("Unexpectedly nil TopicConfig")
	}

	v, exist := c.Config["retention.ms"]
	if !exist {
		t.Error("Expected 'retention.ms' config key to exist")
	}

	if v != "129600000" {
		t.Errorf("Expected config value '129600000', got '%s'", v)
	}
}

func TestGetAllBrokerMeta(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	bm, err := zki.GetAllBrokerMeta()
	if err != nil {
		t.Error(err)
	}

	if len(bm) != 10 {
		t.Errorf("Expected BrokerMetaMap len of 10, got %d", len(bm))
	}

	expected := map[int]string{
		1001: "a",
		1002: "b",
		1003: "c",
		1004: "a",
		1005: "b",
		1006: "c",
		1007: "a",
		1008: "b",
		1009: "c",
		1010: "a",
	}

	for b, r := range bm {
		if r.Rack != expected[b] {
			t.Errorf("Expected rack '%s' for %d, got '%s'", expected[b], b, r.Rack)
		}
	}
}

// func TestGetTopicState(t *testing.T) {}
// func TestGetPartitionMap(t *testing.T) {}
// func TestUpdateKafkaConfig(t *testing.T) {}

// TestTearDown does any tear down cleanup.
func TestTearDown(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	errors := []error{}

	// We sort the paths by descending
	// length. This ensures that we're always
	// deleting children first.
	sort.Sort(byLen(paths))

	// Test data to be removed.

	for _, p := range paths {
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

	zki.Close()
	zkc.Close()
}
