//go:build integration
// +build integration

package kafkazk

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"testing"
	"time"

	zkclient "github.com/go-zookeeper/zk"

	"github.com/stretchr/testify/assert"
)

var (
	zkaddr   = "localhost:2181"
	zkprefix = "/kafkazk_test"
)

var (
	zkc *zkclient.Conn
	zki Handler
	// Paths to pre-populate.
	paths = []string{
		zkprefix,
		zkprefix + "/brokers",
		zkprefix + "/brokers/ids",
		zkprefix + "/brokers/topics",
		zkprefix + "/admin",
		zkprefix + "/admin/reassign_partitions",
		zkprefix + "/admin/delete_topics",
		zkprefix + "/config",
		zkprefix + "/config/topics",
		zkprefix + "/config/brokers",
		zkprefix + "/config/changes",
		zkprefix + "/version",
		// Topicmappr specific.
		"/topicmappr_test",
		"/topicmappr_test/brokermetrics",
		"/topicmappr_test/partitionmeta",
	}
)

// Sort by string length.

type byLen []string

func (a byLen) Len() int           { return len(a) }
func (a byLen) Less(i, j int) bool { return len(a[i]) > len(a[j]) }
func (a byLen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }

// rawHandler is used for testing unexported ZKHandler
// methods that are not part of the Handler interface.
func rawHandler(c *Config) (*ZKHandler, error) {
	z := &ZKHandler{
		Connect:       c.Connect,
		Prefix:        c.Prefix,
		MetricsPrefix: c.MetricsPrefix,
	}

	var err error
	z.client, _, err = zkclient.Connect([]string{z.Connect}, 10*time.Second, zkclient.WithLogInfo(false))
	if err != nil {
		return nil, err
	}

	return z, nil
}

// TestSetup is used for long tests that rely on a blank ZooKeeper
// server listening on localhost:2181. A direct ZooKeeper client
// is initialized to write test data into ZooKeeper that a Handler
// interface implementation may be tested against. Any Handler to be
// tested should also be instantiated here. A usable setup can be done
// with the official ZooKeeper docker image:
// - $ docker pull zookeeper
// - $ docker run --rm -d -p 2181:2181 zookeeper
// While the long tests perform a teardown, it's preferable to run the
// container with --rm and just using starting a new one for each test
// run. The removal logic in TestTearDown is quite rudimentary. If any
// steps fail, subsequent test runs will likely produce errors.
func TestSetup(t *testing.T) {
	overrideZKAddr := os.Getenv("TEST_ZK_ADDR")
	if overrideZKAddr != "" {
		zkaddr = overrideZKAddr
	}

	// Init a direct client.
	var err error
	zkc, _, err = zkclient.Connect([]string{zkaddr}, time.Second, zkclient.WithLogInfo(false))
	if err != nil {
		t.Fatalf("Error initializing ZooKeeper client: %s", err)
	}

	// Init a ZooKeeper based Handler.
	var configPrefix string
	if len(zkprefix) > 0 {
		configPrefix = zkprefix[1:]
	} else {
		configPrefix = ""
	}

	zki, err = NewHandler(&Config{
		Connect:       zkaddr,
		Prefix:        configPrefix,
		MetricsPrefix: "topicmappr_test",
	})

	if err != nil {
		t.Errorf("Error initializing ZooKeeper client: %s", err)
	}

	time.Sleep(250 * time.Millisecond)
	if !zki.Ready() {
		t.Fatal("ZooKeeper client not ready in 250ms")
	}

	/*****************
	Populate test data
	*****************/

	// Create paths.
	for _, p := range paths {
		_, err := zkc.Create(p, []byte{}, 0, zkclient.WorldACL(31))
		if err != nil {
			t.Error(fmt.Sprintf("path %s: %s", p, err))
		}
	}

	// Create topics.
	partitionMeta := NewPartitionMetaMap()
	data := []byte(`{"version":1,"partitions":{"0":[1001,1002],"1":[1002,1001],"2":[1003,1004],"3":[1004,1003]}}`)

	for i := 0; i < 5; i++ {
		// Init config.
		topic := fmt.Sprintf("topic%d", i)
		p := fmt.Sprintf("%s/brokers/topics/%s", zkprefix, topic)
		_, err := zkc.Create(p, data, 0, zkclient.WorldACL(31))
		if err != nil {
			t.Error(err)
		}

		// Create partition meta.
		partitionMeta[topic] = map[int]*PartitionMeta{
			0: {Size: 1000.00},
			1: {Size: 2000.00},
			2: {Size: 3000.00},
			3: {Size: 4000.00},
		}

		// Create topic configs, states.
		statePaths := []string{
			fmt.Sprintf("%s/brokers/topics/topic%d/partitions", zkprefix, i),
		}

		for j := 0; j < 4; j++ {
			statePaths = append(statePaths, fmt.Sprintf("%s/brokers/topics/topic%d/partitions/%d", zkprefix, i, j))
			statePaths = append(statePaths, fmt.Sprintf("%s/brokers/topics/topic%d/partitions/%d/state", zkprefix, i, j))
		}

		for _, p := range statePaths {
			_, err := zkc.Create(p, []byte{}, 0, zkclient.WorldACL(31))
			if err != nil {
				t.Error(err)
			}
		}

		config := fmt.Sprintf(`{"version":3,"topic_id":"bl1zjuFPR6acRu_IjMJwVA%d", "partitions":{"0":[1001,1002], "1":[1002,1001], "2":[1003,1004], "3":[1004,1003]},"adding_replicas":{},"removing_replicas":{}}`, i)

		// Configure two topics to have reassignment data.
		if i == 2 || i == 3 {
			config = fmt.Sprintf(`{"version":3,"topic_id":"bl1zjuFPR6acRu_IjMJwVA%d", "partitions":{"0":[1001,1003,1002], "1":[1002,1001], "2":[1003,1004], "3":[1004,1003]},"adding_replicas":{"0":[1003]},"removing_replicas":{"0":[1001]}}`, i)
		}

		cfgPath := fmt.Sprintf("%s/brokers/topics/topic%d", zkprefix, i)
		_, err = zkc.Set(cfgPath, []byte(config), -1)
		if err != nil {
			t.Error(err)
		}

		states := []string{
			`{"controller_epoch":1,"leader":1001,"version":1,"leader_epoch":1,"isr":[1001,1002]}`,
			`{"controller_epoch":1,"leader":1002,"version":1,"leader_epoch":1,"isr":[1002,1001]}`,
			`{"controller_epoch":1,"leader":1003,"version":1,"leader_epoch":1,"isr":[1003,1004]}`,
			`{"controller_epoch":1,"leader":1004,"version":1,"leader_epoch":1,"isr":[1004,1003]}`,
		}

		// We need at least one topic/partition to appear as under-replicated to
		// test some functions.
		if i == 2 {
			states[0] = `{"controller_epoch":1,"leader":1002,"version":1,"leader_epoch":2,"isr":[1002]}`
		}

		for n, s := range states {
			path := fmt.Sprintf("%s/brokers/topics/topic%d/partitions/%d/state", zkprefix, i, n)
			_, err := zkc.Set(path, []byte(s), -1)
			if err != nil {
				t.Error(err)
			}
		}
	}

	// Store partition meta.
	data, _ = json.Marshal(partitionMeta)
	_, err = zkc.Set("/topicmappr_test/partitionmeta", data, -1)
	if err != nil {
		t.Error(err)
	}

	// Create reassignments data.
	data = []byte(`{"version":1,"partitions":[{"topic":"topic0","partition":0,"replicas":[1003,1004]}]}`)
	_, err = zkc.Set(zkprefix+"/admin/reassign_partitions", data, -1)
	if err != nil {
		t.Error(err)
	}

	// Create delete_topics data.
	_, err = zkc.Create(zkprefix+"/admin/delete_topics/deleting_topic", []byte{}, 0, zkclient.WorldACL(31))
	if err != nil {
		t.Error(err)
	}

	// Create topic config.
	data = []byte(`{"version":1,"config":{"retention.ms":"129600000"}}`)
	_, err = zkc.Create(zkprefix+"/config/topics/topic0", data, 0, zkclient.WorldACL(31))
	if err != nil {
		t.Error(err)
	}

	// Create brokers.
	rack := []string{"a", "b", "c"}
	for i := 0; i < 5; i++ {
		// Create data.
		data := fmt.Sprintf(`{"listener_security_protocol_map":{"PLAINTEXT":"PLAINTEXT"},"endpoints":["PLAINTEXT://10.0.1.%d:9092"],"rack":"%s","jmx_port":9999,"host":"10.0.1.%d","timestamp":"%d","port":9092,"version":4}`,
			100+i, rack[i%3], 100+i, time.Now().Unix())
		p := fmt.Sprintf("%s/brokers/ids/%d", zkprefix, 1001+i)

		// Add.
		_, err = zkc.Create(p, []byte(data), 0, zkclient.WorldACL(31))
		if err != nil {
			t.Error(err)
		}
	}

	// Create broker metrics.
	if err := setBrokerMetrics(); err != nil {
		t.Error(err)
	}
}

func setBrokerMetrics() error {
	data := []byte(`{
		"1001": {"StorageFree": 10000.00},
		"1002": {"StorageFree": 20000.00},
		"1003": {"StorageFree": 30000.00},
		"1004": {"StorageFree": 40000.00},
		"1005": {"StorageFree": 50000.00}}`)

	_, err := zkc.Set("/topicmappr_test/brokermetrics", data, -1)

	return err
}

func TestCreateSetGetDelete(t *testing.T) {
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

	err = zki.Delete("/test")
	if err != nil {
		t.Error(err)
	}

	_, err = zki.Get("/test")
	switch err.(type) {
	case ErrNoNode:
		break
	default:
		t.Error("Expected ErrNoNode error")
	}
}

func TestCreateSequential(t *testing.T) {
	err := zki.Create(zkprefix+"/test", "")
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 3; i++ {
		err = zki.CreateSequential(zkprefix+"/test/seq", "")
		if err != nil {
			t.Error(err)
		}
	}

	c, _, err := zkc.Children(zkprefix + "/test")
	if err != nil {
		t.Error(err)
	}

	sort.Strings(c)

	if len(c) != 3 {
		t.Fatalf("Expected 3 znodes to be found, got %d", len(c))
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
	e, err := zki.Exists(zkprefix)
	if err != nil {
		t.Error(err)
	}

	if !e {
		t.Errorf("Expected path '%s' to exist", zkprefix)
	}
}

func TestNextInt(t *testing.T) {
	for _, expected := range []int32{1, 2, 3} {
		v, err := zki.NextInt(zkprefix + "/version")
		if err != nil {
			t.Fatal(err)
		}

		if v != expected {
			t.Errorf("Expected version value %d, got %d", expected, v)
		}
	}
}

func TestGetUnderReplicated(t *testing.T) {
	ur, err := zki.GetUnderReplicated()
	if err != nil {
		t.Error(err)
	}

	expected := []string{"topic2", "topic3"}
	sort.Strings(ur)

	assert.Equal(t, expected, ur)
}

func TestListReassignments(t *testing.T) {
	re, _ := zki.ListReassignments()

	expected := Reassignments{
		"topic2": {0: []int{1003, 1002}},
		"topic3": {0: []int{1003, 1002}},
	}

	assert.Equal(t, expected, re)
}

func TestGetReassignments(t *testing.T) {
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

func TestGetPendingDeletion(t *testing.T) {
	pd, err := zki.GetPendingDeletion()
	if err != nil {
		t.Error(err)
	}

	if len(pd) != 1 {
		t.Fatalf("Expected 1 pending delete topic, got %d", len(pd))
	}

	if pd[0] != "deleting_topic" {
		t.Errorf("Unexpected deleting topic name: %s", pd[0])
	}
}

func TestGetTopics(t *testing.T) {
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
	bm, err := zki.GetAllBrokerMeta(false)
	if err != nil {
		t.Error(err)
	}

	if len(bm) != 5 {
		t.Errorf("Expected BrokerMetaMap len of 5, got %d", len(bm))
	}

	expected := map[int]string{
		1001: "a",
		1002: "b",
		1003: "c",
		1004: "a",
		1005: "b",
	}

	for b, r := range bm {
		if r.Rack != expected[b] {
			t.Errorf("Expected rack '%s' for %d, got '%s'", expected[b], b, r.Rack)
		}
	}
}

func TestGetBrokerMetrics(t *testing.T) {
	// Get broker meta withMetrics.
	bm, err := zki.GetAllBrokerMeta(true)
	if err != nil {
		t.Error(err)
	}

	expected := map[int]float64{
		1001: 10000.00,
		1002: 20000.00,
		1003: 30000.00,
		1004: 40000.00,
		1005: 50000.00,
	}

	for b, v := range bm {
		if v.StorageFree != expected[b] {
			t.Errorf("Unexpected StorageFree metric for broker %d", b)
		}
	}
}

func TestGetBrokerMetricsCompressed(t *testing.T) {
	// Create a compressed version of the metrics data.
	data := []byte(`{
		"1001": {"StorageFree": 10000.00},
		"1002": {"StorageFree": 20000.00},
		"1003": {"StorageFree": 30000.00},
		"1004": {"StorageFree": 40000.00},
		"1005": {"StorageFree": 50000.00}}`)

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err := zw.Write(data)
	if err != nil {
		t.Error(err)
	}

	if err = zw.Close(); err != nil {
		t.Error(err)
	}

	// Store the compressed version.
	_, err = zkc.Set("/topicmappr_test/brokermetrics", buf.Bytes(), -1)
	if err != nil {
		t.Fatal(err)
	}

	// Test fetching the compressed version.
	bm, errs := zki.GetAllBrokerMeta(true)
	if errs != nil {
		t.Error(err)
	}

	expected := map[int]float64{
		1001: 10000.00,
		1002: 20000.00,
		1003: 30000.00,
		1004: 40000.00,
		1005: 50000.00,
	}

	for b, v := range bm {
		if v.StorageFree != expected[b] {
			t.Errorf("Unexpected StorageFree metric for broker %d", b)
		}
	}

	// Rewrite the uncompressed version.
	if err := setBrokerMetrics(); err != nil {
		t.Error(err)
	}
}

func TestGetAllPartitionMeta(t *testing.T) {
	pm, err := zki.GetAllPartitionMeta()
	if err != nil {
		t.Error(err)
	}

	expected := map[int]float64{
		0: 1000.00,
		1: 2000.00,
		2: 3000.00,
		3: 4000.00,
	}

	for i := 0; i < 5; i++ {
		topic := fmt.Sprintf("topic%d", i)
		meta, exists := pm[topic]
		if !exists {
			t.Errorf("Expected topic '%s' in partition meta", topic)
		}

		for partn, m := range meta {
			if m.Size != expected[partn] {
				t.Errorf("Expected size %f for %s %d, got %f", expected[partn], topic, partn, m.Size)
			}
		}
	}

}

func TestGetAllPartitionMetaCompressed(t *testing.T) {
	// Fetch and hold the original partition meta.
	pm, err := zki.GetAllPartitionMeta()
	if err != nil {
		t.Error(err)
	}

	pmOrig, _ := json.Marshal(pm)

	// Create a compressed copy.
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)

	_, err = zw.Write(pmOrig)
	if err != nil {
		t.Error(err)
	}

	if err := zw.Close(); err != nil {
		t.Error(err)
	}

	// Store the compressed copy.
	_, err = zkc.Set("/topicmappr_test/partitionmeta", buf.Bytes(), -1)
	if err != nil {
		t.Error(err)
	}

	// Test fetching the compressed copy.

	pm, err = zki.GetAllPartitionMeta()
	if err != nil {
		t.Error(err)
	}

	expected := map[int]float64{
		0: 1000.00,
		1: 2000.00,
		2: 3000.00,
		3: 4000.00,
	}

	for i := 0; i < 5; i++ {
		topic := fmt.Sprintf("topic%d", i)
		meta, exists := pm[topic]
		if !exists {
			t.Errorf("Expected topic '%s' in partition meta", topic)
		}

		for partn, m := range meta {
			if m.Size != expected[partn] {
				t.Errorf("Expected size %f for %s %d, got %f", expected[partn], topic, partn, m.Size)
			}
		}
	}

	// Reset to the original partitionMeta.
	_, err = zkc.Set("/topicmappr_test/partitionmeta", pmOrig, -1)
	if err != nil {
		t.Error(err)
	}

}

func TestOldestMetaTs(t *testing.T) {
	// Init a ZKHandler.
	var configPrefix string
	if len(zkprefix) > 0 {
		configPrefix = zkprefix[1:]
	} else {
		configPrefix = ""
	}

	zkr, err := rawHandler(&Config{
		Connect:       zkaddr,
		Prefix:        configPrefix,
		MetricsPrefix: "topicmappr_test",
	})
	if err != nil {
		t.Errorf("Error initializing ZooKeeper client: %s", err)
	}

	var m *zkclient.Stat

	// Get the lowest Mtime value.

	_, m, err = zkc.Get("/topicmappr_test/partitionmeta")
	if err != nil {
		t.Error(err)
	}

	ts1 := m.Mtime

	_, m, err = zkc.Get("/topicmappr_test/brokermetrics")
	if err != nil {
		t.Error(err)
	}

	ts2 := m.Mtime

	var min int64
	if ts1 < ts2 {
		min = ts1
	} else {
		min = ts2
	}

	// Get the ts.
	expected := min * 1000000
	age, err := zkr.oldestMetaTs()
	if err != nil {
		t.Error(err)
	}

	if age != expected {
		t.Errorf("Expected meta ts of %d, got %d", expected, age)
	}
}

func TestGetTopicState(t *testing.T) {
	ts, err := zki.GetTopicState("topic0")
	if err != nil {
		t.Error(err)
	}

	if len(ts.Partitions) != 4 {
		t.Errorf("Expected TopicState.Partitions len of 4, got %d", len(ts.Partitions))
	}

	expected := map[string][]int{
		"0": {1001, 1002},
		"1": {1002, 1001},
		"2": {1003, 1004},
		"3": {1004, 1003},
	}

	for p, rs := range ts.Partitions {
		v, exists := expected[p]
		if !exists {
			t.Errorf("Expected partition %s in TopicState", p)
		}

		if len(rs) != len(v) {
			t.Error("Unexpected replica set length")
		}

		for n := range rs {
			if rs[n] != v[n] {
				t.Errorf("Expected ID %d, got %d", v[n], rs[n])
			}
		}
	}
}

func TestGetTopicStateISR(t *testing.T) {
	ts, err := zki.GetTopicStateISR("topic0")
	if err != nil {
		t.Error(err)
	}

	if len(ts) != 4 {
		t.Errorf("Expected TopicState.Partitions len of 4, got %d", len(ts))
	}

	expected := map[string][]int{
		"0": {1001, 1002},
		"1": {1002, 1001},
		"2": {1003, 1004},
		"3": {1004, 1003},
	}

	for p := range ts {
		v, exists := expected[p]
		if !exists {
			t.Errorf("Expected partition %s in TopicState", p)
		}

		if len(ts[p].ISR) != len(v) {
			t.Error("Unexpected replica set length")
		}

		for n := range ts[p].ISR {
			if ts[p].ISR[n] != v[n] {
				t.Errorf("Expected ID %d, got %d", v[n], ts[p].ISR[n])
			}
		}
	}
}

func TestGetPartitionMap(t *testing.T) {
	pm, err := zki.GetPartitionMap("topic0")
	if err != nil {
		t.Error(err)
	}

	expected := &PartitionMap{
		Version: 1,
		Partitions: PartitionList{
			Partition{Topic: "topic0", Partition: 0, Replicas: []int{1003, 1004}}, // Via the stub reassign_partitions data.
			Partition{Topic: "topic0", Partition: 1, Replicas: []int{1002, 1001}},
			Partition{Topic: "topic0", Partition: 2, Replicas: []int{1003, 1004}},
			Partition{Topic: "topic0", Partition: 3, Replicas: []int{1004, 1003}},
		},
	}

	if matches, err := pm.Equal(expected); !matches {
		t.Errorf("Unexpected PartitionMap inequality: %s", err)
	}
}

func TestUpdateKafkaConfigBroker(t *testing.T) {
	c := KafkaConfig{
		Type: "broker",
		Name: "1001",
		Configs: []KafkaConfigKV{
			{"leader.replication.throttled.rate", "100000"},
			{"follower.replication.throttled.rate", "100000"},
		},
	}

	_, err := zki.UpdateKafkaConfig(c)
	if err != nil {
		t.Error(err)
	}

	// Re-running the same config should
	// be a no-op.
	changes, err := zki.UpdateKafkaConfig(c)
	if err != nil {
		t.Error(err)
	}

	for _, change := range changes {
		if change {
			t.Error("Unexpected config update change status")
		}
	}

	// Validate the config.
	d, _, err := zkc.Get(zkprefix + "/config/changes/config_change_0000000000")
	if err != nil {
		t.Error(err)
	}

	expected := `{"version":2,"entity_path":"brokers/1001"}`
	if string(d) != expected {
		t.Errorf("Expected config '%s', got '%s'", expected, string(d))
	}

	d, _, err = zkc.Get(zkprefix + "/config/brokers/1001")
	if err != nil {
		t.Error(err)
	}

	expected = `{"version":1,"config":{"follower.replication.throttled.rate":"100000","leader.replication.throttled.rate":"100000"}}`
	if string(d) != expected {
		t.Errorf("Expected config '%s', got '%s'", expected, string(d))
	}
}

func TestUpdateKafkaConfigTopic(t *testing.T) {
	c := KafkaConfig{
		Type: "topic",
		Name: "topic0",
		Configs: []KafkaConfigKV{
			{"leader.replication.throttled.replicas", "1003,1004"},
			{"follower.replication.throttled.replicas", "1003,1004"},
		},
	}

	_, err := zki.UpdateKafkaConfig(c)
	if err != nil {
		t.Error(err)
	}

	// Re-running the same config should
	// be a no-op.
	changes, err := zki.UpdateKafkaConfig(c)
	if err != nil {
		t.Error(err)
	}

	for _, change := range changes {
		if change {
			t.Error("Unexpected config update change status")
		}
	}

	// Validate the config.
	d, _, err := zkc.Get(zkprefix + "/config/changes/config_change_0000000001")
	if err != nil {
		t.Error(err)
	}

	expected := `{"version":2,"entity_path":"topics/topic0"}`
	if string(d) != expected {
		t.Errorf("Expected config '%s', got '%s'", expected, string(d))
	}

	d, _, err = zkc.Get(zkprefix + "/config/topics/topic0")
	if err != nil {
		t.Error(err)
	}

	expected = `{"version":1,"config":{"follower.replication.throttled.replicas":"1003,1004","leader.replication.throttled.replicas":"1003,1004","retention.ms":"129600000"}}`
	if string(d) != expected {
		t.Errorf("Expected config '%s', got '%s'", expected, string(d))
	}
}

// TestTearDown does any tear down cleanup.
func TestTearDown(t *testing.T) {
	// Test data to be removed.
	roots := []string{
		zkprefix,
		"/topicmappr_test",
	}

	var paths []string
	for _, p := range roots {
		paths = append(paths, allChildren(p)...)
	}

	sort.Sort(sort.Reverse(byLength(paths)))

	for _, p := range paths {
		_, s, err := zkc.Get(p)
		if err != nil {
			t.Log(p)
			t.Error(err)
		} else {
			err = zkc.Delete(p, s.Version)
			if err != nil {
				t.Log(p)
				t.Error(err)
			}
		}
	}

	zki.Close()
	zkc.Close()
}

// Recursive search.
func allChildren(p string) []string {
	paths := []string{p}

	children, _, _ := zkc.Children(p)
	for _, c := range children {
		paths = append(paths, allChildren(fmt.Sprintf("%s/%s", p, c))...)
	}

	return paths
}

type byLength []string

func (s byLength) Len() int           { return len(s) }
func (s byLength) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byLength) Less(i, j int) bool { return len(s[i]) < len(s[j]) }
