package kafkazk

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strconv"
	"time"

	zkclient "github.com/samuel/go-zookeeper/zk"
)

var (
	// ErrInvalidKafkaConfigType error.
	ErrInvalidKafkaConfigType = errors.New("Invalid Kafka config type")
	// validKafkaConfigTypes is used as a set
	// to define valid configuration type names.
	validKafkaConfigTypes = map[string]struct{}{
		"broker": struct{}{},
		"topic":  struct{}{},
	}
)

// ErrNoNode error type is specifically for
// Get method calls where the underlying
// error type is a zkclient.ErrNoNode.
type ErrNoNode struct {
	s string
}

func (e ErrNoNode) Error() string {
	return e.s
}

// Handler provides basic ZooKeeper operations along with
// calls that return kafkazk types describing Kafka states.
type Handler interface {
	Exists(string) (bool, error)
	Create(string, string) error
	CreateSequential(string, string) error
	Set(string, string) error
	Get(string) ([]byte, error)
	Delete(string) error
	Children(string) ([]string, error)
	Close()
	Ready() bool
	// Kafka specific.
	GetTopicState(string) (*TopicState, error)
	GetTopicStateISR(string) (TopicStateISR, error)
	UpdateKafkaConfig(KafkaConfig) (bool, error)
	GetReassignments() Reassignments
	GetTopics([]*regexp.Regexp) ([]string, error)
	GetTopicConfig(string) (*TopicConfig, error)
	GetAllBrokerMeta(bool) (BrokerMetaMap, []error)
	GetAllPartitionMeta() (PartitionMetaMap, error)
	MaxMetaAge() (time.Duration, error)
	GetPartitionMap(string) (*PartitionMap, error)
}

// TopicState is used for unmarshing ZooKeeper json data from a topic:
// e.g. /brokers/topics/some-topic
type TopicState struct {
	Partitions map[string][]int `json:"partitions"`
}

// TopicStateISR is a map of partition numbers to PartitionState.
type TopicStateISR map[string]PartitionState

// PartitionState is used for unmarshalling json data from a partition state:
// e.g. /brokers/topics/some-topic/partitions/0/state
type PartitionState struct {
	Version         int   `json:"version"`
	ControllerEpoch int   `json:"controller_epoch"`
	Leader          int   `json:"leader"`
	LeaderEpoch     int   `json:"leader_epoch"`
	ISR             []int `json:"isr"`
}

// Reassignments is a map of topic:partition:brokers.
type Reassignments map[string]map[int][]int

// reassignPartitions is used for unmarshalling
// /admin/reassign_partitions data.
type reassignPartitions struct {
	Partitions []reassignConfig `json:"partitions"`
}

type reassignConfig struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Replicas  []int  `json:"replicas"`
}

// TopicConfig is used for unmarshalling
// /config/topics/<topic> from ZooKeeper.
type TopicConfig struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

// KafkaConfig is used to issue configuration updates to either
// topics or brokers in ZooKeeper.
type KafkaConfig struct {
	Type    string      // Topic or broker.
	Name    string      // Entity name.
	Configs [][2]string // Slice of [2]string{key,value} configs.

}

// KafkaConfigData is used for unmarshalling
// /config/<type>/<name> data from ZooKeeper.
type KafkaConfigData struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

// NewKafkaConfigData creates a KafkaConfigData.
func NewKafkaConfigData() KafkaConfigData {
	return KafkaConfigData{
		Config: make(map[string]string),
	}
}

// ZKHandler implements the Handler interface
// for real ZooKeeper clusters.
type ZKHandler struct {
	client        *zkclient.Conn
	Connect       string
	Prefix        string
	MetricsPrefix string
}

// Config holds initialization paramaters for a Handler. Connect
// is a ZooKeeper connect string. Prefix should reflect any prefix
// used for Kafka on the reference ZooKeeper cluster (excluding slashes).
// MetricsPrefix is the prefix used for broker metrics metadata persisted
// in ZooKeeper.
type Config struct {
	Connect       string
	Prefix        string
	MetricsPrefix string
}

// NewHandler takes a *Config, performs
// any initialization and returns a Handler.
func NewHandler(c *Config) (Handler, error) {
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

// Ready returns true if the client is in either state
// StateConnected or StateHasSession.
// See https://godoc.org/github.com/samuel/go-zookeeper/zk#State.
func (z *ZKHandler) Ready() bool {
	switch z.client.State() {
	case 100, 101:
		return true
	default:
		return false
	}
}

// Close calls close on the *ZKHandler. Any additional
// shutdown cleanup or other tasks should be performed here.
func (z *ZKHandler) Close() {
	z.client.Close()
}

// Get returns the data from path p.
func (z *ZKHandler) Get(p string) ([]byte, error) {
	r, _, e := z.client.Get(p)

	if e != nil {
		switch e {
		case zkclient.ErrNoNode:
			return nil, ErrNoNode{s: fmt.Sprintf("[%s] %s", p, e.Error())}
		default:
			return nil, fmt.Errorf("[%s] %s", p, e.Error())
		}
	}

	return r, nil
}

// Set sets the data at path p.
func (z *ZKHandler) Set(p string, d string) error {
	_, e := z.client.Set(p, []byte(d), -1)
	var err error
	if e != nil {
		err = fmt.Errorf("[%s] %s", p, e.Error())
	}

	return err
}

// Delete deletes the znode at path p.
func (z *ZKHandler) Delete(p string) error {
	_, s, err := z.client.Get(p)
	if err != nil {
		return fmt.Errorf("[%s] %s", p, err)
	}

	err = z.client.Delete(p, s.Version)
	if err != nil {
		return fmt.Errorf("[%s] %s", p, err)
	}

	return nil
}

// CreateSequential takes a path p and data d and creates
// a sequential znode at p with data d. An error is
// returned if encountered.
func (z *ZKHandler) CreateSequential(p string, d string) error {
	_, e := z.client.Create(p, []byte(d), zkclient.FlagSequence, zkclient.WorldACL(31))
	var err error
	if e != nil {
		err = fmt.Errorf("[%s] %s", p, e.Error())
	}

	return err
}

// Create creates the provided path p with the data
// from the provided string d and returns an error
// if encountered.
func (z *ZKHandler) Create(p string, d string) error {
	_, e := z.client.Create(p, []byte(d), 0, zkclient.WorldACL(31))
	if e != nil {
		switch e {
		case zkclient.ErrNoNode:
			return ErrNoNode{s: fmt.Sprintf("[%s] %s", p, e.Error())}
		default:
			return fmt.Errorf("[%s] %s", p, e.Error())
		}
	}

	return nil
}

// Exists takes a path p and returns a bool as to whether the
// path exists and an error if encountered.
func (z *ZKHandler) Exists(p string) (bool, error) {
	b, _, e := z.client.Exists(p)
	var err error
	if e != nil {
		err = fmt.Errorf("[%s] %s", p, e.Error())
	}

	return b, err
}

// Children takes a path p and returns a list
// of child znodes and an error if encountered.
func (z *ZKHandler) Children(p string) ([]string, error) {
	c, _, e := z.client.Children(p)

	if e != nil {
		switch e {
		case zkclient.ErrNoNode:
			return nil, ErrNoNode{s: fmt.Sprintf("[%s] %s", p, e.Error())}
		default:
			return nil, fmt.Errorf("[%s] %s", p, e.Error())
		}
	}

	return c, nil
}

// GetReassignments looks up any ongoing topic reassignments and
// returns the data as a Reassignments.
func (z *ZKHandler) GetReassignments() Reassignments {
	reassigns := Reassignments{}

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/admin/reassign_partitions", z.Prefix)
	} else {
		path = "/admin/reassign_partitions"
	}

	// Get reassignment config.
	data, err := z.Get(path)
	if err != nil {
		return reassigns
	}

	rec := &reassignPartitions{}
	json.Unmarshal(data, rec)

	// Map reassignment config to a
	// Reassignments.
	for _, cfg := range rec.Partitions {
		if reassigns[cfg.Topic] == nil {
			reassigns[cfg.Topic] = map[int][]int{}
		}
		reassigns[cfg.Topic][cfg.Partition] = cfg.Replicas
	}

	return reassigns
}

// GetTopics takes a []*regexp.Regexp and returns a []string of all topic
// names that match any of the provided regex.
func (z *ZKHandler) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	matchingTopics := []string{}

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/topics", z.Prefix)
	} else {
		path = "/brokers/topics"
	}

	// Find all topics in zk.
	entries, err := z.Children(path)
	if err != nil {
		return nil, err
	}

	matched := map[string]bool{}
	// Get all topics that match all
	// provided topic regexps.
	for _, topicRe := range ts {
		for _, topic := range entries {
			if topicRe.MatchString(topic) {
				matched[topic] = true
			}
		}
	}

	// Add matches to a slice.
	for topic := range matched {
		matchingTopics = append(matchingTopics, topic)
	}

	return matchingTopics, nil
}

// GetTopicConfig takes a topic name. If the topic exists, the topic config
// is returned as a *TopicConfig.
func (z *ZKHandler) GetTopicConfig(t string) (*TopicConfig, error) {
	config := &TopicConfig{}

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/config/topics/%s", z.Prefix, t)
	} else {
		path = fmt.Sprintf("/config/topics/%s", t)
	}

	// Get topic config.
	data, err := z.Get(path)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(data, config)

	return config, nil
}

// GetAllBrokerMeta looks up all registered Kafka brokers and returns their
// metadata as a BrokerMetaMap. A withMetrics bool param determines whether
// we additionally want to fetch stored broker metrics.
func (z *ZKHandler) GetAllBrokerMeta(withMetrics bool) (BrokerMetaMap, []error) {
	var errs []error

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/ids", z.Prefix)
	} else {
		path = "/brokers/ids"
	}

	// Get all brokers.
	entries, err := z.Children(path)
	if err != nil {
		return nil, []error{err}
	}

	bmm := BrokerMetaMap{}

	// Map each broker.
	for _, b := range entries {
		bm := &BrokerMeta{}
		// In case we encounter non-ints (broker IDs) for
		// whatever reason, just continue.
		bid, err := strconv.Atoi(b)
		if err != nil {
			continue
		}

		// Fetch & unmarshal the data for each broker.
		bpath := fmt.Sprintf("%s/%s", path, b)
		data, err := z.Get(bpath)
		// XXX do something else.
		if err != nil {
			continue
		}

		err = json.Unmarshal(data, bm)
		if err != nil {
			continue
		}

		bmm[bid] = bm
	}

	// Fetch and populate in metrics.
	if withMetrics {
		bmetrics, err := z.getBrokerMetrics()
		if err != nil {
			return nil, []error{err}
		}

		// Populate each broker with
		// metric data.
		for bid := range bmm {
			m, exists := bmetrics[bid]
			if !exists {
				errs = append(errs, fmt.Errorf("Metrics not found for broker %d", bid))
				bmm[bid].MetricsIncomplete = true
			} else {
				bmm[bid].StorageFree = m.StorageFree
			}
		}

	}

	return bmm, errs
}

// GetBrokerMetrics fetches broker metrics stored in ZooKeeper and returns
// a BrokerMetricsMap and an error if encountered.
func (z *ZKHandler) getBrokerMetrics() (BrokerMetricsMap, error) {
	var path string
	if z.MetricsPrefix != "" {
		path = fmt.Sprintf("/%s/brokermetrics", z.MetricsPrefix)
	} else {
		path = "/brokermetrics"
	}

	// Fetch the metrics object.
	data, err := z.Get(path)
	if err != nil {
		return nil, fmt.Errorf("Error fetching broker metrics: %s", err.Error())
	}

	// Check if the data is compressed.
	if out, compressed := uncompress(data); compressed {
		data = out
	}

	bmm := BrokerMetricsMap{}
	err = json.Unmarshal(data, &bmm)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling broker metrics: %s", err.Error())
	}

	return bmm, nil
}

// GetAllPartitionMeta fetches partition metadata stored in Zookeeper.
func (z *ZKHandler) GetAllPartitionMeta() (PartitionMetaMap, error) {
	var path string
	if z.MetricsPrefix != "" {
		path = fmt.Sprintf("/%s/partitionmeta", z.MetricsPrefix)
	} else {
		path = "/partitionmeta"
	}

	// Fetch the metrics object.
	data, err := z.Get(path)
	if err != nil {
		return nil, fmt.Errorf("Error fetching partition meta: %s", err.Error())
	}

	if string(data) == "" {
		return nil, errors.New("No partition meta")
	}

	// Check if the data is compressed.
	if out, compressed := uncompress(data); compressed {
		data = out
	}

	pmm := NewPartitionMetaMap()
	err = json.Unmarshal(data, &pmm)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling partition meta: %s", err.Error())
	}

	return pmm, nil
}

// MaxMetaAge returns the greatest age between the partitionmeta
// and brokermetrics stuctures.
func (z *ZKHandler) MaxMetaAge() (time.Duration, error) {
	t, err := z.oldestMetaTs()
	if err != nil {
		return time.Nanosecond, err
	}

	return time.Since(time.Unix(0, t)), nil
}

// oldestMetaTs returns returns the oldest unix epoch ns between
// partitionmeta and brokermetrics stuctures.
func (z *ZKHandler) oldestMetaTs() (int64, error) {
	var paths []string

	for _, p := range []string{"partitionmeta", "brokermetrics"} {
		var path string
		if z.MetricsPrefix != "" {
			path = fmt.Sprintf("/%s/%s", z.MetricsPrefix, p)
		} else {
			path = fmt.Sprintf("/%s", p)
		}
		paths = append(paths, path)
	}

	var min int64 = math.MaxInt64
	var ts int64

	// Get the lowest Mtime (ts).
	for _, p := range paths {
		_, s, e := z.client.Get(p)
		if e != nil {
			switch e {
			case zkclient.ErrNoNode:
				return 0, ErrNoNode{s: fmt.Sprintf("[%s] %s", p, e.Error())}
			default:
				return 0, fmt.Errorf("[%s] %s", p, e.Error())
			}
		}

		if s.Mtime < min {
			min = s.Mtime
			// To ns.
			ts = s.Mtime * 1000000
		}
	}

	return ts, nil
}

// GetTopicState takes a topic name. If the topic exists,
// the topic state is returned as a *TopicState.
func (z *ZKHandler) GetTopicState(t string) (*TopicState, error) {
	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/topics/%s", z.Prefix, t)
	} else {
		path = fmt.Sprintf("/brokers/topics/%s", t)
	}

	// Fetch topic data from z.
	ts := &TopicState{}
	data, err := z.Get(path)
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(data, ts)
	if err != nil {
		return nil, err
	}

	return ts, nil
}

// GetTopicStateISR takes a topic name. If the topic exists, the topic state
// is returned as a TopicStateISR. GetTopicStateCurrentISR differs from
// GetTopicState in that the actual, current broker IDs in the ISR are
// returned for each partition. This method is more expensive due to the
// need for a call per partition to ZK.
func (z *ZKHandler) GetTopicStateISR(t string) (TopicStateISR, error) {
	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/topics/%s/partitions", z.Prefix, t)
	} else {
		path = fmt.Sprintf("/brokers/topics/%s/partitions", t)
	}

	ts := TopicStateISR{}

	// Get partitions.
	partitions, err := z.Children(path)
	if err != nil {
		return nil, err
	}

	// Get partition data.
	for _, p := range partitions {
		ppath := fmt.Sprintf("%s/%s/state", path, p)
		data, err := z.Get(ppath)
		if err != nil {
			return nil, err
		}

		state := PartitionState{}
		err = json.Unmarshal(data, &state)
		if err != nil {
			return nil, err
		}

		// Populate into TopicState.
		ts[p] = state
	}

	return ts, nil
}

// GetPartitionMap takes a topic name. If the topic exists, the state of
// the topic is fetched and returned as a *PartitionMap.
func (z *ZKHandler) GetPartitionMap(t string) (*PartitionMap, error) {
	// Get current topic state.
	ts, err := z.GetTopicState(t)
	if err != nil {
		return nil, err
	}

	// Get current reassign_partitions.
	re := z.GetReassignments()

	// Update with partitions in reassignment.
	// We might have this in /admin/reassign_partitions:
	// {"version":1,"partitions":[{"topic":"myTopic","partition":14,"replicas":[1039,1044]}]}
	// But retrieved this in /brokers/topics/myTopic:
	// {"version":1,"partitions":{"14":[1039,1044,1041,1071]}}.
	// The latter will be in ts if we're undergoing a partition move, so
	// but we need to overwrite it with what's intended (the former).
	if re[t] != nil {
		for p, replicas := range re[t] {
			pn := strconv.Itoa(p)
			ts.Partitions[pn] = replicas
		}
	}

	// Map TopicState to a
	// PartitionMap.
	pm := NewPartitionMap()
	pl := PartitionList{}

	for partition, replicas := range ts.Partitions {
		i, _ := strconv.Atoi(partition)
		pl = append(pl, Partition{
			Topic:     t,
			Partition: i,
			Replicas:  replicas,
		})
	}
	pm.Partitions = pl

	sort.Sort(pm.Partitions)

	return pm, nil
}

// UpdateKafkaConfig takes a KafkaConfig with key value pairs of
// entity config. If the config is changed, a persistent sequential
// znode is also written to propagate changes (via watches) to all
// Kafka brokers. This is a Kafka specific behavior; further references
// are available from the Kafka codebase. A bool is returned indicating
// whether the config was changed (if a config is updated to the existing
// value, 'false' is returned) along with any errors encountered. If a
// config value is set to an empty string (""), the entire config key
// itself is deleted. This was a convenient way to combine update/delete
// into a single func.
func (z *ZKHandler) UpdateKafkaConfig(c KafkaConfig) (bool, error) {
	if _, valid := validKafkaConfigTypes[c.Type]; !valid {
		return false, ErrInvalidKafkaConfigType
	}

	// Get current config from the
	// appropriate path.
	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/config/%ss/%s", z.Prefix, c.Type, c.Name)
	} else {
		path = fmt.Sprintf("/config/%ss/%s", c.Type, c.Name)
	}

	var config KafkaConfigData

	data, err := z.Get(path)
	if err != nil {
		// The path may be missing if the broker/topic
		// has never had a configuration applied.
		// This has only been observed for newly added
		// brokers. Uncertain under what circumstance
		// a topic config path wouldn't exist.
		switch err.(type) {
		case ErrNoNode:
			config = NewKafkaConfigData()
			// XXX Kafka version switch here.
			config.Version = 1
			d, _ := json.Marshal(config)
			if err := z.Create(path, string(d)); err != nil {
				return false, err
			}
			// Unset this error.
			err = nil
		default:
			return false, err
		}
	} else {
		config = NewKafkaConfigData()
		json.Unmarshal(data, &config)
	}

	// Populate configs.
	var changed bool
	for _, kv := range c.Configs {
		// If the config is value is diff,
		// set and flip the changed var.
		if config.Config[kv[0]] != kv[1] {
			changed = true
			// If the string is empty, we
			// delete the config.
			if kv[1] == "" {
				delete(config.Config, kv[0])
			} else {
				config.Config[kv[0]] = kv[1]
			}
		}
	}

	// Write the config back if it's different from
	// what was already set.
	if changed {
		newConfig, err := json.Marshal(config)
		if err != nil {
			return false, fmt.Errorf("Error marshalling config: %s", err)
		}
		_, err = z.client.Set(path, newConfig, -1)
		if err != nil {
			return false, err
		}
	} else {
		// Return if there's no change.
		// No need to write back the config.
		return false, err
	}

	// If there were any config changes, write a change
	// notification at /config/changes/config_change_<seq>.
	cpath := "/config/changes/config_change_"
	if z.Prefix != "" {
		cpath = "/" + z.Prefix + cpath
	}

	cdata := fmt.Sprintf(`{"version":2,"entity_path":"%ss/%s"}`, c.Type, c.Name)
	err = z.CreateSequential(cpath, cdata)
	if err != nil {
		// If we're here, this would actually be a partial
		// write since the config was updated but we're
		// failing at the watch entry.
		return false, err
	}

	return true, nil
}

// uncompress takes a []byte and attempts to uncompress it as gzip.
// The uncompressed data and a bool that indicates whether the data
// was compressed is returned.
func uncompress(b []byte) ([]byte, bool) {
	zr, err := gzip.NewReader(bytes.NewReader(b))
	if err == nil {
		defer zr.Close()
		var out bytes.Buffer

		if _, err := io.Copy(&out, zr); err == nil {
			return out.Bytes(), true
		}
	}

	return nil, false
}
