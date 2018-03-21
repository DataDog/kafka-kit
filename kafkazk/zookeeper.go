package kafkazk

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"time"

	zkclient "github.com/samuel/go-zookeeper/zk"
)

var (
	// ErrInvalidKafkaConfigType indicates invalid Kafka config types.
	ErrInvalidKafkaConfigType = errors.New("Invalid Kafka config type")
	// validKafkaConfigTypes is used as a set
	// to define valid configuration type names.
	validKafkaConfigTypes = map[string]interface{}{
		"broker": nil,
		"topic":  nil,
	}
)

// Handler exposes basic ZooKeeper operations
// along with additional methods that return
// kafkazk package specific types, populated
// with data fetched from ZooKeeper.
type Handler interface {
	Exists(string) (bool, error)
	Create(string, string) error
	CreateSequential(string, string) error
	Set(string, string) error
	Get(string) ([]byte, error)
	Close()
	GetTopicState(string) (*TopicState, error)
	UpdateKafkaConfig(KafkaConfig) (bool, error)
	GetReassignments() Reassignments
	GetTopics([]*regexp.Regexp) ([]string, error)
	GetTopicConfig(string) (*TopicConfig, error)
	GetAllBrokerMeta(bool) (BrokerMetaMap, error)
	GetAllPartitionMeta() (PartitionMetaMap, error)
	GetPartitionMap(string) (*PartitionMap, error)
}

// BrokerMeta holds metadata that
// describes a broker, used in satisfying
// constraints.
type BrokerMeta struct {
	Rack string `json:"rack"`
}

// BrokerMetaMap is a map of broker IDs
// to BrokerMeta metadata fetched from
// ZooKeeper. Currently, just the rack
// field is retrieved.
type BrokerMetaMap map[int]*BrokerMeta

// TopicState is used for unmarshing
// ZooKeeper json data from a topic:
// e.g. `get /brokers/topics/some-topic`.
type TopicState struct {
	Partitions map[string][]int `json:"partitions"`
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

// KafkaConfig is used to issue configuration
// updates to either topics or brokers in
// ZooKeeper.
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

// zkHandler implements the Handler interface
// for real ZooKeeper clusters.
type zkHandler struct {
	client  *zkclient.Conn
	Connect string
	Prefix  string
}

// Config holds initialization
// paramaters for a Handler. Connect
// is a ZooKeeper connect string.
// Prefix should reflect any prefix
// used for Kafka on the reference
// ZooKeeper cluster (excluding slashes).
type Config struct {
	Connect string
	Prefix  string
}

// NewHandler takes a *Config, performs
// any initialization and returns a Handler.
func NewHandler(c *Config) (Handler, error) {
	z := &zkHandler{
		Connect: c.Connect,
		Prefix:  c.Prefix,
	}

	var err error
	z.client, _, err = zkclient.Connect([]string{z.Connect}, 10*time.Second, zkclient.WithLogInfo(false))
	if err != nil {
		return nil, err
	}

	return z, nil
}

// Close calls close on
// the *zkHandler. Any additional
// shutdown cleanup or other
// tasks should be performed here.
func (z *zkHandler) Close() {
	z.client.Close()
}

// Get gets the provided path p and returns
// the data from the path and an error if encountered.
func (z *zkHandler) Get(p string) ([]byte, error) {
	r, _, err := z.client.Get(p)
	return r, err
}

// Set sets the provided path p data to the
// provided string d and returns an error
// if encountered.
func (z *zkHandler) Set(p string, d string) error {
	_, err := z.client.Set(p, []byte(d), -1)
	return err
}

// CreateSequential takes a path p and data d and
// creates a sequential znode at p with data d.
// An error is returned if encountered.
func (z *zkHandler) CreateSequential(p string, d string) error {
	_, err := z.client.Create(p, []byte(d), zkclient.FlagSequence, zkclient.WorldACL(31))
	return err
}

// Create creates the provided path p with the data
// from the provided string d and returns an error
// if encountered.
func (z *zkHandler) Create(p string, d string) error {
	_, err := z.client.Create(p, []byte(d), 0, zkclient.WorldACL(31))
	return err
}

// Exists takes a path p and returns a bool
// as to whether the path exists and an error
// if encountered.
func (z *zkHandler) Exists(p string) (bool, error) {
	e, _, err := z.client.Exists(p)
	return e, err
}

// GetReassignments looks up any ongoing
// topic reassignments and returns the data
// as a Reassignments type.
func (z *zkHandler) GetReassignments() Reassignments {
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

// GetTopics takes a []*regexp.Regexp and returns
// a []string of all topic names that match any of the
// provided regex.
func (z *zkHandler) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	matchingTopics := []string{}

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/topics", z.Prefix)
	} else {
		path = "/brokers/topics"
	}

	// Find all topics in zk.
	entries, _, err := z.client.Children(path)
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

// GetTopicConfig takes a topic name. If the
// topic exists, the topic config is returned
// as a *TopicConfig.
func (z *zkHandler) GetTopicConfig(t string) (*TopicConfig, error) {
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

// GetAllBrokerMeta looks up all registered Kafka
// brokers and returns their metadata as a BrokerMetaMap.
// An withMetrics bool param determines whether we additionally
// want to fetch stored broker metrics.
func (z *zkHandler) GetAllBrokerMeta(withMetrics bool) (BrokerMetaMap, error) {
	_ = withMetrics

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/brokers/ids", z.Prefix)
	} else {
		path = "/brokers/ids"
	}

	// Get all brokers.
	entries, _, err := z.client.Children(path)
	if err != nil {
		return nil, err
	}

	bmm := BrokerMetaMap{}

	// Map each broker.
	for _, b := range entries {
		bm := &BrokerMeta{}
		// In case we encounter non-ints
		// (broker IDs) for whatever reason,
		// just continue.
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

	return bmm, nil
}

// GetAllPartitionMeta
func (z *zkHandler) GetAllPartitionMeta() (PartitionMetaMap, error) {
	return nil, nil
}

// GetTopicState takes a topic name. If the topic exists,
// the topic state is returned as a *TopicState.
func (z *zkHandler) GetTopicState(t string) (*TopicState, error) {
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
	/* Error handling pattern with
	// previous client.
	switch err {
	case store.ErrKeyNotFound:
		return nil, fmt.Errorf("Topic %s not found in ZooKeeper", t)
	case nil:
		break
	default:
		return nil, err
	}
	*/

	err = json.Unmarshal(data, ts)
	if err != nil {
		return nil, err
	}

	return ts, nil
}

// GetPartitionMap takes a topic name. If the topic
// exists, the state of the topic is fetched and
// translated into a *PartitionMap.
func (z *zkHandler) GetPartitionMap(t string) (*PartitionMap, error) {
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
	pl := partitionList{}

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

// UpdateKafkaConfig takes a KafkaConfig with key
// value pairs of entity config. If the config is changed,
// a persistent sequential znode is also written to
// propagate changes (via watches) to all Kafka brokers.
// This is a Kafka specific behavior; further references
// are available from the Kafka codebase. A bool is returned
// indicating whether the config was changed (if a config is
// updated to the existing value, 'false' is returned) along
// with any errors encountered.
// If a config value is set to an empty string (""),
// the entire config key itself is deleted. This was
// an easy way to merge update/delete into a single func.
func (z *zkHandler) UpdateKafkaConfig(c KafkaConfig) (bool, error) {
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

	data, err := z.Get(path)
	if err != nil {
		return false, err
	}

	config := NewKafkaConfigData()
	json.Unmarshal(data, &config)

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

	// Write the config back
	// if it's different from
	// what was already set.
	if changed {
		newConfig, err := json.Marshal(config)
		if err != nil {
			errS := fmt.Sprintf("Error marshalling config: %s", err)
			return false, errors.New(errS)
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

	// If there were any config changes,
	// write a change notification at
	// /config/changes/config_change_<seq>.
	cpath := "/config/changes/config_change_"
	if z.Prefix != "" {
		cpath = "/" + z.Prefix + cpath
	}

	cdata := fmt.Sprintf(`{"version":2,"entity_path":"%ss/%s"}`, c.Type, c.Name)
	err = z.CreateSequential(cpath, cdata)
	if err != nil {
		// If we're here, this would
		// actually be a partial write since
		// the config was updated but we're
		// failing at the watch entry.
		return false, err
	}

	return true, nil
}
