package kafkazk

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/docker/libkv"
	"github.com/docker/libkv/store"
	"github.com/docker/libkv/store/zookeeper"
	zk "github.com/samuel/go-zookeeper/zk"
)

var (
	ErrZKConn                 = errors.New("Error connecting to ZooKeeper")
	ErrInvalidKafkaConfigType = errors.New("Invalid Kafka config type")
)

type ZK struct {
	client  store.Store
	rclient *zk.Conn
	Connect string
	Prefix  string
}

type ZKConfig struct {
	Connect string
	Prefix  string
}

type ZKHandler interface {
	GetReassignments() Reassignments
	GetTopics([]*regexp.Regexp) ([]string, error)
	GetTopicConfig(string) (*TopicConfig, error)
	GetAllBrokerMeta() (BrokerMetaMap, error)
	getPartitionMap(string) (*PartitionMap, error)
}

func init() {
	zookeeper.Register()
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

// topicState is used for unmarshing
// ZooKeeper json data from a topic:
// e.g. `get /brokers/topics/some-topic`.
type topicState struct {
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
// /config/topics/<topic> data.
type TopicConfig struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

func NewZK(c *ZKConfig) (*ZK, error) {
	z := &ZK{
		Connect: c.Connect,
		Prefix:  c.Prefix,
	}

	var err error
	z.client, err = libkv.NewStore(
		store.ZK,
		[]string{z.Connect},
		&store.Config{
		// DisableLogging: true, // Pending merge upstream.
		},
	)

	if err != nil {
		return nil, err
	}

	return z, nil
}

func (z *ZK) Close() {
	z.client.Close()
}

func (z *ZK) GetReassignments() Reassignments {
	reassigns := Reassignments{}

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("%s/admin/reassign_partitions", z.Prefix)
	} else {
		path = "admin/reassign_partitions"
	}

	// Get reassignment config.
	c, err := z.client.Get(path)
	if err != nil {
		return reassigns
	}

	rec := &reassignPartitions{}
	json.Unmarshal(c.Value, rec)

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

func (z *ZK) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	matchingTopics := []string{}

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("%s/brokers/topics", z.Prefix)
	} else {
		path = "brokers/topics"
	}

	// Find all topics in z.
	entries, err := z.client.List(path)
	if err != nil {
		return nil, err
	}

	matched := map[string]bool{}
	// Get all topics that match all
	// provided topic regexps.
	for _, topicRe := range ts {
		for _, topic := range entries {
			if topicRe.MatchString(topic.Key) {
				matched[topic.Key] = true
			}
		}
	}

	// Add matches to a slice.
	for topic := range matched {
		matchingTopics = append(matchingTopics, topic)
	}

	return matchingTopics, nil
}

func (z *ZK) GetTopicConfig(t string) (*TopicConfig, error) {
	config := &TopicConfig{}

	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("%s/config/topics/%s", z.Prefix, t)
	} else {
		path = fmt.Sprintf("config/topics/%s", t)
	}

	// Get topic config.
	c, err := z.client.Get(path)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(c.Value, config)

	return config, nil
}

func (z *ZK) GetAllBrokerMeta() (BrokerMetaMap, error) {
	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("%s/brokers/ids", z.Prefix)
	} else {
		path = "brokers/ids"
	}

	// Get all brokers.
	entries, err := z.client.List(path)
	if err != nil {
		if err.Error() == "Key not found in store" {
			return nil, errors.New("No brokers registered")
		}
		return nil, err
	}

	bmm := BrokerMetaMap{}

	// Map each broker.
	for _, pair := range entries {
		bm := &BrokerMeta{}
		// In case we encounter non-ints
		// (broker IDs) for whatever reason,
		// just continue.
		bid, err := strconv.Atoi(pair.Key)
		if err != nil {
			continue
		}

		// Same with unmarshalling json meta.
		err = json.Unmarshal(pair.Value, bm)
		if err != nil {
			continue
		}

		bmm[bid] = bm
	}

	return bmm, nil
}

func (z *ZK) getPartitionMap(t string) (*PartitionMap, error) {
	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("%s/brokers/topics/%s", z.Prefix, t)
	} else {
		path = fmt.Sprintf("brokers/topics/%s", t)
	}

	// Get current reassign_partitions.
	re := z.GetReassignments()

	// Fetch topic data from z.
	ts := &topicState{}
	m, err := z.client.Get(path)
	switch err {
	case store.ErrKeyNotFound:
		return nil, fmt.Errorf("Topic %s not found in ZooKeeper", t)
	case nil:
		break
	default:
		return nil, err
	}

	err = json.Unmarshal(m.Value, ts)
	if err != nil {
		return nil, err
	}

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

	// Map topicState to a
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

	return pm, nil
}

type KafkaConfig struct {
	Type    string      // Topic or broker.
	Name    string      // Entity name.
	Configs [][2]string // Slice of [2]string{key,value} configs.

}

type KafkaConfigData struct {
	Version int               `json:"version"`
	Config  map[string]string `json:"config"`
}

var validKafkaConfigTypes = map[string]interface{}{
	"broker": nil,
	"topic":  nil,
}

func (z *ZK) UpdateKafkaConfig(c KafkaConfig) error {
	if _, valid := validKafkaConfigTypes[c.Type]; !valid {
		return ErrInvalidKafkaConfigType
	}

	err := z.InitRawClient()
	if err != nil {
		return ErrZKConn
	}

	// Get current config from the
	// appropriate path.
	var path string
	if z.Prefix != "" {
		path = fmt.Sprintf("/%s/config/%ss/%c", z.Prefix, c.Type, c.Name)
	} else {
		path = fmt.Sprintf("/config/%ss/%s", c.Type, c.Name)
	}

	data, _, err := z.rclient.Get(path)
	if err != nil {
		return err
	}

	config := &KafkaConfigData{}
	json.Unmarshal(data, &config)

	// Populate configs.
	var changed bool
	for _, kv := range c.Configs {
		// If the config is value is diff,
		// set and flip the changed var.
		if config.Config[kv[0]] != kv[1] {
			changed = true
			config.Config[kv[0]] = kv[1]
		}
	}

	// Write the config back.
	if changed {
		newConfig, err := json.Marshal(config)
		if err != nil {
			errS := fmt.Sprintf("Error marshalling config: %s", err)
			return errors.New(errS)
		}
		_, err = z.rclient.Set(path, newConfig, -1)
		if err != nil {
			return err
		}
	}

	// If there were any config changes,
	// write a change notification at
	// /config/changes/config_change_<seq>.
	cpath := "/config/changes/config_change_"
	if z.Prefix != "" {
		cpath = "/" + z.Prefix + cpath
	}

	if changed {
		cdata := fmt.Sprintf(`{"version":2,"entity_path":"%ss/%s"}`, c.Type, c.Name)
		_, err := z.rclient.Create(cpath, []byte(cdata), zk.FlagSequence, zk.WorldACL(31))
		if err != nil {
			return err
		}
	}

	return nil
}

func (z *ZK) InitRawClient() error {
	if z.rclient == nil {
		c, _, err := zk.Connect(
			[]string{z.Connect}, 10*time.Second, zk.WithLogInfo(false))
		if err != nil {
			return err
		}
		z.rclient = c
	}

	return nil
}
