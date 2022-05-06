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
	"strings"
	"time"

	"github.com/DataDog/kafka-kit/v3/mapper"

	zkclient "github.com/go-zookeeper/zk"
)

// Handler specifies an interface for common Kafka metadata retrieval and
// configuration methods.
type Handler interface {
	SimpleZooKeeperClient
	GetTopicState(string) (*mapper.TopicState, error)
	GetTopicStateISR(string) (TopicStateISR, error)
	UpdateKafkaConfig(KafkaConfig) ([]bool, error)
	GetReassignments() Reassignments
	ListReassignments() (Reassignments, error)
	GetUnderReplicated() ([]string, error)
	GetPendingDeletion() ([]string, error)
	GetTopics([]*regexp.Regexp) ([]string, error)
	GetTopicConfig(string) (*TopicConfig, error)
	GetTopicMetadata(string) (TopicMetadata, error)
	GetAllBrokerMeta(bool) (mapper.BrokerMetaMap, []error)
	GetAllPartitionMeta() (mapper.PartitionMetaMap, error)
	MaxMetaAge() (time.Duration, error)
	GetPartitionMap(string) (*mapper.PartitionMap, error)
}

// SimpleZooKeeperClient is an interface that wraps a real ZooKeeper client,
// obscuring much of the API semantics that are unneeded for a ZooKeeper based
// Handler implementation.
type SimpleZooKeeperClient interface {
	Exists(string) (bool, error)
	Create(string, string) error
	CreateSequential(string, string) error
	Set(string, string) error
	Get(string) ([]byte, error)
	Delete(string) error
	Children(string) ([]string, error)
	NextInt(string) (int32, error)
	Close()
	Ready() bool
}

// ZKHandler implements the Handler interface for real ZooKeeper clusters.
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

// NewHandler takes a *Config, performs any initialization and returns a Handler.
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

// Ready returns true if the client is in either state StateConnected or
// StateHasSession. See https://godoc.org/github.com/go-zookeeper/zk#State.
func (z *ZKHandler) Ready() bool {
	switch z.client.State() {
	case 100, 101:
		return true
	default:
		return false
	}
}

// Close calls close on the *ZKHandler. Any additional shutdown cleanup or
// other tasks should be performed here.
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

// CreateSequential takes a path p and data d and creates a sequential znode at
// p with data d. An error is returned if encountered.
func (z *ZKHandler) CreateSequential(p string, d string) error {
	_, e := z.client.Create(p, []byte(d), zkclient.FlagSequence, zkclient.WorldACL(31))
	var err error
	if e != nil {
		err = fmt.Errorf("[%s] %s", p, e.Error())
	}

	return err
}

// Create creates the provided path p with the data from the provided string d
// and returns an error if encountered.
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

// Exists takes a path p and returns a bool as to whether the path exists and
// an error if encountered.
func (z *ZKHandler) Exists(p string) (bool, error) {
	b, _, e := z.client.Exists(p)
	var err error
	if e != nil {
		err = fmt.Errorf("[%s] %s", p, e.Error())
	}

	return b, err
}

// Children takes a path p and returns a list of child znodes and an error
// if encountered.
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

// NextInt works as an atomic int generator. It does this by setting nil value
// to path p and returns the znode version.
func (z *ZKHandler) NextInt(p string) (int32, error) {
	s, err := z.client.Set(p, []byte{}, -1)
	if err != nil {
		return 0, err
	}

	return s.Version, nil
}

// GetReassignments looks up any ongoing topic reassignments and returns the
// data as a Reassignments.
func (z *ZKHandler) GetReassignments() Reassignments {
	reassigns := Reassignments{}
	path := z.getPath("/admin/reassign_partitions")

	// Get reassignment config.
	data, err := z.Get(path)
	if err != nil {
		return reassigns
	}

	rec := &reassignPartitions{}
	json.Unmarshal(data, rec)

	// Map reassignment config to a Reassignments.
	for _, cfg := range rec.Partitions {
		if reassigns[cfg.Topic] == nil {
			reassigns[cfg.Topic] = map[int][]int{}
		}
		reassigns[cfg.Topic][cfg.Partition] = cfg.Replicas
	}

	return reassigns
}

// ListReassignments looks up any ongoing topic reassignments and returns the data
// as a Reassignments. ListReassignments is a KIP-455 compatible call for Kafka
// 2.4 and Kafka cli tools 2.6.
func (z *ZKHandler) ListReassignments() (Reassignments, error) {
	reassignments := Reassignments{}

	// Get a topic list.
	topics, err := z.GetTopics([]*regexp.Regexp{regexp.MustCompile(".*")})
	if err != nil {
		return nil, err
	}

	// Get the current topic configuration for each topic.
	for _, topic := range topics {
		// Fetch the metadata.
		topicMetadata, err := z.GetTopicMetadata(topic)
		if err != nil {
			return reassignments, err
		}
		// Get a Reassignments output from the metadata.
		topicReassignment := topicMetadata.Reassignments()
		// Populate it into the parent reassignments.
		if len(topicReassignment[topic]) > 0 {
			reassignments[topic] = topicReassignment[topic]
		}
	}

	return reassignments, nil
}

// GetPendingDeletion returns any topics pending deletion.
func (z *ZKHandler) GetPendingDeletion() ([]string, error) {
	path := z.getPath("/admin/delete_topics")

	// Get reassignment config.
	p, err := z.Children(path)
	if err != nil {
		switch err.(type) {
		case ErrNoNode:
			return []string{}, nil
		default:
			return nil, err
		}
	}

	return p, nil
}

// GetTopics takes a []*regexp.Regexp and returns a []string of all topic names
// that match any of the provided regex.
func (z *ZKHandler) GetTopics(ts []*regexp.Regexp) ([]string, error) {
	matchingTopics := []string{}
	path := z.getPath("/brokers/topics")

	// Find all topics in zk.
	entries, err := z.Children(path)
	if err != nil {
		return nil, err
	}

	matched := map[string]bool{}
	// Get all topics that match all provided topic regexps.
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

// GetTopicMetadata takes a topic name. If the topic exists, the topic metadata
// is returned as a TopicMetadata.
func (z *ZKHandler) GetTopicMetadata(t string) (TopicMetadata, error) {
	topicMetadata := TopicMetadata{}

	if t == "" {
		return topicMetadata, fmt.Errorf("unspecified topic")
	}

	path := z.getPath("/brokers/topics/" + t)

	// Get the metadata.
	data, err := z.Get(path)
	if err != nil {
		return topicMetadata, err
	}

	// Deserialized.
	if err := json.Unmarshal(data, &topicMetadata); err != nil {
		return topicMetadata, err
	}

	// We have to append the name since it's not part of the metadata.
	topicMetadata.Name = t

	return topicMetadata, nil
}

// GetTopicConfig takes a topic name. If the topic exists, the topic config
// is returned as a *TopicConfig.
func (z *ZKHandler) GetTopicConfig(t string) (*TopicConfig, error) {
	config := &TopicConfig{}
	path := z.getPath("/config/topics/" + t)

	// Get topic config.
	data, err := z.Get(path)
	if err != nil {
		return nil, err
	}

	json.Unmarshal(data, config)

	return config, nil
}

// GetAllBrokerMeta looks up all registered Kafka brokers and returns their
// metadata as a mapper.BrokerMetaMap. A withMetrics bool param determines whether
// we additionally want to fetch stored broker metrics.
func (z *ZKHandler) GetAllBrokerMeta(withMetrics bool) (mapper.BrokerMetaMap, []error) {
	var errs []error
	path := z.getPath("/brokers/ids")

	// Get all brokers.
	entries, err := z.Children(path)
	if err != nil {
		return nil, []error{err}
	}

	bmm := mapper.BrokerMetaMap{}

	// Map each broker.
	for _, b := range entries {
		bm := &mapper.BrokerMeta{}
		// In case we encounter non-ints (broker IDs) for whatever reason, just
		// continue.
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

		// Populate each broker with metric data.
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

// GetBrokerMetrics fetches broker metrics stored in ZooKeeper and returns a
// BrokerMetricsMap and an error if encountered.
func (z *ZKHandler) getBrokerMetrics() (mapper.BrokerMetricsMap, error) {
	path := z.getMetricsPath("/brokermetrics")

	// Fetch the metrics object.
	data, err := z.Get(path)
	if err != nil {
		return nil, fmt.Errorf("Error fetching broker metrics: %s", err.Error())
	}

	// Check if the data is compressed.
	if out, compressed := uncompress(data); compressed {
		data = out
	}

	bmm := mapper.BrokerMetricsMap{}
	err = json.Unmarshal(data, &bmm)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling broker metrics: %s", err.Error())
	}

	return bmm, nil
}

// GetAllPartitionMeta fetches partition metadata stored in Zookeeper.
func (z *ZKHandler) GetAllPartitionMeta() (mapper.PartitionMetaMap, error) {
	path := z.getMetricsPath("/partitionmeta")

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

	pmm := mapper.NewPartitionMetaMap()
	err = json.Unmarshal(data, &pmm)
	if err != nil {
		return nil, fmt.Errorf("Error unmarshalling partition meta: %s", err.Error())
	}

	return pmm, nil
}

// MaxMetaAge returns the greatest age between the partitionmeta and
// brokermetrics stuctures.
func (z *ZKHandler) MaxMetaAge() (time.Duration, error) {
	t, err := z.oldestMetaTs()
	if err != nil {
		return time.Nanosecond, err
	}

	return time.Since(time.Unix(0, t)), nil
}

// oldestMetaTs returns returns the oldest unix epoch ns between partitionmeta
// and brokermetrics stuctures.
func (z *ZKHandler) oldestMetaTs() (int64, error) {
	var paths []string

	for _, p := range []string{"partitionmeta", "brokermetrics"} {
		path := z.getMetricsPath("/" + p)

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

// GetTopicState takes a topic name. If the topic exists,  the topic state is
// returned as a *mapper.TopicState.
func (z *ZKHandler) GetTopicState(t string) (*mapper.TopicState, error) {
	path := z.getPath("/brokers/topics/" + t)

	// Fetch topic data from z.
	ts := &mapper.TopicState{}
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

// GetUnderReplicated returns a []string of all under-replicated topics.
func (z *ZKHandler) GetUnderReplicated() ([]string, error) {
	var underReplicated []string

	// Get a list of all topics.
	topics, err := z.GetTopics([]*regexp.Regexp{allTopicsRegexp})
	if err != nil {
		return underReplicated, err
	}

	// For each topic, compare the configured replica set against the ISR.
	for _, topic := range topics {
		configuredState, err := z.GetTopicState(topic)
		if err != nil {
			return underReplicated, err
		}

		currentState, err := z.GetTopicStateISR(topic)
		if err != nil {
			return underReplicated, err
		}

		// Compare.
		for partn, replicaSet := range configuredState.Partitions {
			state, ok := currentState[partn]
			if !ok {
				return underReplicated, fmt.Errorf("Inconsistent configuration and ISR state for %s", topic)
			}

			// It's cheaper/faster to compare the lengths; there's no known scenario
			// where a configured replica set would be [a,b,c] but the ISR is [a,b,d].
			if len(replicaSet) != len(state.ISR) {
				underReplicated = append(underReplicated, topic)
				break
			}
		}
	}

	return underReplicated, nil
}

// GetTopicStateISR takes a topic name. If the topic exists, the topic state
// is returned as a TopicStateISR. GetTopicStateCurrentISR differs from
// GetTopicState in that the actual, current broker IDs in the ISR are
// returned for each partition. This method is more expensive due to the
// need for a call per partition to ZK.
func (z *ZKHandler) GetTopicStateISR(t string) (TopicStateISR, error) {
	path := z.getPath(fmt.Sprintf("/brokers/topics/%s/partitions", t))

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

// GetPartitionMap takes a topic name. If the topic exists, the state of the
// topic is fetched and returned as a *PartitionMap.
func (z *ZKHandler) GetPartitionMap(t string) (*mapper.PartitionMap, error) {
	// Get current topic state.
	ts, err := z.GetTopicState(t)
	if err != nil {
		return nil, err
	}

	// Get current reassign_partitions.
	re := z.GetReassignments()

	// Update with partitions in reassignment. We might have this in
	// /admin/reassign_partitions:
	// {"version":1,"partitions":[{"topic":"myTopic","partition":14,"replicas":[1039,1044]}]}
	// But retrieved this in /brokers/topics/myTopic:
	// {"version":1,"partitions":{"14":[1039,1044,1041,1071]}}.
	// The latter will be in ts if we're undergoing a partition move, so we
	// need to overwrite it with what's intended (the former).
	if re[t] != nil {
		for p, replicas := range re[t] {
			pn := strconv.Itoa(p)
			ts.Partitions[pn] = replicas
		}
	}

	// Map TopicState to a PartitionMap.
	pm := mapper.NewPartitionMap()
	pl := mapper.PartitionList{}

	for partition, replicas := range ts.Partitions {
		i, _ := strconv.Atoi(partition)
		pl = append(pl, mapper.Partition{
			Topic:     t,
			Partition: i,
			Replicas:  replicas,
		})
	}
	pm.Partitions = pl

	sort.Sort(pm.Partitions)

	return pm, nil
}

// PartitionMapFromZK takes a slice of regexp and finds all matching topics for
// each. A merged *PartitionMap of all matching topic maps is returned.
func PartitionMapFromZK(t []*regexp.Regexp, zk Handler) (*mapper.PartitionMap, error) {
	// Get a list of topic names from Handler
	// matching the provided list.
	topicsToRebuild, err := zk.GetTopics(t)
	if err != nil {
		return nil, err
	}

	// Err if no matching topics were found.
	if len(topicsToRebuild) == 0 {
		return nil, fmt.Errorf("No topics found matching: %s", t)
	}

	// Get a partition map for each topic.
	pmapMerged := mapper.NewPartitionMap()
	for _, t := range topicsToRebuild {
		pmap, err := zk.GetPartitionMap(t)
		if err != nil {
			return nil, err
		}

		// Merge multiple maps.
		pmapMerged.Partitions = append(pmapMerged.Partitions, pmap.Partitions...)
	}

	sort.Sort(pmapMerged.Partitions)

	return pmapMerged, nil
}

// UpdateKafkaConfig takes a KafkaConfig with key value pairs of
// entity config. If the config is changed, a persistent sequential
// znode is also written to propagate changes (via watches) to all
// Kafka brokers. This is a Kafka specific behavior; further references
// are available from the Kafka codebase. A []bool is returned indicating
// whether the config of the respective index was changed (if a config is
// updated to the existing value, 'false' is returned) along with any errors
// encountered. If a config value is set to an empty string (""), the entire
// config key itself is deleted. This was a convenient method to combine
// update/delete into a single func.
func (z *ZKHandler) UpdateKafkaConfig(c KafkaConfig) ([]bool, error) {
	var changed = make([]bool, len(c.Configs))

	if _, valid := validKafkaConfigTypes[c.Type]; !valid {
		return changed, ErrInvalidKafkaConfigType
	}

	// Get current config from the appropriate path.
	path := z.getPath(fmt.Sprintf("/config/%ss/%s", c.Type, c.Name))

	var config KafkaConfigData

	data, err := z.Get(path)
	if err != nil {
		// The path may be missing if the broker/topic has never had a configuration
		// applied. This has only been observed for newly added brokers. It's uncertain
		// under what circumstance a topic config path wouldn't exist.
		switch err.(type) {
		case ErrNoNode:
			config = NewKafkaConfigData()
			// XXX Kafka version switch here.
			config.Version = 1
			d, _ := json.Marshal(config)
			if err := z.Create(path, string(d)); err != nil {
				return changed, err
			}
			// Unset this error.
			err = nil
		default:
			return changed, err
		}
	} else {
		config = NewKafkaConfigData()
		json.Unmarshal(data, &config)
	}

	// Populate configs.
	var anyChanges bool
	for i, kv := range c.Configs {
		// If the config is value is diff, set and flip the changed index.
		if config.Config[kv[0]] != kv[1] {
			changed[i] = true
			anyChanges = true
			// If the string is empty, we delete the config.
			if kv[1] == "" {
				delete(config.Config, kv[0])
			} else {
				config.Config[kv[0]] = kv[1]
			}
		}
	}

	// Write the config back if it's different from what was already set.
	if anyChanges {
		newConfig, err := json.Marshal(config)
		if err != nil {
			return changed, fmt.Errorf("Error marshalling config: %s", err)
		}
		_, err = z.client.Set(path, newConfig, -1)
		if err != nil {
			return changed, err
		}
	} else {
		// Return early if there's no change.
		return changed, err
	}

	// If there were any config changes, write a change notification at
	// /config/changes/config_change_<seq>.
	cpath := "/config/changes/config_change_"
	if z.Prefix != "" {
		cpath = "/" + z.Prefix + cpath
	}

	cdata := fmt.Sprintf(`{"version":2,"entity_path":"%ss/%s"}`, c.Type, c.Name)
	err = z.CreateSequential(cpath, cdata)
	if err != nil {
		// If we're here, this would actually be a partial write since the config
		// was updated but we're failing at the watch entry.
		return changed, err
	}

	return changed, nil
}

func (z *ZKHandler) getPath(p string) string {
	if z.Prefix != "" {
		return fmt.Sprintf("/%s/%s", z.Prefix, strings.TrimLeft(p, "/"))
	}

	return p
}

func (z *ZKHandler) getMetricsPath(p string) string {
	if z.MetricsPrefix != "" {
		return fmt.Sprintf("/%s/%s", z.MetricsPrefix, strings.TrimLeft(p, "/"))
	}

	return p
}

// uncompress takes a []byte and attempts to uncompress it as gzip. The
// uncompressed data and a bool that indicates whether the data was compressed
// is returned.
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
