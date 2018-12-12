package server

import (
	"fmt"
	"reflect"
	"strings"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

// TagHandler provides object filtering by tags.
type TagHandler interface {
	FilterTopics(TopicSet, tags) (TopicSet, error)
	FilterBrokers(BrokerSet, tags) (BrokerSet, error)
}

// NewTagHandler initializes a TagHandler.
func NewTagHandler() TagHandler {
	return &tagHandler{
		restrictedFields: restrictedFields(),
	}
}

type tagHandler struct {
	// Mapping of type (broker, topic) to restricted fields.
	restrictedFields map[string]map[string]struct{}
}

type tags []string
type tagSet map[string]string

func tagSetFromObject(o interface{}) tagSet {
	var ts = tagSet{}

	switch o.(type) {
	case *pb.Topic:
		t := o.(*pb.Topic)
		ts["name"] = t.Name
		ts["partitions"] = fmt.Sprintf("%d", t.Partitions)
		ts["replication"] = fmt.Sprintf("%d", t.Replication)
	case *pb.Broker:
		b := o.(*pb.Broker)
		// TODO deal with map types.
		// ts["listenersecurityprotocolmap"] = b.ListenerSecurityProtocolMap
		ts["rack"] = b.Rack
		ts["jmxport"] = fmt.Sprintf("%d", b.Jmxport)
		ts["host"] = b.Host
		ts["timestamp"] = fmt.Sprintf("%d", b.Timestamp)
		ts["port"] = fmt.Sprintf("%d", b.Port)
		ts["version"] = fmt.Sprintf("%d", b.Version)
	}

	return ts
}

// matchAll takes a tagSet and returns true
// if all key/values are present and equal
// to those in the calling tagSet.
func (t tagSet) matchAll(kv tagSet) bool {
	for k, v := range kv {
		if t[k] != v {
			return false
		}
	}

	return true
}

// tagSet takes a tags and returns a tagSet
// and error for any malformed tags. Tags are
// expected to be formatted as "key:value" strings.
func (t tags) tagSet() (tagSet, error) {
	var ts = tagSet{}

	for _, tag := range t {
		kv := strings.Split(tag, ":")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid tag: %s", t)
		}

		ts[kv[0]] = kv[1]
	}

	return ts, nil
}

// FilterTopics takes a map of topic names to *pb.Topic and tags KV list.
// A filtered map is returned that includes topics where all tags
// values match the provided input tag KVs.
func (t *tagHandler) FilterTopics(in TopicSet, tags tags) (TopicSet, error) {
	if len(tags) == 0 {
		return in, nil
	}

	var out = make(TopicSet)

	// Get tag key/values.
	tagKV, err := tags.tagSet()
	if err != nil {
		return nil, err
	}

	// Filter input topics.
	for name, topic := range in {
		ts := tagSetFromObject(topic)
		if ts.matchAll(tagKV) {
			out[name] = topic
		}
	}

	return out, nil
}

// FilterBrokers takes a map of broker IDs to *pb.Broker and tags KV list.
// A filtered map is returned that includes brokers where all tags
// values match the provided input tag KVs.
func (t *tagHandler) FilterBrokers(in BrokerSet, tags tags) (BrokerSet, error) {
	if len(tags) == 0 {
		return in, nil
	}

	var out = make(BrokerSet)

	// Get tag key/values.
	tagKV, err := tags.tagSet()
	if err != nil {
		return nil, err
	}

	// Filter input brokers.
	for id, broker := range in {
		ts := tagSetFromObject(broker)
		if ts.matchAll(tagKV) {
			out[id] = broker
		}
	}

	return out, nil
}

// restrictedFields returns a map proto message types to field names
// considered reserved for internal use. All fields specified in the
// Registry proto messages are discovered here and restricted by default.
func restrictedFields() map[string]map[string]struct{} {
	var fs = make(map[string]map[string]struct{})

	fs["topic"] = fieldsFromStruct(&pb.Topic{})
	fs["broker"] = fieldsFromStruct(&pb.Broker{})

	return fs
}

// fieldsFromStruct extracts all user-defined fields from proto
// messages. Discovered fields are returned all lowercase.
func fieldsFromStruct(s interface{}) map[string]struct{} {
	var fs = make(map[string]struct{})

	// Iterate over fields.
	v := reflect.ValueOf(s).Elem()
	for i := 0; i < v.NumField(); i++ {
		// Exclude proto generated fields.
		if !strings.HasPrefix(v.Type().Field(i).Name, "XXX") {
			f := strings.ToLower(v.Type().Field(i).Name)
			fs[f] = struct{}{}
		}
	}

	return fs
}
