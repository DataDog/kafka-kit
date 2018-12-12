package server

import (
	"fmt"
	"reflect"
	"strings"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

// TagHandler provides object filtering by tags.
type TagHandler interface {
	FilterTopics(map[string]*pb.Topic, tags) (map[string]*pb.Topic, error)
	FilterBrokers(map[uint32]*pb.Broker, tags) (map[uint32]*pb.Broker, error)
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

func (t tagSet) matchAll(kv tagSet) bool {
	for k, v := range kv {
		if t[k] != v {
			return false
		}
	}

	return true
}

// FilterTopics takes a map of topic names to *pb.Topic and tags KV list.
// A filtered map is returned that includes topics where all tags
// values match the provided input tag KVs.
func (t *tagHandler) FilterTopics(in map[string]*pb.Topic, tags tags) (map[string]*pb.Topic, error) {
	if len(tags) == 0 {
		return in, nil
	}

	var out = make(map[string]*pb.Topic)

	// Get tag key/values.
	tagKV := tagSet{}

	for _, t := range tags {
		ts := strings.Split(t, ":")
		if len(ts) != 2 {
			return nil, fmt.Errorf("invalid tag: %s", t)
		}

		tagKV[ts[0]] = ts[1]
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

// FilterBrokers TODO DRY this.
func (t *tagHandler) FilterBrokers(in map[uint32]*pb.Broker, tags tags) (map[uint32]*pb.Broker, error) {
	if len(tags) == 0 {
		return in, nil
	}

	var out = make(map[uint32]*pb.Broker)

	// Get tag key/values.
	tagKV := tagSet{}

	for _, t := range tags {
		ts := strings.Split(t, ":")
		if len(ts) != 2 {
			return nil, fmt.Errorf("invalid tag: %s", t)
		}

		tagKV[ts[0]] = ts[1]
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
