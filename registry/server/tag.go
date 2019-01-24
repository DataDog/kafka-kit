package server

import (
	"errors"
	"fmt"
	"reflect"
	"strings"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	// ErrInvalidKafkaObjectType error.
	ErrInvalidKafkaObjectType = errors.New("invalid Kafka object type")
)

// ErrReservedTag error.
type ErrReservedTag struct {
	t string
}

func (e ErrReservedTag) Error() string {
	return fmt.Sprintf("tag '%s' is a reserved tag", e.t)
}

// TagHandler provides object filtering by tags
// along with tag storage and retrieval.
type TagHandler struct {
	Store TagStorage
}

// TagStorage handles tag persistence to stable storage.
type TagStorage interface {
	LoadReservedFields(ReservedFields) error
	SetTags(KafkaObject, TagSet) error
}

// NewTagHandler initializes a TagHandler.
func NewTagHandler(c TagHandlerConfig) (*TagHandler, error) {
	ts, err := NewZKTagStorage(ZKTagStorageConfig{Prefix: c.Prefix})
	if err != nil {
		return nil, err
	}

	err = ts.LoadReservedFields(getReservedFields())
	if err != nil {
		return nil, err
	}

	return &TagHandler{
		// More sophisticated initialization/config passing
		// if additional TagStorage backends are written.
		Store: ts,
	}, nil
}

// TagHandlerConfig holds TagHandler configuration.
type TagHandlerConfig struct {
	Prefix string
}

// Tags is a []string of "key:value" pairs.
type Tags []string

// TagSet is a map of key:values.
type TagSet map[string]string

// KafkaObject holds an object type (broker, topic) and
// object identifier (ID, name).
type KafkaObject struct {
	Kind string
	ID   string
}

// Valid checks if a KafkaObject has a valid
// Kind field value.
func (o KafkaObject) Valid() bool {
	switch {
	case o.Kind == "broker", o.Kind == "topic":
		if o.ID != "" {
			return true
		}
	}

	return false
}

// TagSetFromObject takes a protobuf type and
// returns the default TagSet.
func TagSetFromObject(o interface{}) TagSet {
	var ts = TagSet{}

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
		ts["id"] = fmt.Sprintf("%d", b.Id)
		ts["rack"] = b.Rack
		ts["jmxport"] = fmt.Sprintf("%d", b.Jmxport)
		ts["host"] = b.Host
		ts["timestamp"] = fmt.Sprintf("%d", b.Timestamp)
		ts["port"] = fmt.Sprintf("%d", b.Port)
		ts["version"] = fmt.Sprintf("%d", b.Version)
	}

	return ts
}

// matchAll takes a TagSet and returns true
// if all key/values are present and equal
// to those in the input TagSet.
func (t TagSet) matchAll(kv TagSet) bool {
	for k, v := range kv {
		if t[k] != v {
			return false
		}
	}

	return true
}

// TagSet takes a tags and returns a TagSet and error for any
// malformed tags. Tags are expected to be formatted as
// "key:value" strings.
// TODO normalize all tag usage to lower case.
func (t Tags) TagSet() (TagSet, error) {
	var ts = TagSet{}

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
func (t *TagHandler) FilterTopics(in TopicSet, tags Tags) (TopicSet, error) {
	if len(tags) == 0 {
		return in, nil
	}

	var out = make(TopicSet)

	// Get tag key/values.
	tagKV, err := tags.TagSet()
	if err != nil {
		return nil, err
	}

	// Filter input topics.
	for name, topic := range in {
		ts := TagSetFromObject(topic)
		if ts.matchAll(tagKV) {
			out[name] = topic
		}
	}

	return out, nil
}

// FilterBrokers takes a map of broker IDs to *pb.Broker and tags KV list.
// A filtered map is returned that includes brokers where all tags
// values match the provided input tag KVs.
func (t *TagHandler) FilterBrokers(in BrokerSet, tags Tags) (BrokerSet, error) {
	if len(tags) == 0 {
		return in, nil
	}

	var out = make(BrokerSet)

	// Get tag key/values.
	tagKV, err := tags.TagSet()
	if err != nil {
		return nil, err
	}

	// Filter input brokers.
	for id, broker := range in {
		ts := TagSetFromObject(broker)
		if ts.matchAll(tagKV) {
			out[id] = broker
		}
	}

	return out, nil
}

// ReservedFields is a mapping of object types (topic, broker)
// to a set of fields reserved for internal use; these are
// default fields that become searchable through the tags interface.
type ReservedFields map[string]map[string]struct{}

// getReservedFields returns a map proto message types to field names
// considered reserved for internal use. All fields specified in the
// Registry proto messages are discovered here and reserved by default.
func getReservedFields() ReservedFields {
	var fs = make(ReservedFields)

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
