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
	// ErrKafkaObjectDoesNotExist error.
	ErrKafkaObjectDoesNotExist = errors.New("requested Kafka object does not exist")
	// ErrNilTagSet error.
	ErrNilTagSet = errors.New("must provide a non-nil or non-empty TagSet")
	// ErrNilTags error.
	ErrNilTags = errors.New("must provide a non-nil or non-empty tags")
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
	FieldReserved(KafkaObject, string) bool
	SetTags(KafkaObject, TagSet) error
	GetTags(KafkaObject) (TagSet, error)
	DeleteTags(KafkaObject, Tags) error
}

// NewTagHandler initializes a TagHandler.
func NewTagHandler(c TagHandlerConfig) (*TagHandler, error) {
	ts, err := NewZKTagStorage(ZKTagStorageConfig{Prefix: c.Prefix})
	if err != nil {
		return nil, err
	}

	err = ts.LoadReservedFields(GetReservedFields())
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
	Type string
	ID   string
}

// Valid checks if a KafkaObject has a valid
// Type field value.
func (o KafkaObject) Valid() bool {
	switch {
	case o.Type == "broker", o.Type == "topic":
		return true
	}

	return false
}

// Complete checks if a KafkaObject is valid
// and has a non-empty ID field value.
func (o KafkaObject) Complete() bool {
	return o.Valid() && o.ID != ""
}

// TagSetFromObject takes a protobuf type and returns the
// default TagSet along with any user-defined tags.
func (t *TagHandler) TagSetFromObject(o interface{}) (TagSet, error) {
	var ts = TagSet{}
	var ko = KafkaObject{}

	// Populate default tags.
	switch o.(type) {
	case *pb.Topic:
		t := o.(*pb.Topic)
		ko.Type = "topic"
		ko.ID = t.Name

		ts["name"] = t.Name
		ts["partitions"] = fmt.Sprintf("%d", t.Partitions)
		ts["replication"] = fmt.Sprintf("%d", t.Replication)
	case *pb.Broker:
		b := o.(*pb.Broker)
		ko.Type = "broker"
		ko.ID = fmt.Sprintf("%d", b.Id)

		// TODO implement map/nested types.
		// ts["listenersecurityprotocolmap"] = b.ListenerSecurityProtocolMap
		ts["id"] = fmt.Sprintf("%d", b.Id)
		ts["rack"] = b.Rack
		ts["jmxport"] = fmt.Sprintf("%d", b.Jmxport)
		ts["host"] = b.Host
		ts["timestamp"] = fmt.Sprintf("%d", b.Timestamp)
		ts["port"] = fmt.Sprintf("%d", b.Port)
		ts["version"] = fmt.Sprintf("%d", b.Version)
	}

	// Fetch stored tags.
	st, err := t.Store.GetTags(ko)
	if err != nil {
		switch {
		// ErrKafkaObjectDoesNotExist from TagStorage
		// simply means we do not have any user-defined
		// tags stored for the requested object.
		case err == ErrKafkaObjectDoesNotExist:
			break
		default:
			return nil, err
		}
	}

	// Merge stored tags with default tags.
	for k, v := range st {
		ts[k] = v
	}

	return ts, nil
}

// FilterTopics takes a map of topic names to *pb.Topic and tags KV list.
// A filtered map is returned that includes topics where all tags
// values match the provided input tag KVs. Additionally, any custom
// tags persisted in the TagStorage backend are populated into the
// Tags field for each matched object.
func (t *TagHandler) FilterTopics(in TopicSet, tags Tags) (TopicSet, error) {
	var out = make(TopicSet)

	// Get tag key/values.
	tagKV, err := tags.TagSet()
	if err != nil {
		return nil, err
	}

	// Filter input topics.
	for name, topic := range in {
		ts, err := t.TagSetFromObject(topic)
		if err != nil {
			return nil, err
		}

		if ts.matchAll(tagKV) {
			out[name] = topic

			// Ensure that custom tags fetched from storage are
			// populated into the tags field for the object.
			for k, v := range ts {
				// Custom tags are any non-reserved object fields.
				if !t.Store.FieldReserved(KafkaObject{Type: "topic"}, k) {
					if out[name].Tags == nil {
						out[name].Tags = map[string]string{}
					}

					out[name].Tags[k] = v
				}
			}
		}
	}

	return out, nil
}

// FilterBrokers takes a map of broker IDs to *pb.Broker and tags KV list.
// A filtered map is returned that includes brokers where all tags
// values match the provided input tag KVs. Additionally, any custom
// tags persisted in the TagStorage backend are populated into the
// Tags field for each matched object.
func (t *TagHandler) FilterBrokers(in BrokerSet, tags Tags) (BrokerSet, error) {
	var out = make(BrokerSet)

	// Get tag key/values.
	tagKV, err := tags.TagSet()
	if err != nil {
		return nil, err
	}

	// Filter input brokers.
	for id, broker := range in {
		ts, err := t.TagSetFromObject(broker)
		if err != nil {
			return nil, err
		}

		if ts.matchAll(tagKV) {
			out[id] = broker

			// Ensure that custom tags fetched from storage are
			// populated into the tags field for the object.
			for k, v := range ts {
				// Custom tags are any non-reserved object fields.
				if !t.Store.FieldReserved(KafkaObject{Type: "broker"}, k) {
					if out[id].Tags == nil {
						out[id].Tags = map[string]string{}
					}

					out[id].Tags[k] = v
				}
			}
		}
	}

	return out, nil
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

// Equal checks if the input TagSet has the same
// key:value pairs as the calling TagSet.
func (t1 TagSet) Equal(t2 TagSet) bool {
	if len(t1) != len(t2) {
		return false
	}

	for k, v := range t1 {
		if t2[k] != v {
			return false
		}
	}

	return true
}

// TagSet takes a tags and returns a TagSet and error for any
// malformed tags. Tags are expected to be formatted as a
// comma delimited "key:value,key2:value2" string.
// TODO normalize all tag usage to lower case.
func (t Tags) TagSet() (TagSet, error) {
	var ts = TagSet{}

	for _, tag := range t {
		kv := strings.Split(tag, ":")
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid tag '%s': must be formatted as key:value", t)
		}

		ts[kv[0]] = kv[1]
	}

	return ts, nil
}

// ReservedFields is a mapping of object types (topic, broker)
// to a set of fields reserved for internal use; these are
// default fields that become searchable through the tags interface.
type ReservedFields map[string]map[string]struct{}

// GetReservedFields returns a map proto message types to field names
// considered reserved for internal use. All fields specified in the
// Registry proto messages are discovered here and reserved by default.
func GetReservedFields() ReservedFields {
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
