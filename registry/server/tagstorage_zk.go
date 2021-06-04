package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

// ZKTagStorage implements tag persistence in ZooKeeper.
type ZKTagStorage struct {
	ReservedFields ReservedFields
	Prefix         string
	ZK             kafkazk.Handler
}

// ZKTagStorageConfig holds ZKTagStorage configs.
type ZKTagStorageConfig struct {
	ZKAddr string
	Prefix string
}

// NewZKTagStorage initializes a ZKTagStorage.
func NewZKTagStorage(c ZKTagStorageConfig) (*ZKTagStorage, error) {
	if c.Prefix == "" {
		return nil, fmt.Errorf("prefix required")
	}

	zks := &ZKTagStorage{
		Prefix: c.Prefix,
	}

	// Although this implementation is backed by ZooKeeper,
	// we don't dial a connection in this instantiation func.
	// The kafkazk Handler is shared / passed in by the parent
	// registry Server during setup in the DialZK call.
	// TODO needs a standalone dial func.

	return zks, nil
}

// Init ensures the ZooKeeper connection is ready and
// any required znodes are created.
func (t *ZKTagStorage) Init() error {
	// Readiness check.
	time.Sleep(250 * time.Millisecond)
	if !t.ZK.Ready() {
		return fmt.Errorf("connection to ZooKeeper not ready in 250ms")
	}

	// Child znodes to create under the
	// parent prefix.
	baseZnodes := []string{
		fmt.Sprintf("/%s", t.Prefix),
		fmt.Sprintf("/%s/broker", t.Prefix),
		fmt.Sprintf("/%s/topic", t.Prefix),
	}

	for _, p := range baseZnodes {
		exist, err := t.ZK.Exists(p)
		if err != nil {
			return fmt.Errorf("failed to create znode: %s", err)
		}

		if !exist {
			if err := t.ZK.Create(p, ""); err != nil {
				return err
			}
		}
	}

	return nil
}

// SetTags takes a KafkaObject and TagSet and sets the
// tag key:values for the object.
func (t *ZKTagStorage) SetTags(o KafkaObject, ts TagSet) error {
	// Sanity checks.
	if !o.Complete() {
		return ErrInvalidKafkaObjectType
	}

	if ts == nil || len(ts) == 0 {
		return ErrNilTagSet
	}

	// Check if any reserved tags are being
	// attempted for use.
	for k := range ts {
		if t.FieldReserved(o, k) {
			return ErrReservedTag{t: k}
		}
	}

	znode := fmt.Sprintf("/%s/%s/%s", t.Prefix, o.Type, o.ID)

	// Fetch current tags.
	data, err := t.ZK.Get(znode)
	if err != nil {
		switch err.(type) {
		// The znode doesn't exist; create it.
		case kafkazk.ErrNoNode:
			data = []byte{}
			if err := t.ZK.Create(znode, ""); err != nil {
				return err
			}
		default:
			return err
		}
	}

	tags := TagSet{}

	if len(data) != 0 {
		err = json.Unmarshal(data, &tags)
		if err != nil {
			return err
		}
	}

	// Update with provided tags.
	for k, v := range ts {
		tags[k] = v
	}

	// Serialize, persist.
	out, err := json.Marshal(tags)
	if err != nil {
		return err
	}

	return t.ZK.Set(znode, string(out))
}

// GetTags returns the TagSet for the requested KafkaObject.
func (t *ZKTagStorage) GetTags(o KafkaObject) (TagSet, error) {
	// Sanity checks.
	if !o.Complete() {
		return nil, ErrInvalidKafkaObjectType
	}

	znode := fmt.Sprintf("/%s/%s/%s", t.Prefix, o.Type, o.ID)

	// Fetch tags.
	data, err := t.ZK.Get(znode)
	if err != nil {
		switch err.(type) {
		// The object doesn't exist.
		case kafkazk.ErrNoNode:
			return nil, ErrKafkaObjectDoesNotExist
		default:
			return nil, err
		}
	}

	tags := TagSet{}

	if len(data) != 0 {
		err = json.Unmarshal(data, &tags)
		if err != nil {
			return nil, err
		}
	}

	return tags, nil
}

// GetAllTags returns all tags stored in the tagstore, keyed by the resource they correspond to.
func (t *ZKTagStorage) GetAllTags() (map[KafkaObject]TagSet, error) {

	tags := map[KafkaObject]TagSet{}

	// Get all broker tags.
	brokerTags, err := t.GetAllTagsForType("broker")
	// Don't return if the parent broker tag znode doesn't exist; this just
	// means no tags were ever set.
	if err != nil && err != ErrKafkaObjectDoesNotExist {
		return nil, err
	}

	for broker, brokerTags := range brokerTags {
		tags[broker] = brokerTags
	}

	// Get all topic tags.
	topicTags, err := t.GetAllTagsForType("topic")
	// Don't return if the parent topic tag znode doesn't exist; this just
	// means no tags were ever set.
	if err != nil && err != ErrKafkaObjectDoesNotExist {
		return nil, err
	}

	for topic, topicTags := range topicTags {
		tags[topic] = topicTags
	}

	return tags, nil
}

// GetAllTagsForType gets all the tags for objects of the given type. A convenience method that makes getting every tag a little easier.
func (t *ZKTagStorage) GetAllTagsForType(kafkaObjectType string) (map[KafkaObject]TagSet, error) {
	zNode := fmt.Sprintf("/%s/%s", t.Prefix, kafkaObjectType)
	children, err := t.ZK.Children(zNode)
	if err != nil {
		switch err.(type) {
		case kafkazk.ErrNoNode:
			return nil, ErrKafkaObjectDoesNotExist
		default:
			return nil, err
		}
	}

	objectTags := map[KafkaObject]TagSet{}

	for _, objectId := range children {
		object := KafkaObject{Type: kafkaObjectType, ID: objectId}
		tags, err := t.GetTags(object)
		if err != nil {
			switch err.(type) {
			case kafkazk.ErrNoNode:
				return nil, ErrKafkaObjectDoesNotExist
			default:
				return nil, err
			}
		}
		objectTags[object] = tags
	}

	return objectTags, nil
}

// DeleteTags deletes all tags in the list of keys for the requested KafkaObject.
func (t *ZKTagStorage) DeleteTags(o KafkaObject, keysToDelete []string) error {
	// Sanity checks.
	if !o.Complete() {
		return ErrInvalidKafkaObjectType
	}

	if len(keysToDelete) == 0 {
		return ErrNilTags
	}

	znode := fmt.Sprintf("/%s/%s/%s", t.Prefix, o.Type, o.ID)

	// Fetch tags.
	data, err := t.ZK.Get(znode)
	if err != nil {
		switch err.(type) {
		// The object doesn't exist.
		case kafkazk.ErrNoNode:
			return ErrKafkaObjectDoesNotExist
		default:
			return err
		}
	}

	tags := TagSet{}

	if len(data) != 0 {
		err = json.Unmarshal(data, &tags)
		if err != nil {
			return err
		}
	}

	// Delete listed tags.
	for _, k := range keysToDelete {
		delete(tags, k)
	}

	// Serialize, persist.
	out, err := json.Marshal(tags)
	if err != nil {
		return err
	}

	return t.ZK.Set(znode, string(out))
}

// FieldReserved takes a KafkaObject and field name. A bool
// is returned that indicates whether the field is reserved
// for the respective KafkaObject type.
func (t *ZKTagStorage) FieldReserved(o KafkaObject, f string) bool {
	if !o.Valid() {
		return false
	}

	_, ok := t.ReservedFields[o.Type][f]

	return ok
}

// LoadReservedFields takes a ReservedFields and stores it at
// ZKTagStorage.ReservedFields and returns an error.
func (t *ZKTagStorage) LoadReservedFields(r ReservedFields) error {
	t.ReservedFields = r

	return nil
}
