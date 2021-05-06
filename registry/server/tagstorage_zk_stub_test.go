package server

import (
	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

// zkTagStorageStubStub stubs ZKTagStorage.
type zkTagStorageStub struct {
	ReservedFields ReservedFields
	Prefix         string
	ZK             kafkazk.Handler
	// tags is a crude emulation of ZooKeeper storage.
	tags map[string]map[string]TagSet
}

// newzkTagStorageStubStub initializes a zkTagStorageStubStub.
func newzkTagStorageStub() *zkTagStorageStub {
	zks := &zkTagStorageStub{
		Prefix: "stub",
		tags:   map[string]map[string]TagSet{},
	}

	zks.ZK = &kafkazk.Stub{}
	zks.LoadReservedFields(GetReservedFields())

	return zks
}

// SetTags stubs SetTags.
func (t *zkTagStorageStub) SetTags(o KafkaObject, ts TagSet) error {
	if !o.Complete() {
		return ErrInvalidKafkaObjectType
	}

	if _, exist := t.tags[o.Type]; !exist {
		t.tags[o.Type] = map[string]TagSet{}
	}

	if _, exist := t.tags[o.Type][o.ID]; !exist {
		t.tags[o.Type][o.ID] = TagSet{}
	}

	for k, v := range ts {
		t.tags[o.Type][o.ID][k] = v
	}

	return nil
}

// GetTags stubs GetTags.
func (t *zkTagStorageStub) GetTags(o KafkaObject) (TagSet, error) {
	if !o.Complete() {
		return nil, ErrInvalidKafkaObjectType
	}

	if _, exist := t.tags[o.Type]; !exist {
		return nil, ErrKafkaObjectDoesNotExist
	}

	if _, exist := t.tags[o.Type][o.ID]; !exist {
		return nil, ErrKafkaObjectDoesNotExist
	}

	return t.tags[o.Type][o.ID], nil
}

// DeleteTags stubs DeleteTags.
func (t *zkTagStorageStub) DeleteTags(o KafkaObject, tl Tags) error {
	if !o.Complete() {
		return ErrInvalidKafkaObjectType
	}

	if _, exist := t.tags[o.Type]; !exist {
		return ErrKafkaObjectDoesNotExist
	}

	if _, exist := t.tags[o.Type][o.ID]; !exist {
		return ErrKafkaObjectDoesNotExist
	}

	tagSet, _ := tl.TagSet()
	for k, _ := range tagSet {
		delete(t.tags[o.Type][o.ID], k)
	}

	return nil
}

// GetAllTags stubs GetAllTags
func (t *zkTagStorageStub) GetAllTags() (map[KafkaObject]TagSet, error) {
	ts := map[KafkaObject]TagSet{}

	// just flattens and returns all tag sets.
	for objectType, objects := range t.tags {
		for o, tags := range objects {
			ts[KafkaObject{Type: objectType, ID: o}] = tags
		}
	}

	return ts, nil
}

// FieldReserved stubs FieldReserved.
func (t *zkTagStorageStub) FieldReserved(o KafkaObject, f string) bool {
	if !o.Valid() {
		return false
	}

	_, ok := t.ReservedFields[o.Type][f]

	return ok
}

// LoadReservedFields stubs FieldReserved.
func (t *zkTagStorageStub) LoadReservedFields(r ReservedFields) error {
	t.ReservedFields = r

	return nil
}
