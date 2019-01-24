package server

import (
	"github.com/DataDog/kafka-kit/kafkazk"
)

// zkTagStorageMockMock mocks ZKTagStorage.
type zkTagStorageMock struct {
	ReservedFields ReservedFields
	Prefix         string
	ZK             kafkazk.Handler
	// tags is a crude emulation of ZooKeeper storage.
	tags map[string]map[string]TagSet
}

// newzkTagStorageMockMock initializes a zkTagStorageMockMock.
func newzkTagStorageMock() *zkTagStorageMock {
	zks := &zkTagStorageMock{
		Prefix: "mock",
		tags:   map[string]map[string]TagSet{},
	}

	zks.ZK = &kafkazk.Mock{}
	zks.LoadReservedFields(GetReservedFields())

	return zks
}

// SetTags mocks SetTags.
func (t *zkTagStorageMock) SetTags(o KafkaObject, ts TagSet) error {
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

// GetTags mocks GetTags.
func (t *zkTagStorageMock) GetTags(o KafkaObject) (TagSet, error) {
	if _, exist := t.tags[o.Type]; !exist {
		return TagSet{}, nil
	}

	if _, exist := t.tags[o.Type][o.ID]; !exist {
		return TagSet{}, nil
	}

	return t.tags[o.Type][o.ID], nil
}

// FieldReserved mocks FieldReserved.
func (t *zkTagStorageMock) FieldReserved(o KafkaObject, f string) bool {
	if !o.Valid() {
		return false
	}

	_, ok := t.ReservedFields[o.Type][f]

	return ok
}

// LoadReservedFields mocks FieldReserved.
func (t *zkTagStorageMock) LoadReservedFields(r ReservedFields) error {
	t.ReservedFields = r

	return nil
}
