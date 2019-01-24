package server

import (
	"github.com/DataDog/kafka-kit/kafkazk"
)

// zkTagStorageMockMock mocks ZKTagStorage.
type zkTagStorageMock struct {
	ReservedFields ReservedFields
	Prefix         string
	ZK             kafkazk.Handler
}

// newzkTagStorageMockMock initializes a zkTagStorageMockMock.
func newzkTagStorageMock() *zkTagStorageMock {
	zks := &zkTagStorageMock{
		Prefix: "mock",
	}

	zks.ZK = &kafkazk.Mock{}
	zks.LoadReservedFields(GetReservedFields())

	return zks
}

// SetTags mocks SetTags.
func (t *zkTagStorageMock) SetTags(o KafkaObject, ts TagSet) error {
	return nil
}

// GetTags mocks GetTags.
func (t *zkTagStorageMock) GetTags(o KafkaObject) (TagSet, error) {
	return nil, nil
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
