package server

import (
	"fmt"

	"github.com/DataDog/kafka-kit/kafkazk"
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

	return zks, nil
}

// SetTags takes a KafkaObject and TagSet and sets the
// tag key:values.
func (t *ZKTagStorage) SetTags(o KafkaObject, ts TagSet) error {
	// Check the object validity.
	if !o.Valid() {
		return ErrInvalidKafkaObjectType
	}

	// Check if any reserved tags are being
	// attempted for use.
	for k := range ts {
		if _, r := t.ReservedFields[o.Kind][k]; r {
			return ErrRestrictedTag{t: k}
		}
	}

	for k, v := range ts {
		_, _ = k, v
		znode := fmt.Sprintf("/%s/%s/%s", t.Prefix, o.Kind, o.ID)
		fmt.Println(znode)
	}

	return nil
}

func (t *ZKTagStorage) LoadReservedFields(r ReservedFields) error {
	t.ReservedFields = r

	return nil
}
