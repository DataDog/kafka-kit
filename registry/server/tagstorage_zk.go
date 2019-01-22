package server

import (
	"fmt"
)

// ZKTagStorage implements tag persistence in ZooKeeper.
type ZKTagStorage struct {
	Prefix         string
	ReservedFields ReservedFields
}

// ZKTagStorageConfig holds ZKTagStorage configs.
type ZKTagStorageConfig struct {
	Prefix string
}

// NewZKTagStorage initializes a ZKTagStorage.
func NewZKTagStorage(c ZKTagStorageConfig) (*ZKTagStorage, error) {
	if c.Prefix == "" {
		return nil, fmt.Errorf("prefix required")
	}

	return &ZKTagStorage{
		Prefix: c.Prefix,
	}, nil
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
		path := fmt.Sprintf("/%s/%s/%s %s=%s",
			t.Prefix, o.Kind, o.ID, k, v)
		fmt.Println(path)
	}

	return nil
}

func (t *ZKTagStorage) LoadReservedFields(r ReservedFields) error {
	t.ReservedFields = r

	return nil
}
