package server

import (
	"fmt"
)

type ZKTagStorage struct {
	Prefix string
	// Mapping of type (broker, topic) to restricted fields.
	restrictedFields map[string]map[string]struct{}
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
		Prefix:           c.Prefix,
		restrictedFields: restrictedFields(),
	}, nil
}

// SetTags takes a KafkaObject and TagSet and sets the
// tag key:values.
func (t *ZKTagStorage) SetTags(o KafkaObject, ts TagSet) error {
	// Check the object validity.
	if !o.Valid() {
		return ErrInvalidKafkaObjectType
	}

	// Check if any restricted tags are being
	// attempted for use.
	for k := range ts {
		if _, r := t.restrictedFields[o.Kind][k]; r {
			return ErrRestrictedTag{t: k}
		}
	}

	for k, v := range ts {
		_, _ = k, v
		path := fmt.Sprintf("/%s/%s/%s", t.Prefix, o.Kind, o.ID)
		fmt.Println(path)
	}

	return nil
}
