package server

import (
	"encoding/json"
	"fmt"
	"time"

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

	if !o.Valid() {
		return ErrInvalidKafkaObjectType
	}

	if ts == nil {
		return ErrNilTagSet
	}

	// Return early on empty TagSets.
	// Otherwise, we'll read and write
	// the current TagSet needlessly.
	if len(ts) == 0 {
		return nil
	}

	// Check if any reserved tags are being
	// attempted for use.
	for k := range ts {
		if _, r := t.ReservedFields[o.Type][k]; r {
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

	tags := map[string]string{}

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

	// Persist.
	out, err := json.Marshal(tags)
	if err != nil {
		return err
	}

	return t.ZK.Set(znode, string(out))
}

// LoadReservedFields takes a ReservedFields and stores it at
// ZKTagStorage.ReservedFields and returns an error.
func (t *ZKTagStorage) LoadReservedFields(r ReservedFields) error {
	t.ReservedFields = r

	return nil
}
