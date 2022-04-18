package throttlestore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

var (
	ErrNoOverrideSet = errors.New("no override set at path")
)

// ThrottleOverrideConfig holds throttle override configurations.
type ThrottleOverrideConfig struct {
	// Rate in MB.
	Rate int `json:"rate"`
	// Whether the override rate should be
	// removed when the current reassignments finish.
	AutoRemove bool `json:"autoremove"`
}

// fetchThrottleOverride gets a throttle override from path p.
func FetchThrottleOverride(zk kafkazk.Handler, p string) (*ThrottleOverrideConfig, error) {
	c := &ThrottleOverrideConfig{}

	override, err := zk.Get(p)
	if err != nil {
		switch err.(type) {
		case kafkazk.ErrNoNode:
			return c, ErrNoOverrideSet
		default:
			return c, fmt.Errorf("error getting throttle override: %s", err)
		}
	}

	if len(override) == 0 {
		return c, nil
	}

	if err := json.Unmarshal(override, c); err != nil {
		return c, fmt.Errorf("error unmarshalling override config: %s", err)
	}

	return c, nil
}

// storeThrottleOverride sets a throttle override to path p.
func StoreThrottleOverride(zk kafkazk.Handler, p string, c ThrottleOverrideConfig) error {
	d, err := json.Marshal(c)
	if err != nil {
		return fmt.Errorf("error marshalling override config: %s", err)
	}

	// Check if the path exists.
	exists, _ := zk.Exists(p)
	err = nil

	if exists {
		// Update.
		err = zk.Set(p, string(d))
	} else {
		// Create.
		err = zk.Create(p, string(d))
	}

	if err != nil {
		return fmt.Errorf("error setting throttle override: %s", err)
	}

	return nil
}

// removeThrottleOverride deletes an override at path p.
func RemoveThrottleOverride(zk kafkazk.Handler, p string) error {
	exists, err := zk.Exists(p)
	if !exists && err == nil {
		return nil
	}

	err = zk.Delete(p)
	if err != nil {
		return fmt.Errorf("error removing throttle override: %s", err)
	}

	return nil
}

// FetchBrokerOverrides returns a BrokerOverrides populated with all brokers
// with overrides set. This function exists as a convenience since the number of
// broker overrides can vary, as opposed to the global which has a single,
// consistent znode that always exists.
func FetchBrokerOverrides(zk kafkazk.Handler, p string) (BrokerOverrides, error) {
	overrides := BrokerOverrides{}

	// Get brokers with overrides configured.
	brokers, err := zk.Children(p)
	if err != nil {
		return nil, err
	}

	// Fetch override config for each.
	for _, b := range brokers {
		id, _ := strconv.Atoi(b)
		c := &ThrottleOverrideConfig{}
		brokerZnode := fmt.Sprintf("%s/%d", p, id)

		override, err := zk.Get(brokerZnode)
		if err != nil {
			return overrides, fmt.Errorf("error getting throttle override: %s", err)
		}

		err = json.Unmarshal(override, c)
		if err != nil {
			return overrides, fmt.Errorf("error unmarshalling override config: %s", err)
		}

		overrides[id] = BrokerThrottleOverride{
			ID:                      id,
			ReassignmentParticipant: false,
			Config:                  *c,
		}
	}

	return overrides, nil
}
