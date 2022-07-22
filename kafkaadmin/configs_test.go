package kafkaadmin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestAddConfig(t *testing.T) {
	rc := ResourceConfigs{}

	// Various error conditions.

	err := rc.AddConfig("", "config-key", "config-value")
	assert.Equal(t, err.Error(), "all parameters must be non-empty")

	err = rc.AddConfig("test-entry", "", "config-value")
	assert.Equal(t, err.Error(), "all parameters must be non-empty")

	err = rc.AddConfig("test-entry", "config-key", "")
	assert.Equal(t, err.Error(), "all parameters must be non-empty")

	// Check that the entry was added.

	err = rc.AddConfig("test-entry", "config-key", "config-value")
	assert.Nil(t, err)

	assert.Equal(t, "config-value", rc["test-entry"]["config-key"], "unexpected value")
}

//func TestAddConfigEntry(t *testing.T) {}