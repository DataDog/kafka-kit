package kafkazk

import (
	"errors"
	"regexp"
)

var (
	// ErrInvalidKafkaConfigType error.
	ErrInvalidKafkaConfigType = errors.New("Invalid Kafka config type")
	// validKafkaConfigTypes is used as a set to define valid configuration
	// type names.
	validKafkaConfigTypes = map[string]struct{}{
		"broker": {},
		"topic":  {},
	}
	// Misc.
	allTopicsRegexp = regexp.MustCompile(".*")
)

// ErrNoNode error type is specifically for Get method calls where the underlying
// error type is a zkclient.ErrNoNode.
type ErrNoNode struct {
	s string
}

func (e ErrNoNode) Error() string {
	return e.s
}
