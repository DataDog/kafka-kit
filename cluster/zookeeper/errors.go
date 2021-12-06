package zookeeper

import (
	"errors"
)

var (
	ErrLockingFailed = errors.New("attempt to acquire lock timed out or failed")
	// ErrInvalidSeqNode is returned when sequential znodes are being parsed for
	// a trailing integer ID, but one isn't found.
	ErrInvalidSeqNode = errors.New("znode doesn't appear to be a sequential type")
)
