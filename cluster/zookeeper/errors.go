package zookeeper

import (
	"errors"
)

var (
	// ErrLockingFailed is a general failure
	ErrLockingFailed = errors.New("attempt to acquire lock failed")
	// ErrLockingTimedOut is returned when a lock couldn't be acquired  by the
	// context deadline.
	ErrLockingTimedOut = errors.New("attempt to acquire lock timed out")
	// ErrInvalidSeqNode is returned when sequential znodes are being parsed for
	// a trailing integer ID, but one isn't found.
	ErrInvalidSeqNode = errors.New("znode doesn't appear to be a sequential type")
)
