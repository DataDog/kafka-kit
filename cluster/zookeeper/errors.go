package zookeeper

import (
	"errors"
	"fmt"
)

var (
	// ErrLockingTimedOut is returned when a lock couldn't be acquired  by the
	// context deadline.
	ErrLockingTimedOut = errors.New("attempt to acquire lock timed out")
	// ErrInvalidSeqNode is returned when sequential znodes are being parsed for
	// a trailing integer ID, but one isn't found.
	ErrInvalidSeqNode = errors.New("znode doesn't appear to be a sequential type")
)

// ErrLockingFailed is a general failure.
type ErrLockingFailed struct {
	message string
}

// Error returns an error string.
func (err ErrLockingFailed) Error() string {
	return fmt.Sprint("attempt to acquire lock failed :%s", err.message)
}
