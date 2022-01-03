package zookeeper

import (
	"errors"
	"fmt"
)

var (
	// ErrAlreadyOwnLock is returned if Lock is called with a context holding an
	// OwnerKey equal to that of an active lock.
	ErrAlreadyOwnLock = errors.New("requestor already has an active lock")
	// ErrLockingTimedOut is returned when a lock couldn't be acquired  by the
	// context deadline.
	ErrLockingTimedOut = errors.New("attempt to acquire lock timed out")
	// ErrInvalidSeqNode is returned when sequential znodes are being parsed for
	// a trailing integer ID, but one isn't found.
	ErrInvalidSeqNode = errors.New("znode doesn't appear to be a sequential type")
	// ErrNotLockOwner is returned when Unlock is attempting to be called where the
	// requestor's OwnerKey value does not equal the current lock owner.
	ErrNotLockOwner = errors.New("non-owner attempted to call unlock")
	// ErrOwnerAlreadySet is returned when SetOwner is being called on a lock where
	// the owner field is non-nil.
	ErrOwnerAlreadySet = errors.New("attempt to set owner on a claimed lock")
)

// ErrLockingFailed is a general failure.
type ErrLockingFailed struct {
	message string
}

// Error returns an error string.
func (err ErrLockingFailed) Error() string {
	return fmt.Sprintf("attempt to acquire lock failed: %s", err.message)
}

// ErrUnlockingFailed is a general failure.
type ErrUnlockingFailed struct {
	message string
}

// Error returns an error string.
func (err ErrUnlockingFailed) Error() string {
	return fmt.Sprintf("attempt to release lock failed: %s", err.message)
}
