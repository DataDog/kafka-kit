// package cluster specifies clustering primitives for multi-node service
// coordination.
package cluster

import (
	"context"
)

// Lock defines a distributed locking service.
type Lock interface {
	// Lock and Unlock are simple, coarse grain locks based on a pre-defined
	// lock path. The lock path is an implementation detail that isn't negotiated
	// through this interface. A context is accepted for setting wait bounds.
	Lock(context.Context) error
	Unlock(context.Context) error
}
