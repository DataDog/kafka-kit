# Overview

This package provides a ZooKeeper backed, coarse grained distributed lock. The lock path is determined at instantiation time. At request time, locks are enqueued and block until the lock is either acquired or the context deadline is me.

Further implementation notes:
- Locks are enqueued and granted in order as locks ahead are relinquished or timed out.
- Session timeouts/disconnects are handled through ZooKeeper sessions with automatic cleanup; locks that fail to acquire before the context timeout are removed from the queue even if the lock session is still active.

# Examples

Example implementation in `zookeeper-example`:

```go
package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/DataDog/kafka-kit/v3/cluster"
	zklocking "github.com/DataDog/kafka-kit/v3/cluster/zookeeper"
)

func main() {
	// Init a Lock.
	cfg := zklocking.ZooKeeperLockConfig{
		Address: "localhost:2181",
		Path:    "/my/locks",
	}

	lock1, _ := zklocking.NewZooKeeperLock(cfg)
	lock2, _ := zklocking.NewZooKeeperLock(cfg)
	lock3, _ := zklocking.NewZooKeeperLock(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = cancel

	var wg = &sync.WaitGroup{}
	wg.Add(3)

	// Get a lock.
	tryToUseTheLock(context.WithValue(ctx, "id", 1), lock1, wg)

	// An imaginary second process attempting a lock. This one times out.
	tryToUseTheLock(context.WithValue(ctx, "id", 2), lock2, wg)

	// Another imaginary process attempting a lock. This one waits, but succeeds
	// after the first lock is relinquished.
	go tryToUseTheLock(context.WithValue(ctx, "id", 3), lock3, wg)

	// The first process releases the lock.
	releaseTheLock(context.WithValue(ctx, "id", 1), lock1)

	wg.Wait()
}

func tryToUseTheLock(ctx context.Context, lock cluster.Lock, wg *sync.WaitGroup) {
	id, _ := ctx.Value("id").(int)

	if err := lock.Lock(ctx); err != nil {
		log.Printf("[process %d] error: %s\n", id, err)
	} else {
		log.Printf("[process %d] I've got the lock!\n", id)
	}

	wg.Done()
}

func releaseTheLock(ctx context.Context, lock cluster.Lock) {
	id, _ := ctx.Value("id").(int)

	if err := lock.Unlock(ctx); err != nil {
		log.Printf("[process %d] error: %s\n", id, err)
	} else {
		log.Printf("[process %d] I've released the lock!\n", id)
	}
}
```

Running the test (note: this is currently timing dependent and technically needs more coordination code for the sake of consistent testing):
```
% go run ./cluster/zookeeper/zookeeper-example
2021/12/07 00:20:29 [process 1] I've got the lock!
2021/12/07 00:20:39 [process 2] error: attempt to acquire lock timed out
2021/12/07 00:20:39 [process 1] I've released the lock!
2021/12/07 00:20:39 [process 3] I've got the lock!
```

Ephemeral lock znodes visible at the configured path:
```
[zk: localhost:2181(CONNECTED) 8] ls /my/locks
[_c_83c1bcf372c265e9ac7ee364e5d3bac5-lock-0000000027, _c_979cb11f40bb3dbc6908edeaac8f2de1-lock-0000000028]
[zk: localhost:2181(CONNECTED) 9] ls /my/locks
[_c_64c30aea1b15839542824a7b47d49ce3-lock-0000000029]
```
