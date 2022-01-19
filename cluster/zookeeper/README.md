# Overview

This package provides a ZooKeeper backed, coarse grained distributed lock. The lock path is determined at instantiation time. At request time, locks are enqueued and block until the lock is either acquired or the context deadline is met.

Further implementation notes:
- Locks are enqueued and granted in order as locks ahead are relinquished or timed out.
- Session timeouts/disconnects are handled through ZooKeeper sessions with automatic cleanup; locks that fail to acquire before the context timeout are removed from the queue even if the lock session is still active.

# Examples

Example implementation in `zookeeper-example`:

```go
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	zklocking "github.com/DataDog/kafka-kit/v3/cluster/zookeeper"
)

func main() {
	timeout := flag.Duration("timeout", 3*time.Second, "lock wait timeout")
	owner := flag.String("owner", "user1", "the lock owner ID")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Init a Lock.
	cfg := zklocking.ZooKeeperLockConfig{
		Address:  "localhost:2181",
		Path:     "/my/locks",
		OwnerKey: "owner",
	}

	lock, _ := zklocking.NewZooKeeperLock(cfg)
	ctx, c := context.WithTimeout(context.WithValue(context.Background(), "owner", *owner), *timeout)
	defer c()

	// Try the lock.
	if err := lock.Lock(ctx); err != nil {
		log.Println(err)
	} else {
		log.Println("I've got the lock!")
		defer log.Println("I've released the lock")
		defer lock.Unlock(ctx)
	}

	<-sigs
}
```

Running the test in two terminals:
```
% ./zookeeper-example
2021/12/08 10:46:31 I've got the lock!
```

```
% ./zookeeper-example
2021/12/08 10:46:39 attempt to acquire lock timed out
```

Same test, exiting the first lock while the second lock is waiting:
```
% ./zookeeper-example
2021/12/08 10:46:58 I've got the lock!
^C2021/12/08 10:47:00 I've released the lock
```

```
% ./zookeeper-example
2021/12/08 10:47:00 I've got the lock!
```

Ephemeral lock znodes visible at the configured path:
```
[zk: localhost:2181(CONNECTED) 8] ls /my/locks
[_c_83c1bcf372c265e9ac7ee364e5d3bac5-lock-0000000027, _c_979cb11f40bb3dbc6908edeaac8f2de1-lock-0000000028]
[zk: localhost:2181(CONNECTED) 9] ls /my/locks
[_c_64c30aea1b15839542824a7b47d49ce3-lock-0000000029]
```
