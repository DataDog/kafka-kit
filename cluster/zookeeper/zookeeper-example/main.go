package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	zklocking "github.com/DataDog/kafka-kit/v4/cluster/zookeeper"
)

func main() {
	timeout := flag.Duration("timeout", 3*time.Second, "lock wait timeout")
	owner := flag.String("owner", "user1", "the lock owner ID")
	ttl := flag.Int("lock-ttl", 10000, "lock TTL (milliseconds)")
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Init a Lock.
	cfg := zklocking.ZooKeeperLockConfig{
		Address:  "localhost:2181",
		Path:     "/my/locks",
		OwnerKey: "owner",
		TTL:      *ttl,
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
		defer lock.UnlockLogError(ctx)
	}

	<-sigs
}
