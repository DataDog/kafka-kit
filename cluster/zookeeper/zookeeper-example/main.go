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
	flag.Parse()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	// Init a Lock.
	cfg := zklocking.ZooKeeperLockConfig{
		Address: "localhost:2181",
		Path:    "/my/locks",
	}

	lock, _ := zklocking.NewZooKeeperLock(cfg)
	ctx, c := context.WithTimeout(context.Background(), *timeout)
	defer c()

	// Try the lock.
	if err := lock.Lock(ctx); err != nil {
		log.Println(err)
	} else {
		log.Println("I've got the lock!")
	}

	<-sigs
	lock.Unlock(ctx)
	log.Println("I've released the lock")
}
