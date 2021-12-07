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
