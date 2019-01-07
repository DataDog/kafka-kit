package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/DataDog/kafka-kit/kafkazk"
	"github.com/DataDog/kafka-kit/registry/server"

	"github.com/jamiealquiza/envy"
)

func main() {
	serverConfig := server.Config{}
	zkConfig := kafkazk.Config{}

	flag.StringVar(&serverConfig.HTTPListen, "http-listen", "localhost:8080", "Server HTTP listen address")
	flag.StringVar(&serverConfig.GRPCListen, "grpc-listen", "localhost:8090", "Server gRPC listen address")
	flag.IntVar(&serverConfig.ReadReqRate, "read-rate-limit", 5, "Read request rate limit (reqs/s)")
	flag.IntVar(&serverConfig.WriteReqRate, "write-rate-limit", 1, "Write request rate limit (reqs/s)")
	flag.StringVar(&zkConfig.Connect, "zk-addr", "localhost:2181", "ZooKeeper connect string")
	flag.StringVar(&zkConfig.Prefix, "zk-prefix", "", "ZooKeeper prefix (if Kafka is configured with a chroot path prefix)")

	envy.Parse("REGISTRY")
	flag.Parse()

	log.Println("Registry running")

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	// Initialize Server.
	srvr, err := server.NewServer(serverConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Dial ZooKeeper.
	if err := srvr.DialZK(ctx, wg, &zkConfig); err != nil {
		log.Fatal(err)
	}

	// Start the gRPC listener.
	if err := srvr.RunRPC(ctx, wg); err != nil {
		log.Fatal(err)
	}

	// Start the HTTP listener.
	if err := srvr.RunHTTP(ctx, wg); err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown on SIGINT.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	go func() {
		<-c
		cancel()
	}()

	wg.Wait()
}
