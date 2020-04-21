package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/DataDog/kafka-kit/kafkazk"
	"github.com/DataDog/kafka-kit/registry/admin"
	"github.com/DataDog/kafka-kit/registry/server"

	"github.com/jamiealquiza/envy"
	"github.com/masterminds/semver"
)

// This can be set with -ldflags "-X main.version=x.x.x"
var version = "0.0.0"

func main() {
	serverConfig := server.Config{}
	zkConfig := kafkazk.Config{}
	adminConfig := admin.Config{Type: "kafka"}

	v := flag.Bool("version", false, "version")
	flag.StringVar(&serverConfig.HTTPListen, "http-listen", "localhost:8080", "Server HTTP listen address")
	flag.StringVar(&serverConfig.GRPCListen, "grpc-listen", "localhost:8090", "Server gRPC listen address")
	flag.IntVar(&serverConfig.ReadReqRate, "read-rate-limit", 5, "Read request rate limit (reqs/s)")
	flag.IntVar(&serverConfig.WriteReqRate, "write-rate-limit", 1, "Write request rate limit (reqs/s)")
	flag.StringVar(&serverConfig.ZKTagsPrefix, "zk-tags-prefix", "registry", "Tags storage ZooKeeper prefix")
	flag.StringVar(&zkConfig.Connect, "zk-addr", "localhost:2181", "ZooKeeper connect string")
	flag.StringVar(&zkConfig.Prefix, "zk-prefix", "", "ZooKeeper prefix (if Kafka is configured with a chroot path prefix)")
	flag.StringVar(&adminConfig.BootstrapServers, "bootstrap-servers", "localhost", "Kafka bootstrap servers")
	flag.BoolVar(&adminConfig.SSLEnabled, "kafka-ssl-enabled", false, "Enable SSL encryption for kafka")
	flag.StringVar(&adminConfig.SSLCACertPath, "kafka-ca-cert-path", "", "CA certificate path (.pem/.crt) for verifying broker's identity")
	kafkaVersionString := flag.String("kafka-version", "v0.10.2", "Kafka release (Semantic Versioning)")

	envy.Parse("REGISTRY")
	flag.Parse()

	if *v {
		fmt.Println(version)
		os.Exit(0)
	}

	_, err := semver.NewVersion(*kafkaVersionString)
	if err != nil {
		fmt.Printf("Invalid SemVer: %s\n", *kafkaVersionString)
		os.Exit(1)
	}

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

	// Init an admin Client.
	if err := srvr.InitKafkaAdmin(ctx, wg, adminConfig); err != nil {
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
