package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/DataDog/kafka-kit/v3/kafkaadmin"
	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"github.com/DataDog/kafka-kit/v3/registry/server"

	"github.com/Masterminds/semver"
	"github.com/jamiealquiza/envy"
	"gopkg.in/DataDog/dd-trace-go.v1/profiler"
)

// This can be set with -ldflags "-X main.version=x.x.x"
var version = "0.0.0"

func main() {
	serverConfig := server.Config{}
	zkConfig := kafkazk.Config{}
	adminConfig := kafkaadmin.Config{}

	securityProtocols := make([]string, 0, len(kafkaadmin.SecurityProtocolSet))
	for k := range kafkaadmin.SecurityProtocolSet {
		securityProtocols = append(securityProtocols, k)
	}
	saslMechanims := make([]string, 0, len(kafkaadmin.SASLMechanismSet))
	for k := range kafkaadmin.SASLMechanismSet {
		saslMechanims = append(saslMechanims, k)
	}

	v := flag.Bool("version", false, "version")
	profiling := flag.Bool("enable-profiling", false, "Enable Datadog continuous profiling")
	flag.StringVar(&serverConfig.HTTPListen, "http-listen", "localhost:8080", "Server HTTP listen address")
	flag.StringVar(&serverConfig.GRPCListen, "grpc-listen", "localhost:8090", "Server gRPC listen address")
	flag.IntVar(&serverConfig.ReadReqRate, "read-rate-limit", 5, "Read request rate limit (reqs/s)")
	flag.IntVar(&serverConfig.WriteReqRate, "write-rate-limit", 1, "Write request rate limit (reqs/s)")
	flag.StringVar(&serverConfig.ZKTagsPrefix, "zk-tags-prefix", "registry", "Tags storage ZooKeeper prefix")
	flag.StringVar(&zkConfig.Connect, "zk-addr", "localhost:2181", "ZooKeeper connect string")
	flag.StringVar(&zkConfig.Prefix, "zk-prefix", "", "ZooKeeper prefix (if Kafka is configured with a chroot path prefix)")
	flag.StringVar(&adminConfig.BootstrapServers, "bootstrap-servers", "localhost", "Kafka bootstrap servers")
	flag.StringVar(&adminConfig.SecurityProtocol, "kafka-security-protocol", "", fmt.Sprintf("Protocol used to communicate with brokers. Supported: %s", strings.Join(securityProtocols, ", ")))
	flag.StringVar(&adminConfig.SSLCALocation, "kafka-ssl-ca-location", "", "CA certificate path (.pem/.crt) for verifying broker's identity. Needed for SSL and SASL_SSL protocols.")
	flag.StringVar(&adminConfig.SASLMechanism, "kafka-sasl-mechanism", "", fmt.Sprintf("SASL mechanism to use for authentication. Supported: %s", strings.Join(saslMechanims, ", ")))
	flag.StringVar(&adminConfig.SASLUsername, "kafka-sasl-username", "", "SASL username for use with the PLAIN and SASL-SCRAM-* mechanisms")
	flag.StringVar(&adminConfig.SASLPassword, "kafka-sasl-password", "", "SASL password for use with the PLAIN and SASL-SCRAM-* mechanisms")
	flag.IntVar(&serverConfig.TagAllowedStalenessMinutes, "tag-allowed-staleness", 60, "Minutes before tags with no associated resource are deleted")
	flag.IntVar(&serverConfig.TagCleanupFrequencyMinutes, "tag-cleanup-frequency", 20, "Minutes between runs of tag cleanup")

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

	// Start profiling if enabled.
	if *profiling {
		// Optional trace agent URL override.
		traceAgentURL := "localhost:8126"
		if e := os.Getenv("TRACE_AGENT_URL"); e != "" {
			traceAgentURL = e
		}

		err = profiler.Start(
			profiler.WithService("kafka-registry"),
			profiler.WithAgentAddr(traceAgentURL),
			profiler.WithProfileTypes(
				profiler.CPUProfile,
				profiler.HeapProfile,
			),
		)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}

	defer profiler.Stop()

	if adminConfig.SecurityProtocol != "" {
		adminConfig.SecurityProtocol = strings.ToUpper(adminConfig.SecurityProtocol)
		if _, validChoice := kafkaadmin.SecurityProtocolSet[adminConfig.SecurityProtocol]; !validChoice {
			fmt.Printf("Invalid kafka security protocol. Supported protocols: %s\n", strings.Join(securityProtocols, ", "))
			os.Exit(1)
		}
	}

	if adminConfig.SASLMechanism != "" {
		adminConfig.SASLMechanism = strings.ToUpper(adminConfig.SASLMechanism)
		if _, validChoice := kafkaadmin.SASLMechanismSet[adminConfig.SASLMechanism]; !validChoice {
			fmt.Printf("Invalid kafka SASL mechanism. Supported mechanisms: %s\n", strings.Join(saslMechanims, ", "))
			os.Exit(1)
		}
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

	// Init a kafka consumer. Needed for offset translations.
	if err := srvr.InitKafkaConsumer(ctx, wg, adminConfig); err != nil {
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

	// Start the tag cleanup background thread
	if err := srvr.RunTagCleanup(ctx, wg, serverConfig); err != nil {
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
