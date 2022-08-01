package server

import (
	"time"

	"github.com/DataDog/kafka-kit/v4/kafkaadmin"
	"github.com/DataDog/kafka-kit/v4/kafkaadmin/stub"
	"github.com/DataDog/kafka-kit/v4/kafkazk"
	pb "github.com/DataDog/kafka-kit/v4/registry/registry"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	testConfig = TagHandlerConfig{
		Prefix: "registry_test",
	}

	kafkaBootstrapServersSSL = "kafka:9093"
	kafkaSSLCALocation       = "/etc/kafka/config/kafka-ca-crt.pem"
	kafkaSecurityProtocol    = "SASL_SSL"
	kafkaSASLMechanism       = "PLAIN"
	kafkaSASLUsername        = "registry"
	kafkaSASLPassword        = "registry-secret"
	registryAddr             = "kafka-kit_registry_1:8090"
)

func testServer() *Server {
	s, _ := NewServer(Config{
		ReadReqRate:           10,
		WriteReqRate:          10,
		DefaultRequestTimeout: 5 * time.Second,
		ZKTagsPrefix:          testConfig.Prefix,
		test:                  true,
	})

	s.kafkaadmin = stub.NewClient()
	s.ZK = kafkazk.NewZooKeeperStub()
	s.Tags.Store = newzkTagStorageStub()

	return s
}

func RegistryClient() (pb.RegistryClient, error) {
	conn, err := grpc.Dial(registryAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	return pb.NewRegistryClient(conn), nil
}

func kafkaAdminClient() (kafkaadmin.KafkaAdmin, error) {
	return kafkaadmin.NewClient(
		kafkaadmin.Config{
			BootstrapServers: kafkaBootstrapServersSSL,
			SSLCALocation:    kafkaSSLCALocation,
			SecurityProtocol: kafkaSecurityProtocol,
			SASLMechanism:    kafkaSASLMechanism,
			SASLUsername:     kafkaSASLUsername,
			SASLPassword:     kafkaSASLPassword,
		})
}

func testTagHandler() *TagHandler {
	th, _ := NewTagHandler(testConfig)
	th.Store = newzkTagStorageStub()

	return th
}

func intsEqual(s1, s2 []uint32) bool {
	if len(s1) != len(s2) {
		return false
	}

	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

func stringsEqual(s1, s2 []string) bool {
	if len(s1) != len(s2) {
		return false
	}

	for i := range s1 {
		if s1[i] != s2[i] {
			return false
		}
	}

	return true
}

type byLength []string

func (s byLength) Len() int           { return len(s) }
func (s byLength) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s byLength) Less(i, j int) bool { return len(s[i]) < len(s[j]) }
