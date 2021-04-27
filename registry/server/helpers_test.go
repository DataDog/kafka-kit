package server

import (
	"context"
	"sync"
	"time"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
	"github.com/DataDog/kafka-kit/v3/registry/admin"
)

var (
	testConfig = TagHandlerConfig{
		Prefix: "registry_test",
	}
)

func testServer() *Server {
	s, _ := NewServer(Config{
		ReadReqRate:  10,
		WriteReqRate: 10,
		ZKTagsPrefix: testConfig.Prefix,
		test:         true,
	})

	s.DialZK(nil, nil, nil)

	return s
}

func testIntegrationServer() (*Server, error) {
	s, _ := NewServer(Config{
		HTTPListen:   "localhost:8080",
		GRPCListen:   "localhost:8090",
		ReadReqRate:  10,
		WriteReqRate: 10,
		ZKTagsPrefix: testConfig.Prefix,
	})

	wg := &sync.WaitGroup{}
	ctx, _ := context.WithTimeout(context.Background(), time.Second*3)

	// Init kafakzk.
	zkCfg := &kafkazk.Config{
		Connect: "zookeeper:2181",
	}

	if err := s.DialZK(ctx, wg, zkCfg); err != nil {
		return nil, err
	}

	// Init KafkaAdmin.
	adminConfig := admin.Config{
		Type:             "kafka",
		BootstrapServers: "kafka:9093",
		SSLCALocation:    "/etc/kafka/config/kafka-ca-crt.pem",
		SecurityProtocol: "SASL_SSL",
		SASLMechanism:    "PLAIN",
		SASLUsername:     "registry",
		SASLPassword:     "registry-secret",
	}

	if err := s.InitKafkaAdmin(ctx, wg, adminConfig); err != nil {
		return nil, err
	}

	return s, nil
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
