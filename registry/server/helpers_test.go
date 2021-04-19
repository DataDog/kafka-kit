package server

import (
	"context"
	"sync"

	"github.com/DataDog/kafka-kit/v3/kafkazk"
)

var (
	testConfig = TagHandlerConfig{
		Prefix: "test",
	}
)

func testServer() *Server {
	s, _ := NewServer(Config{
		ReadReqRate:  1,
		WriteReqRate: 1,
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
		ReadReqRate:  1,
		WriteReqRate: 1,
		ZKTagsPrefix: testConfig.Prefix,
	})

	zkCfg := &kafkazk.Config{
		Connect: "localhost:2181",
	}

	wg := &sync.WaitGroup{}

	if err := s.DialZK(context.Background(), wg, zkCfg); err != nil {
		return nil, err
	}

	return s, nil
}

func testTagHandler() *TagHandler {
	th, _ := NewTagHandler(testConfig)
	th.Store = newzkTagStorageMock()

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
