package server

import (
	"github.com/DataDog/kafka-kit/kafkazk"
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

func testTagHandler() *TagHandler {
	th, _ := NewTagHandler(testConfig)
	th.Store.(*ZKTagStorage).ZK = &kafkazk.Mock{}

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
