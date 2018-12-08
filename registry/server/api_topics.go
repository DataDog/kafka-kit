package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	ErrFetchingTopics = errors.New("Error fetching topics")
	tregex            = regexp.MustCompile(".*")
)

// GetTopics gets topics.
func (s *Server) GetTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	s.LogRequest(ctx, fmt.Sprintf("%v", req))

	// Fetch topics from ZK.
	topics, errs := s.ZK.GetTopics([]*regexp.Regexp{tregex})
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	matchedTopics := map[string]*pb.Topic{}
	resp := &pb.TopicResponse{Topics: matchedTopics}

	// Populate all topics.
	for _, t := range topics {
		s, _ := s.ZK.GetTopicState(t)
		matchedTopics[t] = &pb.Topic{
			Name:        t,
			Partitions:  uint32(len(s.Partitions)),
			Replication: uint32(len(s.Partitions["0"])),
		}
	}

	return resp, nil
}

// ListTopics gets topic names.
func (s *Server) ListTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	s.LogRequest(ctx, fmt.Sprintf("%v", req))

	// Fetch topics from ZK.
	r := regexp.MustCompile(".*")

	topics, errs := s.ZK.GetTopics([]*regexp.Regexp{r})
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	resp := &pb.TopicResponse{Names: topics}

	return resp, nil
}
