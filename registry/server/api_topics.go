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
	if err := s.ValidateRequest(ctx, req, ReadRequest); err != nil {
		return nil, err
	}

	topicRegex := []*regexp.Regexp{}

	// Check if a specific topic is being fetched.
	if req.Topic != nil {
		r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Topic.Name))
		topicRegex = append(topicRegex, r)
	} else {
		topicRegex = append(topicRegex, tregex)
	}

	// Fetch topics from ZK.
	topics, errs := s.ZK.GetTopics(topicRegex)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	matchedTopics := map[string]*pb.Topic{}
	resp := &pb.TopicResponse{Topics: matchedTopics}

	// Populate all topics.
	for _, t := range topics {
		s, _ := s.ZK.GetTopicState(t)
		matchedTopics[t] = &pb.Topic{
			Name:       t,
			Partitions: uint32(len(s.Partitions)),
			// TODO more sophisticated check than the
			// first partition len.
			Replication: uint32(len(s.Partitions["0"])),
		}
	}

	return resp, nil
}

// ListTopics gets topic names.
func (s *Server) ListTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	if err := s.ValidateRequest(ctx, req, ReadRequest); err != nil {
		return nil, err
	}

	topicRegex := []*regexp.Regexp{}

	// Check if a specific topic is being fetched.
	if req.Topic != nil {
		r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Topic.Name))
		topicRegex = append(topicRegex, r)
	} else {
		topicRegex = append(topicRegex, tregex)
	}

	// Fetch topics from ZK.
	topics, errs := s.ZK.GetTopics(topicRegex)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	resp := &pb.TopicResponse{Names: topics}

	return resp, nil
}
