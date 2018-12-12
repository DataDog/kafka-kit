package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	// Errors.
	ErrFetchingTopics = errors.New("Error fetching topics")
	// Misc.
	tregex = regexp.MustCompile(".*")
)

type TopicSet map[string]*pb.Topic

// GetTopics gets topics.
func (s *Server) GetTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	if err := s.ValidateRequest(ctx, req, ReadRequest); err != nil {
		return nil, err
	}

	topicRegex := []*regexp.Regexp{}

	// Check if a specific topic is being fetched.
	if req.Name != "" {
		r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Name))
		topicRegex = append(topicRegex, r)
	} else {
		topicRegex = append(topicRegex, tregex)
	}

	// Fetch topics from ZK.
	topics, errs := s.ZK.GetTopics(topicRegex)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	matchedTopics := TopicSet{}

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

	filteredTopics, err := s.Tags.FilterTopics(matchedTopics, req.Tag)
	if err != nil {
		return nil, err
	}

	resp := &pb.TopicResponse{Topics: filteredTopics}

	return resp, nil
}

// ListTopics gets topic names.
func (s *Server) ListTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	if err := s.ValidateRequest(ctx, req, ReadRequest); err != nil {
		return nil, err
	}

	topicRegex := []*regexp.Regexp{}

	// Check if a specific topic is being fetched.
	if req.Name != "" {
		r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Name))
		topicRegex = append(topicRegex, r)
	} else {
		topicRegex = append(topicRegex, tregex)
	}

	// Fetch topics from ZK.
	topics, errs := s.ZK.GetTopics(topicRegex)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	matchedTopics := TopicSet{}

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

	filteredTopics, err := s.Tags.FilterTopics(matchedTopics, req.Tag)
	if err != nil {
		return nil, err
	}

	var filteredTopicNames []string
	for t := range filteredTopics {
		filteredTopicNames = append(filteredTopicNames, t)
	}

	resp := &pb.TopicResponse{Names: filteredTopicNames}

	return resp, nil
}
