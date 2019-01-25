package server

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	"github.com/DataDog/kafka-kit/kafkazk"
	pb "github.com/DataDog/kafka-kit/registry/protos"
)

var (
	// ErrFetchingTopics error.
	ErrFetchingTopics = errors.New("error fetching topics")
	// ErrTopicNotExist error.
	ErrTopicNotExist = errors.New("topic does not exist")
	// ErrTopicNameEmpty error.
	ErrTopicNameEmpty = errors.New("topic Name field must be specified")
	// Misc.
	tregex = regexp.MustCompile(".*")
)

// TopicSet is a mapping of topic name to *pb.Topic.
type TopicSet map[string]*pb.Topic

// GetTopics gets topics. If the input *pb.TopicRequest Name field is
// non-nil, the specified topic is matched if it exists. Otherwise, all
// topics found in ZooKeeper are matched. Matched topics are then filtered
// by all tags specified, if specified, in the *pb.TopicRequest tag field.
func (s *Server) GetTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	if err := s.ValidateRequest(ctx, req, readRequest); err != nil {
		return nil, err
	}

	// Get topics.
	topics, err := s.fetchTopicSet(req)
	if err != nil {
		return nil, err
	}

	// Populate the response Topics field.
	resp := &pb.TopicResponse{Topics: topics}

	return resp, nil
}

// ListTopics gets topic names. If the input *pb.TopicRequest Name field is
// non-nil, the specified topic is matched if it exists. Otherwise, all
// topics found in ZooKeeper are matched. Matched topics are then filtered
// by all tags specified, if specified, in the *pb.TopicRequest tag field.
func (s *Server) ListTopics(ctx context.Context, req *pb.TopicRequest) (*pb.TopicResponse, error) {
	if err := s.ValidateRequest(ctx, req, readRequest); err != nil {
		return nil, err
	}

	// Get topics.
	topics, err := s.fetchTopicSet(req)
	if err != nil {
		return nil, err
	}

	// Populate the response Names field.
	resp := &pb.TopicResponse{Names: topics.Names()}

	return resp, nil
}

// TopicMappings returns all broker IDs that hold at least one partition for
// the requested topic. The topic is specified in the TopicRequest.Name
// field.
func (s *Server) TopicMappings(ctx context.Context, req *pb.TopicRequest) (*pb.BrokerResponse, error) {
	if err := s.ValidateRequest(ctx, req, readRequest); err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	// Get a kafkazk.PartitionMap for the topic.
	pm, err := s.ZK.GetPartitionMap(req.Name)
	if err != nil {
		switch err.(type) {
		case kafkazk.ErrNoNode:
			return nil, ErrTopicNotExist
		default:
			return nil, err
		}
	}

	// Get a kafkazk.BrokerMap from the PartitionMap.
	bm := kafkazk.BrokerMapFromPartitionMap(pm, nil, false)

	// Get all brokers as a []int of IDs.
	allf := func(*kafkazk.Broker) bool { return true }
	bl := bm.Filter(allf).List()
	bl.SortByID()

	var ids []uint32

	// Populate broker IDs.
	for _, b := range bl {
		ids = append(ids, uint32(b.ID))
	}

	resp := &pb.BrokerResponse{Ids: ids}

	return resp, nil
}

// TagTopic sets custom tags for the specified topic. Any previously existing
// tags that were not specified in the request remain unmodified.
func (s *Server) TagTopic(ctx context.Context, req *pb.TopicRequest) (*pb.TagResponse, error) {
	if err := s.ValidateRequest(ctx, req, writeRequest); err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	if len(req.Tag) == 0 {
		return nil, ErrNilTags
	}

	// Get a TagSet from the supplied tags.
	ts, err := Tags(req.Tag).TagSet()
	if err != nil {
		return nil, err
	}

	// Ensure the topic exists.

	// Get topics from ZK.
	r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Name))
	tr := []*regexp.Regexp{r}

	topics, errs := s.ZK.GetTopics(tr)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	if len(topics) == 0 {
		return nil, ErrTopicNotExist
	}

	// Set the tags.
	err = s.Tags.Store.SetTags(KafkaObject{Type: "topic", ID: req.Name}, ts)
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
}

// DeleteTopicTag deletes custom tags for the specified topic.
func (s *Server) DeleteTopicTags(ctx context.Context, req *pb.TopicRequest) (*pb.TagResponse, error) {
	if err := s.ValidateRequest(ctx, req, writeRequest); err != nil {
		return nil, err
	}

	if req.Name == "" {
		return nil, ErrTopicNameEmpty
	}

	if len(req.Tag) == 0 {
		return nil, ErrNilTags
	}

	// Ensure the topic exists.

	// Get topics from ZK.
	r := regexp.MustCompile(fmt.Sprintf("^%s$", req.Name))
	tr := []*regexp.Regexp{r}

	topics, errs := s.ZK.GetTopics(tr)
	if errs != nil {
		return nil, ErrFetchingTopics
	}

	if len(topics) == 0 {
		return nil, ErrTopicNotExist
	}

	// Delete the tags.
	err := s.Tags.Store.DeleteTags(KafkaObject{Type: "topic", ID: req.Name}, req.Tag)
	if err != nil {
		return nil, err
	}

	return &pb.TagResponse{Message: "success"}, nil
}

// fetchBrokerSet fetches metadata for all topics.
func (s *Server) fetchTopicSet(req *pb.TopicRequest) (TopicSet, error) {
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

	matched := TopicSet{}

	// Populate all topics.
	for _, t := range topics {
		s, _ := s.ZK.GetTopicState(t)
		matched[t] = &pb.Topic{
			Name:       t,
			Partitions: uint32(len(s.Partitions)),
			// TODO more sophisticated check than the
			// first partition len.
			Replication: uint32(len(s.Partitions["0"])),
		}
	}

	filtered, err := s.Tags.FilterTopics(matched, req.Tag)
	if err != nil {
		return nil, err
	}

	return filtered, nil
}

// Names returns a []string of topic names from a TopicSet.
func (t TopicSet) Names() []string {
	var names = []string{}

	for n := range t {
		names = append(names, n)
	}

	sort.Strings(names)

	return names
}
